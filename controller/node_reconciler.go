package controller

import (
	"context"
	"errors"
	"fmt"

	clusterv1alpha1 "github.com/andrewstucki/cluster-controller/controller/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type ClusterNodeReconciler[T, U, V any, PT ptrToObject[T], PU ptrToObject[U], PV ptrToObject[V]] struct {
	config *Config[T, U, V, PT, PU, PV]

	podManager      *PodManager[V, PV]
	nodeManager     *NodeManager[T, U, V, PT, PU, PV]
	resourceManager *ResourceManager[V, PV]
	subscriber      LifecycleSubscriber[T, V, PT, PV]
}

func setupClusterNodeReconciler[T, U, V any, PT ptrToObject[T], PU ptrToObject[U], PV ptrToObject[V]](mgr ctrl.Manager, config *Config[T, U, V, PT, PU, PV]) error {
	return (&ClusterNodeReconciler[T, U, V, PT, PU, PV]{
		config:          config,
		podManager:      config.podManager(),
		nodeManager:     config.clusterNodeManager(),
		resourceManager: config.nodeResourceManager(),
		subscriber:      config.Subscriber,
	}).setupWithManager(mgr)
}

func (r *ClusterNodeReconciler[T, U, V, PT, PU, PV]) setupWithManager(mgr ctrl.Manager) error {
	builder := ctrl.NewControllerManagedBy(mgr).For(newKubeObject[V, PV]())

	return r.resourceManager.WatchResources(builder).Owns(&corev1.Pod{}).Complete(r)
}

func (r *ClusterNodeReconciler[T, U, V, PT, PU, PV]) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	deploymentFirstStabilized := false

	logger := log.FromContext(ctx)

	node := newKubeObject[V, PV]()
	if err := r.config.Client.Get(ctx, req.NamespacedName, node); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	cluster, err := r.getCluster(ctx, node)
	if err != nil {
		logger.Error(err, "fetching cluster")
		return ctrl.Result{}, err
	}
	if cluster == nil {
		// we have no cluster that this node is associated with, which isn't supported
		// so just ignore this node
		return ctrl.Result{}, nil
	}

	pods, err := r.podManager.ListPods(ctx, node)
	if err != nil {
		logger.Error(err, "fetching cluster pods")
		return ctrl.Result{}, err
	}

	originalStatus := r.config.Factory.GetClusterNodeStatus(node)
	status := originalStatus.DeepCopy()
	status.ObservedGeneration = node.GetGeneration()

	// set our cluster version immediately
	status.ClusterVersion = r.nodeManager.GetHash(node)
	// if we don't match, overwrite our current matching status
	// which will only get updated when we've fully stabilized
	if originalStatus.ClusterVersion != status.ClusterVersion {
		status.MatchesCluster = false
	}

	markPodIfReady := func(pod *corev1.Pod) {
		healthy, running := r.podManager.IsHealthy(pod), r.podManager.IsRunningAndReady(pod)
		status.Healthy, status.Running = healthy, running

		if healthy {
			// we have a one way operation of setting this as fully ready for the current cluster
			if !status.MatchesCluster {
				// we now have an initially deployed node
				deploymentFirstStabilized = true
				status.MatchesCluster = true
			}
			setPhase(node, status, readyPhase("cluster node ready"))
		}
	}

	syncStatus := func(err error) (ctrl.Result, error) {
		if deploymentFirstStabilized {
			// we tell our manager that we are now stabilized, we special case our error here
			// so that we retry if it fails without setting our status in case this is a fully needed
			// operation
			if err := r.subscriber.AfterNodeDeployed(ctx, cluster, node); err != nil {
				logger.Error(err, "running node stabilized callback")
				return ctrl.Result{Requeue: true}, nil
			}
		}

		if !apiequality.Semantic.DeepEqual(status, &originalStatus) {
			r.config.Factory.SetClusterNodeStatus(node, *status)
			syncErr := r.config.Client.Status().Update(ctx, node)
			err = errors.Join(syncErr, err)
		}

		return ignoreConflict(err)
	}

	// we are being deleted, clean up everything
	if node.GetDeletionTimestamp() != nil {
		setPhase(node, status, terminatingPhase("node terminating"))

		// delete all my resources
		if deleted, err := r.resourceManager.DeleteAll(ctx, node); deleted || err != nil {
			return syncStatus(err)
		}

		if len(pods) > 0 {
			if err := r.podManager.Delete(ctx, pods[0]); err != nil {
				logger.Error(err, "decommissioning pod")
				return syncStatus(err)
			}
			// we scale down each pod one at a time
			return syncStatus(nil)
		}

		if controllerutil.RemoveFinalizer(node, r.config.Finalizer) {
			if err := r.config.Client.Update(ctx, node); err != nil {
				logger.Error(err, "updating node finalizer")
				return ignoreConflict(err)
			}

			if err := r.subscriber.AfterNodeDeleted(ctx, cluster, node); err != nil {
				logger.Error(err, "after node termination (non-retryable)")
			}
		}
		return ctrl.Result{}, nil
	}

	// add a finalizer
	if controllerutil.AddFinalizer(node, r.config.Finalizer) {
		if err := r.config.Client.Update(ctx, node); err != nil {
			logger.Error(err, "updating node finalizer")
			return ignoreConflict(err)
		}
		return ctrl.Result{}, nil
	}

	// sync all my resources
	if err := r.resourceManager.SyncAll(ctx, node); err != nil {
		return syncStatus(err)
	}

	if len(pods) == 0 {
		pod, err := r.createPod(ctx, status, node)
		if err != nil {
			logger.Error(err, "creating pod")
			return syncStatus(err)
		}

		// if the pod becomes immediately ready, mark this as such
		markPodIfReady(pod)

		// we created a pod, that's all we're going to do for now
		return syncStatus(nil)
	}

	if len(pods) > 1 {
		if err := r.decommissionPod(ctx, status, node, pods[0]); err != nil {
			logger.Error(err, "decommissioning pod")
			return syncStatus(err)
		}
		// we delete pods 1 at a time until we only have one left
		return syncStatus(nil)
	}

	// we now know we have a single pod, do the management work for it
	pod := pods[0]

	if !r.podManager.VersionMatches(status.CurrentVersion, pod) {
		if err := r.decommissionPod(ctx, status, node, pod); err != nil {
			logger.Error(err, "decommissioning pod with non-current version")
			return syncStatus(err)
		}
		return syncStatus(nil)
	}

	// if we detect a pod as finished, attempt to restart it
	if r.podManager.IsFailed(pod) || r.podManager.IsSucceeded(pod) {
		if err := r.restartPod(ctx, status, node, pod); err != nil {
			logger.Error(err, "restarting stopped pod")
			return syncStatus(err)
		}

		return syncStatus(nil)
	}

	// if everything worked out and we have a ready pod, then mark the node as such
	markPodIfReady(pod)

	return syncStatus(nil)
}

func (r *ClusterNodeReconciler[T, U, V, PT, PU, PV]) createPod(ctx context.Context, status *clusterv1alpha1.ClusterNodeStatus, node PV) (*corev1.Pod, error) {
	setPhase(node, status, initializingPhase("initializing pod"))

	version, pod, err := r.podManager.CreatePod(ctx, node)
	if err != nil {
		return nil, fmt.Errorf("applying pod: %w", err)
	}

	status.PreviousVersion = status.CurrentVersion
	status.CurrentVersion = version

	return pod, nil
}

func (r *ClusterNodeReconciler[T, U, V, PT, PU, PV]) decommissionPod(ctx context.Context, status *clusterv1alpha1.ClusterNodeStatus, node PV, pod *corev1.Pod) error {
	if r.podManager.IsTerminating(pod) {
		return nil
	}

	setPhase(node, status, decommissioningPhase("decommissioning pod"))
	if err := r.podManager.Delete(ctx, pod); err != nil {
		return fmt.Errorf("deleting pod: %w", err)
	}

	return nil
}

func (r *ClusterNodeReconciler[T, U, V, PT, PU, PV]) restartPod(ctx context.Context, status *clusterv1alpha1.ClusterNodeStatus, node PV, pod *corev1.Pod) error {
	if r.podManager.IsTerminating(pod) {
		return nil
	}

	setPhase(node, status, restartingPhase("restarting pod"))
	if err := r.podManager.Delete(ctx, pod); err != nil {
		return fmt.Errorf("deleting pod: %w", err)
	}

	return nil
}

func (r *ClusterNodeReconciler[T, U, V, PT, PU, PV]) getCluster(ctx context.Context, node PV) (PT, error) {
	cluster := newKubeObject[T, PT]()

	labels := node.GetLabels()
	if labels == nil {
		return nil, nil
	}

	clusterName, ok := labels[r.config.Labels.ClusterLabel]
	if !ok {
		return nil, nil
	}

	if err := r.config.Client.Get(ctx, types.NamespacedName{Namespace: node.GetNamespace(), Name: clusterName}, cluster); err != nil {
		return nil, client.IgnoreNotFound(err)
	}

	return cluster, nil
}
