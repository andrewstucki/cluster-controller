package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"

	clusterv1alpha1 "github.com/andrewstucki/cluster-controller/controller/api/v1alpha1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	k8sapierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type ClusterReconciler[T, U any, PT ptrToObject[T], PU ptrToObject[U]] struct {
	config *Config[T, U, PT, PU]

	nodeManager     *NodeManager[T, U, PT, PU]
	resourceManager *ResourceManager[T, PT]
	subscriber      LifecycleSubscriber[T, U, PT, PU]
}

func setupClusterReconciler[T, U any, PT ptrToObject[T], PU ptrToObject[U]](mgr ctrl.Manager, config *Config[T, U, PT, PU]) error {
	return (&ClusterReconciler[T, U, PT, PU]{
		config:          config,
		nodeManager:     config.clusterNodeManager(),
		resourceManager: config.clusterResourceManager(),
		subscriber:      config.Subscriber,
	}).setupWithManager(mgr)
}

func (r *ClusterReconciler[T, U, PT, PU]) setupWithManager(mgr ctrl.Manager) error {
	builder := ctrl.NewControllerManagedBy(mgr).For(newKubeObject[T, PT]())

	return r.resourceManager.WatchResources(builder).Owns(newKubeObject[U, PU]()).Complete(r)
}

func (r *ClusterReconciler[T, U, PT, PU]) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	cluster := newKubeObject[T, PT]()
	if err := r.config.Client.Get(ctx, req.NamespacedName, cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	nodes, err := r.nodeManager.ListNodes(ctx, cluster)
	if err != nil {
		logger.Error(err, "fetching cluster nodes")
		return ctrl.Result{}, err
	}

	hash, err := r.nodeManager.Hash(cluster)
	if err != nil {
		logger.Error(err, "calculating cluster hash")
		return ctrl.Result{}, err
	}

	originalStatus := r.config.Factory.GetClusterStatus(cluster)
	status := originalStatus.DeepCopy()
	status.ObservedGeneration = cluster.GetGeneration()
	status.Replicas = 0
	status.RunningReplicas = 0
	status.HealthyReplicas = 0
	status.UpToDateReplicas = 0
	status.OutOfDateReplicas = 0

	syncStatus := func(err error) (ctrl.Result, error) {
		if !apiequality.Semantic.DeepEqual(status, &originalStatus) {
			r.config.Factory.SetClusterStatus(cluster, *status)
			syncErr := r.config.Client.Status().Update(ctx, cluster)
			err = errors.Join(syncErr, err)
		}

		return ignoreConflict(err)
	}

	// we are being deleted, clean up everything
	if cluster.GetDeletionTimestamp() != nil {
		if !isTerminatingPhase(status) {
			if err := r.subscriber.BeforeClusterDeleted(ctx, cluster); err != nil {
				logger.Error(err, "before cluster termination")
				return syncStatus(err)
			}
			setPhase(cluster, status, terminatingPhase("cluster terminating"))
		}

		// clean up all my resources
		if deleted, err := r.resourceManager.DeleteAll(ctx, cluster); deleted || err != nil {
			return syncStatus(err)
		}

		if len(nodes) > 0 {
			node := nodes[0]

			if err := r.decommissionNode(ctx, status, cluster, node); err != nil {
				logger.Error(err, "decommissioning node")
				return syncStatus(err)
			}

			// we scale down each node one at a time
			return syncStatus(nil)
		}

		if controllerutil.RemoveFinalizer(cluster, r.config.Finalizer) {
			if err := r.config.Client.Update(ctx, cluster); err != nil {
				logger.Error(err, "updating cluster finalizer")
				return ignoreConflict(err)
			}

			if err := r.subscriber.AfterClusterDeleted(ctx, cluster); err != nil {
				logger.Error(err, "after cluster termination (non-retryable)")
			}
		}

		return ctrl.Result{}, nil
	}

	// add a finalizer
	if controllerutil.AddFinalizer(cluster, r.config.Finalizer) {
		if err := r.config.Client.Update(ctx, cluster); err != nil {
			logger.Error(err, "updating cluster finalizer")
			return ignoreConflict(err)
		}
		return ctrl.Result{}, nil
	}

	// generate NextNode before doing anything
	if status.NextNode == "" {
		generateNextNode(cluster.GetName(), status)
		return syncStatus(nil)
	}

	// sync all my resources
	if err := r.resourceManager.SyncAll(ctx, cluster); err != nil {
		return syncStatus(err)
	}

	// General implementation:
	// 1. grab all nodes for the cluster
	// 2. see if we need to scale down
	// 3. scale down starting with either the newest node or an unhealthy node
	// 4. rolling restart all nodes if we meet the rollout threshold
	// 5. scale up to desired size

	unhealthyNodes := []PU{}
	for _, node := range nodes {
		nodeStatus := ptr.To(r.config.Factory.GetClusterNodeStatus(node))

		if !r.nodeManager.VersionMatches(hash, node) {
			status.OutOfDateReplicas++
		}
		if isNodeUpdated(nodeStatus, hash, r.nodeManager.VersionMatches(hash, node)) {
			status.UpToDateReplicas++
		}
		if isNodeRunning(nodeStatus) {
			status.RunningReplicas++
		}
		if isNodeHealthy(nodeStatus) {
			status.HealthyReplicas++
			unhealthyNodes = append(unhealthyNodes, node)
		}
		status.Replicas++
	}

	desiredReplicas := r.config.Factory.GetClusterReplicas(cluster)
	if len(nodes) > desiredReplicas {
		// we need to decommission the replicas, start with the newest one
		node := nodes[len(nodes)-1]

		// if we have any unhealthy nodes, do those first instead
		if len(unhealthyNodes) > 0 {
			node = unhealthyNodes[0]
		}

		if node.GetDeletionTimestamp() != nil {
			// we've already deleted a node, just wait for now
			return syncStatus(nil)
		}

		if err := r.decommissionNode(ctx, status, cluster, node); err != nil {
			logger.Error(err, "decommissioning node")
			return syncStatus(err)
		}
		// we've deleted a node replica, so try again
		return syncStatus(nil)
	}

	// we've now decommissioned any superfluous nodes, now we handle updating the existing nodes

	// first check if we have a pending update
	for _, node := range nodes {
		nodeStatus := ptr.To(r.config.Factory.GetClusterNodeStatus(node))

		if isNodeUpdating(nodeStatus, hash, r.nodeManager.VersionMatches(hash, node)) {
			// we have a pending update, just wait
			return syncStatus(nil)
		}
	}

	// we don't so roll out any needed change

	for _, node := range nodes {
		if !r.nodeManager.VersionMatches(hash, node) {
			// if we don't meet the minimum healthy threshold, then don't do anything
			minimumReplicas := r.config.Factory.GetClusterMinimumHealthyReplicas(cluster)
			if minimumReplicas > 0 && len(nodes)-len(unhealthyNodes)-1 > minimumReplicas {
				// don't do a rollout because we are below the minimum threshold
				setPhase(cluster, status, gatedPhase("upgrade is gated on minimum replicas"))
				return syncStatus(nil)
			}

			// we have an out-of-date node, so patch the node with the latest changes
			if err := r.updateNode(ctx, status, cluster, node, hash); err != nil {
				logger.Error(err, "updating node")
				return syncStatus(err)
			}
			return syncStatus(nil)
		}
	}

	// we've now decommissioned any superfluous nodes, and rolling restarted, now we handle scale up
	if len(nodes) < desiredReplicas {
		if err := r.createNode(ctx, status, cluster, hash); err != nil {
			logger.Error(err, "creating node")
			return syncStatus(err)
		}
		// we've created a node, so try again
		return syncStatus(nil)
	}

	setPhase(cluster, status, readyPhase("cluster ready"))

	return syncStatus(nil)
}

func (r *ClusterReconciler[T, U, PT, PU]) decommissionNode(ctx context.Context, status *clusterv1alpha1.ClusterStatus, cluster PT, node PU) error {
	if node.GetDeletionTimestamp() != nil {
		return nil
	}

	setPhase(cluster, status, decommissioningPhase(fmt.Sprintf("decommissioning node: \"%s/%s\"", node.GetNamespace(), node.GetName())))

	if err := r.subscriber.BeforeNodeDecommissioned(ctx, cluster, node); err != nil {
		return fmt.Errorf("running node before delete hook: %w", err)
	}

	if err := r.config.Client.Delete(ctx, node); err != nil {
		if !k8sapierrors.IsNotFound(err) {
			return fmt.Errorf("deleting node: %w", err)
		}
	}

	status.Nodes = slices.DeleteFunc(status.Nodes, func(name string) bool {
		return node.GetName() == name
	})

	return nil
}

func (r *ClusterReconciler[T, U, PT, PU]) createNode(ctx context.Context, status *clusterv1alpha1.ClusterStatus, cluster PT, hash string) error {
	node, err := r.nodeManager.CreateNode(ctx, status, cluster, hash)

	setPhase(cluster, status, initializingPhase(fmt.Sprintf("creating node \"%s/%s\"", node.GetNamespace(), node.GetName())))

	if err != nil {
		return fmt.Errorf("creating node: %w", err)
	}

	status.Nodes = append(status.Nodes, node.GetName())

	return nil
}

func (r *ClusterReconciler[T, U, PT, PU]) updateNode(ctx context.Context, status *clusterv1alpha1.ClusterStatus, cluster PT, node PU, hash string) error {
	setPhase(cluster, status, updatingPhase(fmt.Sprintf("updating node \"%s/%s\"", node.GetNamespace(), node.GetName())))

	if err := r.nodeManager.UpdateNode(ctx, cluster, hash, node); err != nil {
		return fmt.Errorf("updating node: %w", err)
	}

	return nil
}

func isNodeUpdating(status *clusterv1alpha1.ClusterNodeStatus, hash string, matchesVersion bool) bool {
	return matchesVersion && (status.ClusterVersion != hash || !status.MatchesCluster)
}

func isNodeUpdated(status *clusterv1alpha1.ClusterNodeStatus, hash string, matchesVersion bool) bool {
	return matchesVersion && status.ClusterVersion == hash && status.MatchesCluster
}

func isNodeHealthy(status *clusterv1alpha1.ClusterNodeStatus) bool {
	return status.Healthy
}

func isNodeRunning(status *clusterv1alpha1.ClusterNodeStatus) bool {
	return status.Running
}

func generateNextNode(clusterName string, status *clusterv1alpha1.ClusterStatus) string {
	previousNode := status.NextNode
	status.NextNode = clusterName + fmt.Sprintf("-%s", rand.String(8))
	return previousNode
}
