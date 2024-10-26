package controller

import (
	"context"
	"errors"
	"fmt"
	"sort"

	clusterv1alpha1 "github.com/andrewstucki/cluster-controller/controller/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	k8sapierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type ClusterNodeReconciler[T, U any, PT ptrToObject[T], PU ptrToObject[U]] struct {
	config *Config[T, U, PT, PU]
}

func SetupClusterNodeReconciler[T, U any, PT ptrToObject[T], PU ptrToObject[U]](ctx context.Context, mgr ctrl.Manager, config *Config[T, U, PT, PU]) error {
	return (&ClusterNodeReconciler[T, U, PT, PU]{
		config: config,
	}).setupWithManager(ctx, mgr)
}

func (r *ClusterNodeReconciler[T, U, PT, PU]) podOwnerIndex() string {
	return r.config.IndexPrefix + ".pod" + ownerIndex
}

func (r *ClusterNodeReconciler[T, U, PT, PU]) pvcOwnerIndex() string {
	return r.config.IndexPrefix + ".pvc" + ownerIndex
}

func (r *ClusterNodeReconciler[T, U, PT, PU]) setupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	kind, err := getRegisteredSchemaKind(r.config.Scheme, newKubeObject[U, PU]())
	if err != nil {
		return err
	}

	if err := indexOwner[corev1.Pod](ctx, mgr, kind, r.podOwnerIndex()); err != nil {
		return err
	}

	if err := indexOwner[corev1.PersistentVolumeClaim](ctx, mgr, kind, r.pvcOwnerIndex()); err != nil {
		return err
	}

	return watchResources(
		ctrl.NewControllerManagedBy(mgr).For(newKubeObject[U, PU]()),
		r.config.Manager, ResourceKindNode, r.config.NamespaceLabel, r.config.NodeLabel, r.ownerTypeLabels(),
	).Owns(&corev1.Pod{}).Complete(r)
}

func (r *ClusterNodeReconciler[T, U, PT, PU]) matchingLabels(cluster PT, node PU) map[string]string {
	return map[string]string{
		r.config.ClusterLabel:   cluster.GetName(),
		r.config.NodeLabel:      node.GetName(),
		r.config.NamespaceLabel: node.GetNamespace(),
		r.config.OwnerTypeLabel: "node",
	}
}

func (r *ClusterNodeReconciler[T, U, PT, PU]) ownerTypeLabels() map[string]string {
	return map[string]string{
		r.config.OwnerTypeLabel: "node",
	}
}

func (r *ClusterNodeReconciler[T, U, PT, PU]) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	deploymentFirstStabilized := false

	logger := log.FromContext(ctx)

	node := newKubeObject[U, PU]()
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

	pods, err := r.getClusterPods(ctx, req.String())
	if err != nil {
		logger.Error(err, "fetching cluster pods")
		return ctrl.Result{}, err
	}

	originalStatus := r.config.Manager.GetClusterNodeStatus(node)
	status := originalStatus.DeepCopy()
	status.ObservedGeneration = node.GetGeneration()

	// set our cluster version immediately
	status.ClusterVersion = getHash(r.config.HashLabel, node)
	// if we don't match, overwrite our current matching status
	// which will only get updated when we've fully stabilized
	if originalStatus.ClusterVersion != status.ClusterVersion {
		status.MatchesCluster = false
	}

	markPodIfReady := func(pod *corev1.Pod) {
		status.Healthy = isHealthy(r.config.Testing, pod)
		status.Running = isRunningAndReady(r.config.Testing, pod)

		if isHealthy(r.config.Testing, pod) {
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
			objects := &ClusterObjects[T, U, PT, PU]{Cluster: cluster, Node: node}
			if err := runNodeSubscriberCallback(ctx, r.config.Manager, objects, afterStabilizedCallback); err != nil {
				logger.Error(err, "running node stabilized callback")
				return ctrl.Result{Requeue: true}, nil
			}
		}

		if !apiequality.Semantic.DeepEqual(status, &originalStatus) {
			r.config.Manager.SetClusterNodeStatus(node, *status)
			syncErr := r.config.Client.Status().Update(ctx, node)
			err = errors.Join(syncErr, err)
		}

		return ignoreConflict(err)
	}

	// we are being deleted, clean up everything
	if node.GetDeletionTimestamp() != nil {
		modifying := &ClusterObjects[T, U, PT, PU]{Cluster: cluster, Node: node}

		if !isTerminatingPhase(status) {
			if err := runNodeSubscriberCallback(ctx, r.config.Manager, modifying, beforeDeleteCallback); err != nil {
				logger.Error(err, "before node termination")
				return syncStatus(err)
			}
			setPhase(node, status, terminatingPhase("node terminating"))
		}

		// delete all my resources
		if deleted, err := deleteAllResources(ctx, r.config.Client, r.config.Manager, ResourceKindNode, r.matchingLabels(cluster, node)); deleted || err != nil {
			return syncStatus(err)
		}

		if len(pods) > 0 {
			if err := r.decommissionPod(ctx, status, cluster, node, pods[0]); err != nil {
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

			if err := runNodeSubscriberCallback(ctx, r.config.Manager, modifying, afterDeleteCallback); err != nil {
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
	if err := syncAllResources(ctx, r.config.Client, r.config.Manager, ResourceKindNode, r.config.FieldOwner, node, r.matchingLabels(cluster, node)); err != nil {
		return syncStatus(err)
	}

	if len(pods) == 0 {
		pod, err := r.createPod(ctx, status, cluster, node)
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
		if err := r.decommissionPod(ctx, status, cluster, node, pods[0]); err != nil {
			logger.Error(err, "decommissioning pod")
			return syncStatus(err)
		}
		// we delete pods 1 at a time until we only have one left
		return syncStatus(nil)
	}

	// we now know we have a single pod, do the management work for it
	pod := pods[0]

	podVersion := getHash(r.config.HashLabel, pod)
	if podVersion == "" || podVersion != status.CurrentVersion {
		if err := r.decommissionPod(ctx, status, cluster, node, pod); err != nil {
			logger.Error(err, "decommissioning pod with non-current version", "version", podVersion)
			return syncStatus(err)
		}
		return syncStatus(nil)
	}

	// if we detect a pod as finished, attempt to restart it
	if isFailed(r.config.Testing, pod) || isSucceeded(r.config.Testing, pod) {
		if err := r.restartPod(ctx, status, cluster, node, pod); err != nil {
			logger.Error(err, "restarting stopped pod")
			return syncStatus(err)
		}

		return syncStatus(nil)
	}

	// if everything worked out and we have a ready pod, then mark the node as such
	markPodIfReady(pod)

	return syncStatus(nil)
}

func (r *ClusterNodeReconciler[T, U, PT, PU]) createPod(ctx context.Context, status *clusterv1alpha1.ClusterNodeStatus, cluster PT, node PU) (*corev1.Pod, error) {
	// we set the phase here in case of an early return due to an error
	setPhase(node, status, initializingPhase("generating pod"))

	pod := r.config.Manager.GetClusterNodePod(node)

	version, err := r.initPod(node, pod)
	if err != nil {
		return nil, fmt.Errorf("initializing pod template: %w", err)
	}

	message := fmt.Sprintf("initializing pod \"%s/%s\"", pod.Namespace, pod.Name)
	setPhase(node, status, initializingPhase(message))

	modifying := &ClusterObjects[T, U, PT, PU]{Cluster: cluster, Node: node, Pod: pod}

	if err := runNodePodSubscriberCallback(ctx, r.config.Manager, modifying, beforeCreateCallback); err != nil {
		return nil, fmt.Errorf("running pod before create hook: %w", err)
	}

	if err := r.config.Client.Patch(ctx, pod, client.Apply, r.config.FieldOwner, client.ForceOwnership); err != nil {
		return nil, fmt.Errorf("applying pod: %w", err)
	}

	status.PreviousVersion = status.CurrentVersion
	status.CurrentVersion = version

	if err := runNodePodSubscriberCallback(ctx, r.config.Manager, modifying, afterCreateCallback); err != nil {
		return nil, fmt.Errorf("running pod after create hook: %w", err)
	}

	return pod, nil
}

func (r *ClusterNodeReconciler[T, U, PT, PU]) decommissionPod(ctx context.Context, status *clusterv1alpha1.ClusterNodeStatus, cluster PT, node PU, pod *corev1.Pod) error {
	if pod.DeletionTimestamp != nil {
		return nil
	}

	setPhase(node, status, decommissioningPhase(fmt.Sprintf("decommissioning pod: %s/%s", pod.Namespace, pod.Name)))

	modifying := &ClusterObjects[T, U, PT, PU]{Cluster: cluster, Node: node, Pod: pod}
	if err := runNodePodSubscriberCallback(ctx, r.config.Manager, modifying, beforeDeleteCallback); err != nil {
		return fmt.Errorf("running pod before delete hook: %w", err)
	}

	if err := r.config.Client.Delete(ctx, pod); err != nil {
		if !k8sapierrors.IsNotFound(err) {
			return fmt.Errorf("deleting pod: %w", err)
		}
	}

	if err := runNodePodSubscriberCallback(ctx, r.config.Manager, modifying, afterDeleteCallback); err != nil {
		return fmt.Errorf("running pod after delete hook: %w", err)
	}

	return nil
}

func (r *ClusterNodeReconciler[T, U, PT, PU]) restartPod(ctx context.Context, status *clusterv1alpha1.ClusterNodeStatus, cluster PT, node PU, pod *corev1.Pod) error {
	setPhase(node, status, restartingPhase(fmt.Sprintf("restarting pod: %s/%s", pod.Namespace, pod.Name)))

	modifying := &ClusterObjects[T, U, PT, PU]{Cluster: cluster, Node: node, Pod: pod}
	if err := runNodePodSubscriberCallback(ctx, r.config.Manager, modifying, beforeDeleteCallback); err != nil {
		return fmt.Errorf("running pod before delete hook on restart: %w", err)
	}

	if err := r.config.Client.Delete(ctx, pod); err != nil {
		if !k8sapierrors.IsNotFound(err) {
			return fmt.Errorf("restarting pod: %w", err)
		}
	}

	if err := runNodePodSubscriberCallback(ctx, r.config.Manager, modifying, afterDeleteCallback); err != nil {
		return fmt.Errorf("running pod after delete hook on restart: %w", err)
	}

	return nil
}

func (r *ClusterNodeReconciler[T, U, PT, PU]) getCluster(ctx context.Context, node PU) (PT, error) {
	cluster := newKubeObject[T, PT]()

	labels := node.GetLabels()
	if labels == nil {
		return nil, nil
	}

	clusterName, ok := labels[r.config.ClusterLabel]
	if !ok {
		return nil, nil
	}

	if err := r.config.Client.Get(ctx, types.NamespacedName{Namespace: node.GetNamespace(), Name: clusterName}, cluster); err != nil {
		return nil, client.IgnoreNotFound(err)
	}

	return cluster, nil
}

func (r *ClusterNodeReconciler[T, U, PT, PU]) getClusterPods(ctx context.Context, nodeID string) ([]*corev1.Pod, error) {
	var pods corev1.PodList
	if err := r.config.Client.List(ctx, &pods, &client.ListOptions{
		FieldSelector: fields.OneTermEqualSelector(r.podOwnerIndex(), nodeID),
	}); err != nil {
		return nil, err
	}

	items := []*corev1.Pod{}
	for _, item := range pods.Items {
		items = append(items, ptr.To(item))
	}

	return sortCreation(items), nil
}

func sortCreation[T client.Object](objects []T) []T {
	sort.SliceStable(objects, func(i, j int) bool {
		a, b := objects[i], objects[j]
		aTimestamp, bTimestamp := ptr.To(a.GetCreationTimestamp()), ptr.To(b.GetCreationTimestamp())
		if aTimestamp.Equal(bTimestamp) {
			return a.GetName() < b.GetName()
		}
		return aTimestamp.Before(bTimestamp)
	})
	return objects
}

func (r *ClusterNodeReconciler[T, U, PT, PU]) initPod(node PU, pod *corev1.Pod) (string, error) {
	hash, err := r.config.Manager.HashClusterNode(node)
	if err != nil {
		return "", fmt.Errorf("hashing node: %w", err)
	}

	if pod.Labels == nil {
		pod.Labels = map[string]string{}
	}

	pod.TypeMeta = metav1.TypeMeta{
		Kind:       "Pod",
		APIVersion: corev1.SchemeGroupVersion.String(),
	}
	pod.Name = node.GetName()
	pod.Namespace = node.GetNamespace()
	pod.Labels[r.config.HashLabel] = hash
	pod.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(node, node.GetObjectKind().GroupVersionKind())}

	return hash, nil
}

func getHash(hashLabel string, o client.Object) string {
	labels := o.GetLabels()
	if labels == nil {
		return ""
	}
	return labels[hashLabel]
}

type phasedStatus interface {
	SetPhase(phase clusterv1alpha1.Phase)
	GetPhase() clusterv1alpha1.Phase
}

func setPhase(o client.Object, status phasedStatus, phase clusterv1alpha1.Phase) {
	if isTerminatingPhase(status) {
		// Terminating is a final phase, so nothing can override it
		return
	}

	if status.GetPhase().Name == phase.Name && status.GetPhase().Message == phase.Message && status.GetPhase().ObservedGeneration == o.GetGeneration() {
		return
	}

	phase.LastTransitionTime = metav1.Now()
	phase.ObservedGeneration = o.GetGeneration()
	status.SetPhase(phase)
}

func decommissioningPhase(message string) clusterv1alpha1.Phase {
	return clusterv1alpha1.Phase{
		Name:    "Decommissioning",
		Message: message,
	}
}

func gatedPhase(message string) clusterv1alpha1.Phase {
	return clusterv1alpha1.Phase{
		Name:    "Gated",
		Message: message,
	}
}

func terminatingPhase(message string) clusterv1alpha1.Phase {
	return clusterv1alpha1.Phase{
		Name:    "Terminating",
		Message: message,
	}
}

func isTerminatingPhase(status phasedStatus) bool {
	return status.GetPhase().Name == "Terminating"
}

func restartingPhase(message string) clusterv1alpha1.Phase {
	return clusterv1alpha1.Phase{
		Name:    "Restarting",
		Message: message,
	}
}

func updatingPhase(message string) clusterv1alpha1.Phase {
	return clusterv1alpha1.Phase{
		Name:    "Updating",
		Message: message,
	}
}

func readyPhase(message string) clusterv1alpha1.Phase {
	return clusterv1alpha1.Phase{
		Name:    "Ready",
		Message: message,
	}
}

func initializingPhase(message string) clusterv1alpha1.Phase {
	return clusterv1alpha1.Phase{
		Name:    "Initializing",
		Message: message,
	}
}
