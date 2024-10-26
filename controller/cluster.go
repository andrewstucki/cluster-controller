package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"

	clusterv1alpha1 "github.com/andrewstucki/cluster-controller/api/v1alpha1"
	k8sapierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type ClusterReconciler[T, U any, PT ptrToObject[T], PU ptrToObject[U]] struct {
	config *Config[T, U, PT, PU]
}

func SetupClusterReconciler[T, U any, PT ptrToObject[T], PU ptrToObject[U]](mgr ctrl.Manager, config *Config[T, U, PT, PU]) error {
	return (&ClusterReconciler[T, U, PT, PU]{
		config: config,
	}).setupWithManager(mgr)
}

func (r *ClusterReconciler[T, U, PT, PU]) setupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(newKubeObject[T, PT]()).
		Owns(newKubeObject[U, PU]()).
		Complete(r)
}

func (r *ClusterReconciler[T, U, PT, PU]) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	cluster := newKubeObject[T, PT]()
	if err := r.config.Client.Get(ctx, req.NamespacedName, cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	nodes, err := r.getClusterNodes(ctx, cluster.GetNamespace(), cluster.GetName())
	if err != nil {
		logger.Error(err, "fetching cluster nodes")
		return ctrl.Result{}, err
	}

	hash, err := r.config.Manager.HashCluster(cluster)
	if err != nil {
		logger.Error(err, "calculating cluster node hash")
		return ctrl.Result{}, err
	}

	originalStatus := r.config.Manager.GetClusterStatus(cluster)
	status := originalStatus.DeepCopy()
	status.ObservedGeneration = cluster.GetGeneration()
	status.Replicas = 0
	status.RunningReplicas = 0
	status.HealthyReplicas = 0
	status.UpToDateReplicas = 0
	status.OutOfDateReplicas = 0

	syncStatus := func(err error) (ctrl.Result, error) {
		updated := isClusterStatusDirty(&originalStatus, status)
		for _, condition := range []metav1.Condition{} {
			if meta.SetStatusCondition(&status.Conditions, condition) {
				updated = true
			}
		}

		if updated {
			r.config.Manager.SetClusterStatus(cluster, *status)
			syncErr := r.config.Client.Status().Update(ctx, cluster)
			err = errors.Join(syncErr, err)
		}

		return ignoreConflict(err)
	}

	// we are being deleted, clean up everything
	if cluster.GetDeletionTimestamp() != nil {
		modifying := &ClusterObjects[T, U, PT, PU]{Cluster: cluster}

		if !isTerminatingPhase(status) {
			if err := runSubscriberCallback(ctx, r.config.Manager, modifying, beforeDeleteCallback); err != nil {
				logger.Error(err, "before cluster termination")
				return syncStatus(err)
			}
			setPhase(cluster, status, terminatingPhase("cluster terminating"))
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

			if err := runSubscriberCallback(ctx, r.config.Manager, modifying, afterDeleteCallback); err != nil {
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

	// General implementation:
	// 1. grab all nodes for the cluster
	// 2. see if we need to scale down
	// 3. scale down starting with either the newest node or an unhealthy node
	// 4. rolling restart all nodes if we meet the rollout threshold
	// 5. scale up to desired size

	unhealthyNodes := []PU{}
	for _, node := range nodes {
		nodeStatus := ptr.To(r.config.Manager.GetClusterNodeStatus(node))

		if isNodeStale(node, r.config.HashLabel, hash) {
			status.OutOfDateReplicas++
		}
		if isNodeUpdated(nodeStatus, node, r.config.HashLabel, hash) {
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

	desiredReplicas := r.config.Manager.GetClusterReplicas(cluster)
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
		nodeStatus := ptr.To(r.config.Manager.GetClusterNodeStatus(node))

		if isNodeUpdating(nodeStatus, node, r.config.HashLabel, hash) {
			// we have a pending update, just wait
			return syncStatus(nil)
		}
	}

	// we don't so roll out any needed change

	for _, node := range nodes {
		if getHash(r.config.HashLabel, node) != hash {
			// if we don't meet the minimum healthy threshold, then don't do anything
			minimumReplicas := r.config.Manager.GetClusterMinimumHealthyReplicas(cluster)
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
	// don't attempt to delete anything if it's being deleted already
	if node.GetDeletionTimestamp() != nil {
		return nil
	}

	setPhase(cluster, status, decommissioningPhase(fmt.Sprintf("decommissioning node: \"%s/%s\"", node.GetNamespace(), node.GetName())))

	modifying := &ClusterObjects[T, U, PT, PU]{Cluster: cluster, Node: node}
	if err := runNodeSubscriberCallback(ctx, r.config.Manager, modifying, beforeDecommissionCallback); err != nil {
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

	if err := runNodeSubscriberCallback(ctx, r.config.Manager, modifying, afterDecommissionCallback); err != nil {
		return fmt.Errorf("running node after delete hook: %w", err)
	}

	return nil
}

func (r *ClusterReconciler[T, U, PT, PU]) createNode(ctx context.Context, status *clusterv1alpha1.ClusterStatus, cluster PT, hash string) error {
	node := r.config.Manager.GetClusterNodeTemplate(cluster)
	r.initNode(r.config.ClusterLabel, r.config.HashLabel, status, cluster, node, hash)

	setPhase(cluster, status, initializingPhase(fmt.Sprintf("creating node \"%s/%s\"", node.GetNamespace(), node.GetName())))

	modifying := &ClusterObjects[T, U, PT, PU]{Cluster: cluster, Node: node}
	if err := runNodeSubscriberCallback(ctx, r.config.Manager, modifying, beforeCreateCallback); err != nil {
		return fmt.Errorf("running node before create hook: %w", err)
	}

	if err := r.config.Client.Create(ctx, node); err != nil {
		if k8sapierrors.IsAlreadyExists(err) {
			return nil
		}
		return fmt.Errorf("creating node: %w", err)
	}

	status.Nodes = append(status.Nodes, node.GetName())

	if err := runNodeSubscriberCallback(ctx, r.config.Manager, modifying, afterCreateCallback); err != nil {
		return fmt.Errorf("running node after create hook: %w", err)
	}

	return nil
}

func (r *ClusterReconciler[T, U, PT, PU]) updateNode(ctx context.Context, status *clusterv1alpha1.ClusterStatus, cluster PT, node PU, hash string) error {
	updated := r.config.Manager.GetClusterNodeTemplate(cluster)
	r.initNode(r.config.ClusterLabel, r.config.HashLabel, status, cluster, updated, hash)
	updated.SetName(node.GetName())

	// set the spec from the updated node if the node has a Spec field
	specValue := reflect.ValueOf(node).Elem().FieldByName("Spec")
	if specValue.IsValid() && specValue.CanSet() {
		specValue.Set(reflect.ValueOf(updated).Elem().FieldByName("Spec"))
	}

	labels := node.GetLabels()
	labels[r.config.HashLabel] = hash
	node.SetLabels(labels)

	setPhase(cluster, status, updatingPhase(fmt.Sprintf("updating node \"%s/%s\"", node.GetNamespace(), node.GetName())))

	modifying := &ClusterObjects[T, U, PT, PU]{Cluster: cluster, Node: node}
	if err := runNodeSubscriberCallback(ctx, r.config.Manager, modifying, beforeUpdateCallback); err != nil {
		return fmt.Errorf("running node before update hook: %w", err)
	}

	if err := r.config.Client.Update(ctx, node); err != nil {
		return fmt.Errorf("updating node: %w", err)
	}

	if err := runNodeSubscriberCallback(ctx, r.config.Manager, modifying, afterUpdateCallback); err != nil {
		return fmt.Errorf("running node after update hook: %w", err)
	}

	return nil
}

func (r *ClusterReconciler[T, U, PT, PU]) initNode(clusterLabel, hashLabel string, status *clusterv1alpha1.ClusterStatus, cluster PT, node PU, hash string) {
	node.SetName(generateNextNode(cluster.GetName(), status))
	node.SetNamespace(cluster.GetNamespace())

	kinds, _, _ := r.config.Scheme.ObjectKinds(node)
	kind := kinds[0]

	node.GetObjectKind().SetGroupVersionKind(kind)
	node.SetOwnerReferences([]metav1.OwnerReference{*metav1.NewControllerRef(cluster, cluster.GetObjectKind().GroupVersionKind())})

	labels := node.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}

	labels[hashLabel] = hash
	labels[clusterLabel] = cluster.GetName()
	node.SetLabels(labels)
}

func (r *ClusterReconciler[T, U, PT, PU]) getClusterNodes(ctx context.Context, namespace, clusterName string) ([]PU, error) {
	kinds, _, err := r.config.Scheme.ObjectKinds(newKubeObject[U, PU]())
	if err != nil {
		return nil, fmt.Errorf("fetching object kind: %w", err)
	}
	if len(kinds) == 0 {
		return nil, fmt.Errorf("unable to determine object kind")
	}
	kind := kinds[0]
	kind.Kind += "List"

	o, err := r.config.Scheme.New(kind)
	if err != nil {
		return nil, fmt.Errorf("initializing list: %w", err)
	}
	list, ok := o.(client.ObjectList)
	if !ok {
		return nil, fmt.Errorf("invalid object list type: %T", o)
	}

	if err := r.config.Client.List(ctx, list, client.InNamespace(namespace), client.MatchingLabels{
		r.config.ClusterLabel: clusterName,
	}); err != nil {
		return nil, fmt.Errorf("listing cluster nodes: %w", err)
	}

	converted := []PU{}

	items := reflect.ValueOf(list).Elem().FieldByName("Items")
	if items.IsZero() {
		return nil, fmt.Errorf("unable to get cluster node items")
	}
	for i := 0; i < items.Len(); i++ {
		item := items.Index(i).Interface()
		node, ok := item.(U)
		if !ok {
			return nil, fmt.Errorf("unable to convert cluster node item of type %T", item)
		}
		converted = append(converted, ptr.To(node))
	}

	return sortCreation(converted), nil
}

func isClusterStatusDirty(a, b *clusterv1alpha1.ClusterStatus) bool {
	return a.ObservedGeneration != b.ObservedGeneration || a.Replicas != b.Replicas || a.UpToDateReplicas != b.UpToDateReplicas || a.Phase != b.Phase || a.HealthyReplicas != b.HealthyReplicas || a.RunningReplicas != b.RunningReplicas
}

func isNodeUpdating(status *clusterv1alpha1.ClusterNodeStatus, node client.Object, hashLabel, hash string) bool {
	return getHash(hashLabel, node) == hash && (status.ClusterVersion != hash || !status.MatchesCluster)
}

func isNodeUpdated(status *clusterv1alpha1.ClusterNodeStatus, node client.Object, hashLabel, hash string) bool {
	return getHash(hashLabel, node) == hash && status.ClusterVersion == hash && status.MatchesCluster
}

func isNodeStale(node client.Object, hashLabel, hash string) bool {
	return getHash(hashLabel, node) != hash
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
