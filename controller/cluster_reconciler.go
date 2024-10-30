package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"

	clusterv1alpha1 "github.com/andrewstucki/cluster-controller/controller/api/v1alpha1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	k8sapierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type ClusterReconciler[T, U, V any, PT ptrToObject[T], PU ptrToObject[U], PV ptrToObject[V]] struct {
	config *Config[T, U, V, PT, PU, PV]

	poolOwnerFromLabels func(m map[string]string) types.NamespacedName
	nodeManager         *NodeManager[T, U, V, PT, PU, PV]
	pooled              bool
	poolManager         PoolManager[T, U, PT, PU]
	resourceManager     *ResourceManager[T, PT]
	subscriber          LifecycleSubscriber[T, V, PT, PV]
}

func setupClusterReconciler[T, U, V any, PT ptrToObject[T], PU ptrToObject[U], PV ptrToObject[V]](mgr ctrl.Manager, config *Config[T, U, V, PT, PU, PV]) error {
	return (&ClusterReconciler[T, U, V, PT, PU, PV]{
		config:              config,
		poolOwnerFromLabels: config.poolOwnerFromLabels,
		nodeManager:         config.clusterNodeManager(),
		resourceManager:     config.clusterResourceManager(),
		pooled:              config.Pooled,
		poolManager:         config.PoolManager,
		subscriber:          config.Subscriber,
	}).setupWithManager(mgr)
}

func (r *ClusterReconciler[T, U, V, PT, PU, PV]) setupWithManager(mgr ctrl.Manager) error {
	builder := ctrl.NewControllerManagedBy(mgr).For(newKubeObject[T, PT]())

	if r.pooled {
		builder = builder.Watches(newKubeObject[U, PU](), handler.EnqueueRequestsFromMapFunc(func(_ context.Context, o client.Object) []reconcile.Request {
			return []reconcile.Request{{NamespacedName: r.config.Factory.GetClusterForPool(o.(PU))}}
		}))
	}

	return r.resourceManager.WatchResources(builder).
		Owns(newKubeObject[V, PV]()).
		Complete(r)
}

func (r *ClusterReconciler[T, U, V, PT, PU, PV]) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	cluster := newKubeObject[T, PT]()
	if err := r.config.Client.Get(ctx, req.NamespacedName, cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	pools, err := r.poolManager.ListPools(ctx, cluster)
	if err != nil {
		logger.Error(err, "fetching cluster pools")
		return ctrl.Result{}, err
	}

	hashes := map[types.NamespacedName]string{}
	for _, pool := range pools {
		hash, err := r.nodeManager.Hash(pool)
		if err != nil {
			logger.Error(err, "calculating pool hash")
			return ctrl.Result{}, err
		}
		hashes[client.ObjectKeyFromObject(pool)] = hash
	}

	nodes, err := r.nodeManager.ListNodes(ctx, cluster)
	if err != nil {
		logger.Error(err, "fetching cluster nodes")
		return ctrl.Result{}, err
	}

	nodesByPool := map[types.NamespacedName][]PV{}
	for _, node := range nodes {
		pool := r.poolOwnerFromLabels(node.GetLabels())
		nodesByPool[pool] = append(nodesByPool[pool], node)
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

	defunctNodes := []PV{}
	unhealthyNodesByPool := map[types.NamespacedName][]PV{}
	healthyNodesByPool := map[types.NamespacedName][]PV{}
	for _, node := range nodes {
		pool := r.poolOwnerFromLabels(node.GetLabels())

		hash, found := hashes[pool]
		if !found {
			defunctNodes = append(defunctNodes, node)
			status.DefunctReplicas++
			status.Replicas++
			continue
		}

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
			healthyNodesByPool[pool] = append(healthyNodesByPool[pool], node)
		}
		if !isNodeHealthy(nodeStatus) {
			unhealthyNodesByPool[pool] = append(unhealthyNodesByPool[pool], node)
		}
		status.Replicas++
	}

	// if we have any defunct nodes, decommission them
	if len(defunctNodes) > 0 {
		if err := r.decommissionNode(ctx, status, cluster, defunctNodes[0]); err != nil {
			logger.Error(err, "decommissioning node")
			return syncStatus(err)
		}
		// we've deleted a node, so re-reconcile
		return syncStatus(nil)
	}

	for _, pool := range pools {
		poolID := client.ObjectKeyFromObject(pool)
		healthyNodes := healthyNodesByPool[poolID]
		unhealthyNodes := unhealthyNodesByPool[poolID]

		desiredReplicas := r.config.Factory.GetClusterPoolReplicas(pool)
		if len(healthyNodes)+len(unhealthyNodes) > desiredReplicas {
			var node PV

			if len(unhealthyNodes) > 0 {
				// we need to decommission the replicas, start anything unhealthy
				node = unhealthyNodes[len(unhealthyNodes)-1]
			} else {
				// otherwise use healthy nodes
				node = healthyNodes[len(healthyNodes)-1]
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
	}

	// we've now decommissioned any superfluous nodes, now we handle updating the existing nodes

	// first check if we have a pending update
	for _, pool := range pools {
		poolID := client.ObjectKeyFromObject(pool)
		hash := hashes[poolID]
		for _, node := range nodesByPool[poolID] {
			nodeStatus := ptr.To(r.config.Factory.GetClusterNodeStatus(node))

			if isNodeUpdating(nodeStatus, hash, r.nodeManager.VersionMatches(hash, node)) {
				// we have a pending update, just wait
				return syncStatus(nil)
			}
		}
	}

	// we don't so roll out any needed change

	for _, pool := range pools {
		poolID := client.ObjectKeyFromObject(pool)
		hash := hashes[poolID]
		for _, node := range nodesByPool[poolID] {
			if !r.nodeManager.VersionMatches(hash, node) {
				// if we don't meet the minimum healthy threshold, then don't do anything
				minimumReplicas := r.config.Factory.GetClusterMinimumHealthyReplicas(cluster)
				if minimumReplicas > 0 && status.HealthyReplicas-1 > minimumReplicas {
					// don't do a rollout because we are below the minimum threshold
					setPhase(cluster, status, gatedPhase("upgrade is gated on minimum replicas"))
					return syncStatus(nil)
				}

				// we have an out-of-date node, so patch the node with the latest changes
				if err := r.updateNode(ctx, status, cluster, pool, node, hash); err != nil {
					logger.Error(err, "updating node")
					return syncStatus(err)
				}
				return syncStatus(nil)
			}
		}
	}

	// we've now decommissioned any superfluous nodes, and rolling restarted, now we handle scale up
	for _, pool := range pools {
		poolID := client.ObjectKeyFromObject(pool)
		hash := hashes[poolID]
		nodes := nodesByPool[poolID]

		desiredReplicas := r.config.Factory.GetClusterPoolReplicas(pool)
		if len(nodes) < desiredReplicas {
			if err := r.createNode(ctx, status, cluster, pool, hash); err != nil {
				logger.Error(err, "creating node")
				return syncStatus(err)
			}
			// we've created a node, so try again
			return syncStatus(nil)
		}
	}

	setPhase(cluster, status, readyPhase("cluster ready"))

	return syncStatus(nil)
}

func (r *ClusterReconciler[T, U, V, PT, PU, PV]) decommissionNode(ctx context.Context, status *clusterv1alpha1.ClusterStatus, cluster PT, node PV) error {
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

func (r *ClusterReconciler[T, U, V, PT, PU, PV]) createNode(ctx context.Context, status *clusterv1alpha1.ClusterStatus, cluster PT, pool PU, hash string) error {
	node, err := r.nodeManager.CreateNode(ctx, status, cluster, pool, hash)

	setPhase(cluster, status, initializingPhase(fmt.Sprintf("creating node \"%s/%s\"", node.GetNamespace(), node.GetName())))

	if err != nil {
		return fmt.Errorf("creating node: %w", err)
	}

	status.Nodes = append(status.Nodes, node.GetName())

	return nil
}

func (r *ClusterReconciler[T, U, V, PT, PU, PV]) updateNode(ctx context.Context, status *clusterv1alpha1.ClusterStatus, cluster PT, pool PU, node PV, hash string) error {
	setPhase(cluster, status, updatingPhase(fmt.Sprintf("updating node \"%s/%s\"", node.GetNamespace(), node.GetName())))

	if err := r.nodeManager.UpdateNode(ctx, cluster, hash, pool, node); err != nil {
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
