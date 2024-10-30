package controller

import (
	"context"
	"errors"
	"fmt"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const poolClusterIndex = "__pool.__cluster"

type ClusterPoolReconciler[T, U, V any, PT ptrToObject[T], PU ptrToObject[U], PV ptrToObject[V]] struct {
	config *Config[T, U, V, PT, PU, PV]

	nodeManager     *NodeManager[T, U, V, PT, PU, PV]
	resourceManager *ResourceManager[U, PU]
}

func setupClusterPoolReconciler[T, U, V any, PT ptrToObject[T], PU ptrToObject[U], PV ptrToObject[V]](ctx context.Context, mgr ctrl.Manager, config *Config[T, U, V, PT, PU, PV]) error {
	return (&ClusterPoolReconciler[T, U, V, PT, PU, PV]{
		config:          config,
		resourceManager: config.poolResourceManager(),
		nodeManager:     config.clusterNodeManager(),
	}).setupWithManager(ctx, mgr)
}

func (r *ClusterPoolReconciler[T, U, V, PT, PU, PV]) setupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	indexer := mgr.GetFieldIndexer()

	err := indexer.IndexField(ctx, newKubeObject[U, PU](), poolClusterIndex, func(o client.Object) []string {
		return []string{r.config.Factory.GetClusterForPool(o.(PU)).String()}
	})
	if err != nil {
		return fmt.Errorf("adding field index: %w", err)
	}

	builder := ctrl.NewControllerManagedBy(mgr).For(newKubeObject[U, PU]())

	return r.resourceManager.WatchResources(builder).
		Watches(newKubeObject[V, PV](), handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
			return []reconcile.Request{{
				NamespacedName: r.config.poolOwnerFromLabels(o.GetLabels()),
			}}
		})).
		Complete(r)
}

func (r *ClusterPoolReconciler[T, U, V, PT, PU, PV]) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	pool := newKubeObject[U, PU]()
	if err := r.config.Client.Get(ctx, req.NamespacedName, pool); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	labels := r.config.poolRelatedLabels(pool)
	nodeObjects, err := r.nodeManager.ListMatchingResources(ctx, newKubeObject[V, PV](), labels)
	if err != nil {
		logger.Error(err, "listing nodes")
		return ctrl.Result{}, err
	}
	nodes := mapObjectsTo[PV](nodeObjects)

	selector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{MatchLabels: labels})
	if err != nil {
		logger.Error(err, "constructing label selector")
		return ctrl.Result{}, err
	}

	hash, err := r.config.Factory.HashClusterPool(pool)
	if err != nil {
		logger.Error(err, "hashing pool")
		return ctrl.Result{}, err
	}

	originalStatus := r.config.Factory.GetClusterPoolStatus(pool)
	status := originalStatus.DeepCopy()
	status.Replicas = 0
	status.RunningReplicas = 0
	status.HealthyReplicas = 0
	status.UpToDateReplicas = 0
	status.OutOfDateReplicas = 0
	status.Selector = selector.String()

	syncStatus := func(err error) (ctrl.Result, error) {
		if !apiequality.Semantic.DeepEqual(status, &originalStatus) {
			r.config.Factory.SetClusterPoolStatus(pool, *status)
			syncErr := r.config.Client.Status().Update(ctx, pool)
			err = errors.Join(syncErr, err)
		}

		return ignoreConflict(err)
	}

	// we are being deleted, clean up everything
	if pool.GetDeletionTimestamp() != nil {
		// clean up all my resources
		if deleted, err := r.resourceManager.DeleteAll(ctx, pool); deleted || err != nil {
			return syncStatus(err)
		}

		if controllerutil.RemoveFinalizer(pool, r.config.Finalizer) {
			if err := r.config.Client.Update(ctx, pool); err != nil {
				logger.Error(err, "updating pool finalizer")
				return ignoreConflict(err)
			}
		}

		return ctrl.Result{}, nil
	}

	// add a finalizer
	if controllerutil.AddFinalizer(pool, r.config.Finalizer) {
		if err := r.config.Client.Update(ctx, pool); err != nil {
			logger.Error(err, "updating pool finalizer")
			return ignoreConflict(err)
		}
		return ctrl.Result{}, nil
	}

	// sync all my resources
	if err := r.resourceManager.SyncAll(ctx, pool); err != nil {
		return syncStatus(err)
	}

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
		}
		status.Replicas++
	}

	return syncStatus(nil)
}
