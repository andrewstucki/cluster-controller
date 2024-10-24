package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	clusterv1alpha1 "github.com/andrewstucki/cluster-controller/api/v1alpha1"
	"github.com/go-logr/logr"
	k8sapierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type ModifiedClusterObjects[U any, PU ptrToClusterNode[U]] struct {
	Node PU
}

func modifiedClusterObjects[U any, PU ptrToClusterNode[U]](node PU) *ModifiedClusterObjects[U, PU] {
	return &ModifiedClusterObjects[U, PU]{
		Node: node,
	}
}

type ClusterHooks[T, U any, PU ptrToClusterNode[U], PT ptrToCluster[T, U, PU]] struct {
	Create LifecycleHook[PT, *ModifiedClusterObjects[U, PU]]
	Update LifecycleHook[PT, *ModifiedClusterObjects[U, PU]]
	Delete LifecycleHook[PT, *ModifiedClusterObjects[U, PU]]
}

type debugClusterHook[T, U any, PU ptrToClusterNode[U], PT ptrToCluster[T, U, PU]] struct {
	operation string
	logger    logr.Logger
}

func (h *debugClusterHook[T, U, PU, PT]) Before(ctx context.Context, cluster PT, modified *ModifiedClusterObjects[U, PU]) error {
	h.logger.Info("before "+h.operation, "modified", modified)
	return nil
}

func (h *debugClusterHook[T, U, PU, PT]) After(ctx context.Context, cluster PT, modified *ModifiedClusterObjects[U, PU]) error {
	h.logger.Info("after "+h.operation, "modified", modified)
	return nil
}

func NewDebugClusterHooks[T, U any, PU ptrToClusterNode[U], PT ptrToCluster[T, U, PU]](logger logr.Logger) *ClusterHooks[T, U, PU, PT] {
	return &ClusterHooks[T, U, PU, PT]{
		Create: &debugClusterHook[T, U, PU, PT]{operation: "create", logger: logger},
		Update: &debugClusterHook[T, U, PU, PT]{operation: "update", logger: logger},
		Delete: &debugClusterHook[T, U, PU, PT]{operation: "delete", logger: logger},
	}
}

func runClusterLifecycleBeforeHook[T, U any, PU ptrToClusterNode[U], PT ptrToCluster[T, U, PU]](ctx context.Context, cluster PT, modified *ModifiedClusterObjects[U, PU], hook LifecycleHook[PT, *ModifiedClusterObjects[U, PU]]) error {
	if hook == nil {
		return nil
	}
	return runClusterLifecycleHook(ctx, cluster, modified, hook.Before)
}

func runClusterLifecycleAfterHook[T, U any, PU ptrToClusterNode[U], PT ptrToCluster[T, U, PU]](ctx context.Context, cluster PT, modified *ModifiedClusterObjects[U, PU], hook LifecycleHook[PT, *ModifiedClusterObjects[U, PU]]) error {
	if hook == nil {
		return nil
	}
	return runClusterLifecycleHook(ctx, cluster, modified, hook.After)
}

func runClusterLifecycleHook[T, U any, PU ptrToClusterNode[U], PT ptrToCluster[T, U, PU]](ctx context.Context, cluster PT, modified *ModifiedClusterObjects[U, PU], hook func(ctx context.Context, cluster PT, modified *ModifiedClusterObjects[U, PU]) error) error {
	if hook == nil {
		return nil
	}
	return hook(ctx, cluster, modified)
}

type ClusterNodeMerger[U any, PU ptrToClusterNode[U]] interface {
	Merge(lhs, rhs PU)
}

type mergableNode[U any, PU ptrToClusterNode[U]] interface {
	Merge(other PU)
}

type ClusterReconciler[T, U any, PU ptrToClusterNode[U], PT ptrToCluster[T, U, PU]] struct {
	client.Client
	Scheme *runtime.Scheme

	indexPrefix string
	merger      ClusterNodeMerger[U, PU]
	hooks       *ClusterHooks[T, U, PU, PT]
}

func SetupClusterReconciler[T, U any, PU ptrToClusterNode[U], PT ptrToCluster[T, U, PU]](ctx context.Context, mgr ctrl.Manager, hooks *ClusterHooks[T, U, PU, PT]) error {
	if hooks == nil {
		hooks = &ClusterHooks[T, U, PU, PT]{}
	}
	return (&ClusterReconciler[T, U, PU, PT]{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		hooks:  hooks,
	}).setupWithManager(ctx, mgr)
}

func (r *ClusterReconciler[T, U, PU, PT]) setupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	r.indexPrefix = rand.String(10)

	node := newKubeObject[U, PU]()

	if err := indexOwnerObject(ctx, mgr, node, r.clusterNodeOwnerIndex()); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(newKubeObject[T, PT]()).
		Owns(node).
		Complete(r)
}

func (r *ClusterReconciler[T, U, PU, PT]) clusterNodeOwnerIndex() string {
	return r.indexPrefix + ".clusterNode" + ownerIndex
}

func (r *ClusterReconciler[T, U, PU, PT]) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	cluster := newKubeObject[T, PT]()
	if err := r.Get(ctx, req.NamespacedName, cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	nodes, err := r.getClusterNodes(ctx, cluster.GetName())
	if err != nil {
		logger.Error(err, "fetching cluster nodes")
		return ctrl.Result{}, err
	}

	hash, err := cluster.GetNodeHash()
	if err != nil {
		logger.Error(err, "calculating cluster node hash")
		return ctrl.Result{}, err
	}

	originalStatus := cluster.GetClusterStatus()
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
			cluster.SetClusterStatus(*status)
			syncErr := r.Client.Status().Update(ctx, cluster)
			return ctrl.Result{}, errors.Join(syncErr, err)
		}

		return ctrl.Result{}, err
	}

	// General implementation:
	// 1. grab all nodes for the cluster
	// 2. see if we need to scale down
	// 3. scale down starting with either the newest node or an unhealthy node
	// 4. rolling restart all nodes if we meet the rollout threshold
	// 5. scale up to desired size

	unhealthyNodes := []PU{}
	for _, node := range nodes {
		if isNodeStale(node, hash) {
			status.OutOfDateReplicas++
		}
		if isNodeUpdated(node, hash) {
			status.UpToDateReplicas++
		}
		if isNodeRunning(node) {
			status.RunningReplicas++
		}
		if isNodeHealthy(node) {
			status.HealthyReplicas++
			unhealthyNodes = append(unhealthyNodes, node)
		}
		status.Replicas++
	}

	desiredReplicas := cluster.GetReplicas()
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
		if isNodeUpdating(node, hash) {
			// we have a pending update, just wait
			return syncStatus(nil)
		}
	}

	// we don't so roll out any needed change

	for _, node := range nodes {
		if getHash(node) != hash {
			// if we don't meet the minimum healthy threshold, then don't do anything
			minimumReplicas := cluster.GetMinimumHealthyReplicas()
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

func (r *ClusterReconciler[T, U, PU, PT]) decommissionNode(ctx context.Context, status *clusterv1alpha1.ClusterStatus, cluster PT, node PU) error {
	setPhase(cluster, status, decommissioningPhase(fmt.Sprintf("decommissioning node: \"%s/%s\"", node.GetNamespace(), node.GetName())))

	modifying := modifiedClusterObjects(node)
	if err := runClusterLifecycleBeforeHook(ctx, cluster, modifying, r.hooks.Delete); err != nil {
		return fmt.Errorf("running node before delete hook: %w", err)
	}

	if err := r.Client.Delete(ctx, node); err != nil {
		if !k8sapierrors.IsNotFound(err) {
			return fmt.Errorf("deleting node: %w", err)
		}
	}

	if err := runClusterLifecycleAfterHook(ctx, cluster, modifying, r.hooks.Delete); err != nil {
		return fmt.Errorf("running node after delete hook: %w", err)
	}

	return nil
}

func (r *ClusterReconciler[T, U, PU, PT]) createNode(ctx context.Context, status *clusterv1alpha1.ClusterStatus, cluster PT, hash string) error {
	node := cluster.GetClusterNode()
	initNode(cluster, node, hash)

	setPhase(cluster, status, initializingPhase(fmt.Sprintf("creating node \"%s/%s\"", node.GetNamespace(), node.GetName())))

	modifying := modifiedClusterObjects(node)
	if err := runClusterLifecycleBeforeHook(ctx, cluster, modifying, r.hooks.Delete); err != nil {
		return fmt.Errorf("running node before create hook: %w", err)
	}

	if err := r.Client.Patch(ctx, node, client.Apply, fieldOwner, client.ForceOwnership); err != nil {
		return fmt.Errorf("creating node: %w", err)
	}

	if err := runClusterLifecycleAfterHook(ctx, cluster, modifying, r.hooks.Create); err != nil {
		return fmt.Errorf("running node after create hook: %w", err)
	}

	return nil
}

func (r *ClusterReconciler[T, U, PU, PT]) updateNode(ctx context.Context, status *clusterv1alpha1.ClusterStatus, cluster PT, node PU, hash string) error {
	updated := cluster.GetClusterNode()
	initNode(cluster, updated, hash)
	updated.SetName(node.GetName())

	if merger, ok := reflect.ValueOf(node).Interface().(mergableNode[U, PU]); ok {
		merger.Merge(updated)
	} else if r.merger != nil {
		r.merger.Merge(node, updated)
	}

	labels := node.GetLabels()
	labels[hashLabel] = hash
	node.SetLabels(labels)

	setPhase(cluster, status, updatingPhase(fmt.Sprintf("updating node \"%s/%s\"", node.GetNamespace(), node.GetName())))
	modifying := modifiedClusterObjects(node)

	if err := runClusterLifecycleBeforeHook(ctx, cluster, modifying, r.hooks.Update); err != nil {
		return fmt.Errorf("running node before update hook: %w", err)
	}

	if err := r.Client.Patch(ctx, node, client.Apply, fieldOwner, client.ForceOwnership); err != nil {
		return fmt.Errorf("updating node: %w", err)
	}

	if err := runClusterLifecycleAfterHook(ctx, cluster, modifying, r.hooks.Update); err != nil {
		return fmt.Errorf("running node after update hook: %w", err)
	}

	return nil
}

func initNode[T, U any, PU ptrToClusterNode[U], PT ptrToCluster[T, U, PU]](cluster PT, node PU, hash string) {
	node.SetName(cluster.GetName() + fmt.Sprintf("-%s", rand.String(8)))
	node.SetNamespace(cluster.GetNamespace())

	node.SetOwnerReferences([]metav1.OwnerReference{*metav1.NewControllerRef(cluster, cluster.GetObjectKind().GroupVersionKind())})

	labels := node.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}

	labels[hashLabel] = hash
	labels[clusterLabel] = cluster.GetName()
	node.SetLabels(labels)
}

func (r *ClusterReconciler[T, U, PU, PT]) getClusterNodes(ctx context.Context, clusterName string) ([]PU, error) {
	kinds, _, err := r.Scheme.ObjectKinds(newKubeObject[U, PU]())
	if err != nil {
		return nil, fmt.Errorf("fetching object kind: %w", err)
	}
	if len(kinds) == 0 {
		return nil, fmt.Errorf("unable to determine object kind")
	}
	kind := kinds[0]
	kind.Kind += "List"

	o, err := r.Scheme.New(kind)
	if err != nil {
		return nil, fmt.Errorf("initializing list: %w", err)
	}
	list, ok := o.(client.ObjectList)
	if !ok {
		return nil, fmt.Errorf("invalid object list type: %T", o)
	}

	if err := r.Client.List(ctx, list, &client.ListOptions{
		FieldSelector: fields.OneTermEqualSelector(r.clusterNodeOwnerIndex(), clusterName),
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

func isNodeUpdating[T any, PT ptrToClusterNode[T]](node PT, hash string) bool {
	return getHash(node) == hash && (node.GetClusterNodeStatus().ClusterVersion != hash || !node.GetClusterNodeStatus().MatchesCluster)
}

func isNodeUpdated[T any, PT ptrToClusterNode[T]](node PT, hash string) bool {
	return getHash(node) == hash && node.GetClusterNodeStatus().ClusterVersion == hash && node.GetClusterNodeStatus().MatchesCluster
}

func isNodeStale[T any, PT ptrToClusterNode[T]](node PT, hash string) bool {
	return getHash(node) != hash
}

func isNodeHealthy[T any, PT ptrToClusterNode[T]](node PT) bool {
	return node.GetClusterNodeStatus().Healthy
}

func isNodeRunning[T any, PT ptrToClusterNode[T]](node PT) bool {
	return node.GetClusterNodeStatus().Running
}
