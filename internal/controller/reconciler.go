package controller

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"

	clusterv1alpha1 "github.com/andrewstucki/cluster-controller/api/v1alpha1"
	"github.com/andrewstucki/cluster-controller/internal/kubernetes"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8sapierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	errorutils "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	ownerIndex = ".metadata.controller"
	nodeIndex  = ".spec.nodeName"
)

var (
	nodesPredicate = predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldNode, oldOk := e.ObjectOld.(*corev1.Node)
			newNode, newOk := e.ObjectNew.(*corev1.Node)
			return oldOk && newOk && oldNode.Spec.Unschedulable != newNode.Spec.Unschedulable
		},
		CreateFunc: func(_ event.CreateEvent) bool {
			return false
		},
		DeleteFunc: func(_ event.DeleteEvent) bool {
			return false
		},
		GenericFunc: func(_ event.GenericEvent) bool {
			return false
		},
	}
)

type ClusterObject interface {
	client.Object
	GetClusterStatus() clusterv1alpha1.ClusterStatus
	SetClusterStatus(status clusterv1alpha1.ClusterStatus)
	GetReplicatedPodSpec() clusterv1alpha1.ReplicatedPodSpec
	GetReplicatedVolumeClaims() []corev1.PersistentVolumeClaim
}

type ptrToObject[T any] interface {
	client.Object
	*T
}

type ptrToCluster[T any] interface {
	ClusterObject
	*T
}

type Reconciler[T any, PT ptrToCluster[T]] struct {
	client.Client
	Scheme *runtime.Scheme

	keepNRevisions int
	indexPrefix    string
}

func SetupReconciler[T any, PT ptrToCluster[T]](ctx context.Context, mgr ctrl.Manager) error {
	return (&Reconciler[T, PT]{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(ctx, mgr)
}

func newKubeObject[T any, PT ptrToObject[T]]() PT {
	var t T
	return PT(&t)
}

func (r *Reconciler[T, PT]) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	if err := r.createFieldIndexes(ctx, mgr); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(newKubeObject[T, PT]()).
		Owns(&corev1.Pod{}).
		Owns(&appsv1.ControllerRevision{}).
		Watches(
			&corev1.Node{},
			handler.EnqueueRequestsFromMapFunc(r.mapNodeToClusters()),
			builder.WithPredicates(nodesPredicate),
		).
		Complete(r)
}

func (r *Reconciler[T, PT]) podOwnerIndex() string {
	return r.indexPrefix + ".pod" + ownerIndex
}

func (r *Reconciler[T, PT]) controllerRevisionOwnerIndex() string {
	return r.indexPrefix + ".controller-revision" + ownerIndex
}

func (r *Reconciler[T, PT]) podNodeIndex() string {
	return r.indexPrefix + ".pod" + nodeIndex
}

func indexOwner[T any, PT ptrToObject[T]](ctx context.Context, mgr ctrl.Manager, index string) error {
	return mgr.GetFieldIndexer().IndexField(ctx, newKubeObject[T, PT](), index, func(o client.Object) []string {
		if ownerName, ok := isOwnedByCluster(o); ok {
			return []string{ownerName}
		}

		return nil
	})
}

func indexPodNode(ctx context.Context, mgr ctrl.Manager, index string) error {
	return mgr.GetFieldIndexer().IndexField(ctx, &corev1.Pod{}, index, func(o client.Object) []string {
		pod := o.(*corev1.Pod)
		if pod.Spec.NodeName == "" {
			return nil
		}

		return []string{pod.Spec.NodeName}
	})
}

func (r *Reconciler[T, PT]) createFieldIndexes(ctx context.Context, mgr ctrl.Manager) error {
	r.indexPrefix = rand.String(10)

	// Create a new indexed field on Pods. This field will be used to easily
	// find all the Pods created by this controller
	if err := indexOwner[corev1.Pod](ctx, mgr, r.podOwnerIndex()); err != nil {
		return err
	}

	// Create a new indexed field on ControllerRevisions. This field will be used to easily
	// find all the ControllerRevisions created by this controller
	if err := indexOwner[appsv1.ControllerRevision](ctx, mgr, r.controllerRevisionOwnerIndex()); err != nil {
		return err
	}

	// Create a new indexed field on Pods. This field will be used to easily
	// find all the Pods created by node
	if err := indexPodNode(ctx, mgr, r.podNodeIndex()); err != nil {
		return err
	}

	return nil
}

func (r *Reconciler[T, PT]) mapNodeToClusters() handler.MapFunc {
	return func(ctx context.Context, obj client.Object) []reconcile.Request {
		node := obj.(*corev1.Node)

		// exit if the node is schedulable (e.g. not cordoned)
		// could be expanded here with other conditions (e.g. pressure or issues)
		if !node.Spec.Unschedulable {
			return nil
		}

		var childPods corev1.PodList

		// get all the pods handled by the operator on that node
		err := r.List(ctx, &childPods, client.MatchingFields{r.podNodeIndex(): node.Name}, client.MatchingLabels(r.defaultLabels()))
		if err != nil {
			log.FromContext(ctx).Error(err, "while getting instances for node")
			return nil
		}
		var requests []reconcile.Request

		for idx := range childPods.Items {
			if cluster, ok := isOwnedByCluster(&childPods.Items[idx]); ok {
				requests = append(requests,
					reconcile.Request{
						NamespacedName: types.NamespacedName{
							Name:      cluster,
							Namespace: childPods.Items[idx].Namespace,
						},
					},
				)
			}
		}
		return requests
	}
}

func isOwnedByCluster(obj client.Object) (string, bool) {
	owner := metav1.GetControllerOf(obj)
	if owner == nil {
		return "", false
	}

	if owner.Kind != clusterv1alpha1.ClusterKind {
		return "", false
	}

	if owner.APIVersion != clusterv1alpha1.GroupVersion.String() {
		return "", false
	}

	return owner.Name, true
}

func (r *Reconciler[T, PT]) defaultLabels() map[string]string {
	return map[string]string{
		"managed-by": "cluster",
	}
}

// General implementation:
// 1. Create a controller resource version snapshot for the current CRD
// 2. Pull the last
// 3. If an update is needed:
//    a. scale up to final desired state with old snapshot
//    b. restart any failed pods
//    c. wait until everything stabilizes
//    d. scale down waiting for stabilization in between
//    e. rolling restart all pods

func initIdentity[T ClusterObject](cluster T, pod *corev1.Pod) {
	updateIdentity(cluster, pod)
	pod.Spec.Hostname = pod.Name
}

func newPod[T ClusterObject](parent T, spec *clusterv1alpha1.ReplicatedPodSpec, ordinal int) *corev1.Pod {
	pod, _ := kubernetes.GetPodFromTemplate(&spec.PodSpec, parent, metav1.NewControllerRef(parent, parent.GetObjectKind().GroupVersionKind()))
	pod.Name = getPodName(parent, ordinal)
	initIdentity(parent, pod)
	updateStorage(parent, pod)
	return pod
}

func setPodRevision(pod *corev1.Pod, revision string) {
	if pod.Labels == nil {
		pod.Labels = make(map[string]string)
	}
	pod.Labels[revisionLabel] = revision
}

func newVersionedPod[T ClusterObject](cluster T, currentSpec, updateSpec *clusterv1alpha1.ReplicatedPodSpec, currentRevision, updateRevision string, replicas, ordinal int) *corev1.Pod {
	if ordinal < int(replicas) {
		pod := newPod(cluster, currentSpec, ordinal)
		setPodRevision(pod, currentRevision)
		return pod
	}
	pod := newPod(cluster, updateSpec, ordinal)
	setPodRevision(pod, updateRevision)
	return pod
}

func (r *Reconciler[T, PT]) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	cluster := newKubeObject[T, PT]()
	if err := r.Get(ctx, req.NamespacedName, cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	pods, err := r.getClusterPods(ctx, cluster.GetName())
	if err != nil {
		logger.Error(err, "fetching cluster pods")
		return ctrl.Result{}, err
	}

	revisions, err := r.getControllerRevisions(ctx, cluster.GetName())
	if err != nil {
		logger.Error(err, "fetching controller revisions")
		return ctrl.Result{}, err
	}

	currentRevision, updateRevision, collisionCount, err := r.getRevisions(ctx, cluster, revisions)
	if err != nil {
		logger.Error(err, "getting latest revisions")
		return ctrl.Result{}, err
	}

	currentSpec, err := revisionToSpec(currentRevision)
	if err != nil {
		return ctrl.Result{}, err
	}
	updateSpec, err := revisionToSpec(updateRevision)
	if err != nil {
		return ctrl.Result{}, err
	}

	status := clusterv1alpha1.ClusterStatus{}
	status.ObservedGeneration = cluster.GetGeneration()
	status.CurrentRevision = currentRevision.Name
	status.UpdateRevision = updateRevision.Name
	status.CollisionCount = new(int32)
	*status.CollisionCount = collisionCount

	replicaCount := updateSpec.Replicas
	replicas := make([]*corev1.Pod, replicaCount)
	condemned := make([]*corev1.Pod, 0, len(pods))
	unhealthy := 0
	firstUnhealthyOrdinal := math.MaxInt32
	var firstUnhealthyPod *corev1.Pod

	for i := range pods {
		pod := &pods[i]

		status.Replicas++

		// count the number of running and ready replicas
		if isRunningAndReady(pod) {
			status.ReadyReplicas++
		}

		// count the number of current and update replicas
		if isCreated(pod) && !isTerminating(pod) {
			if getPodRevision(pod) == currentRevision.Name {
				status.CurrentReplicas++
			}
			if getPodRevision(pod) == updateRevision.Name {
				status.UpdatedReplicas++
			}
		}

		if ord := getOrdinal(pod); 0 <= ord && ord < replicaCount {
			// if the ordinal of the pod is within the range of the current number of replicas,
			// insert it at the indirection of its ordinal
			replicas[ord] = pod

		} else if ord >= replicaCount {
			// if the ordinal is greater than the number of replicas add it to the condemned list
			condemned = append(condemned, pod)
		}
		// If the ordinal could not be parsed (ord < 0), ignore the Pod.
	}

	for ord := 0; ord < replicaCount; ord++ {
		if replicas[ord] == nil {
			replicas[ord] = newVersionedPod(cluster,
				currentSpec,
				updateSpec,
				currentRevision.Name,
				updateRevision.Name, status.CurrentReplicas, ord)
		}
	}

	sort.SliceStable(condemned, func(i, j int) bool {
		return getOrdinal(condemned[i]) < getOrdinal(condemned[j])
	})

	for i := range replicas {
		if replicas[i] == nil {
			continue
		}
		if !isHealthy(replicas[i]) {
			unhealthy++
			if ord := getOrdinal(replicas[i]); ord < firstUnhealthyOrdinal {
				firstUnhealthyOrdinal = ord
				firstUnhealthyPod = replicas[i]
			}
		}
	}

	for i := range condemned {
		if !isHealthy(condemned[i]) {
			unhealthy++
			if ord := getOrdinal(condemned[i]); ord < firstUnhealthyOrdinal {
				firstUnhealthyOrdinal = ord
				firstUnhealthyPod = condemned[i]
			}
		}
	}

	if cluster.GetDeletionTimestamp() != nil {
		// we're deleting, just update status and return
		// TODO: status update
		return ctrl.Result{}, nil
	}

	for i := range replicas {
		if replicas[i] == nil {
			continue
		}
		// restart any replicas
		if isFailed(replicas[i]) || isSucceeded(replicas[i]) {
			if err := r.Client.Delete(ctx, replicas[i]); err != nil {
				if !k8sapierrors.IsNotFound(err) {
					return ctrl.Result{}, err
				}
			}
			if getPodRevision(replicas[i]) == currentRevision.Name {
				status.CurrentReplicas--
			}
			if getPodRevision(replicas[i]) == updateRevision.Name {
				status.UpdatedReplicas--
			}
			status.Replicas--
			replicas[i] = newVersionedPod(cluster,
				currentSpec,
				updateSpec,
				currentRevision.Name,
				updateRevision.Name,
				status.CurrentReplicas, i)
		}

		if !isCreated(replicas[i]) {
			if err := r.createPersistentVolumeClaims(ctx, cluster, replicas[i]); err != nil {
				return ctrl.Result{}, err
			}

			if err := r.Client.Create(ctx, replicas[i]); err != nil {
				if !k8sapierrors.IsAlreadyExists(err) {
					return ctrl.Result{}, err
				}
			}

			status.Replicas++
			if getPodRevision(replicas[i]) == currentRevision.Name {
				status.CurrentReplicas++
			}
			if getPodRevision(replicas[i]) == updateRevision.Name {
				status.UpdatedReplicas++
			}

			return ctrl.Result{}, nil
		}

		// If we find a Pod that is currently terminating, we must wait until graceful deletion
		// completes before we continue to make progress.
		if isTerminating(replicas[i]) {
			return ctrl.Result{}, nil
		}

		// If we have a Pod that has been created but is not running and ready we can not make progress.
		// We must ensure that all for each Pod, when we create it, all of its predecessors, with respect to its
		// ordinal, are Running and Ready.
		if !isRunningAndReady(replicas[i]) {
			return ctrl.Result{}, nil
		}

		// Enforce the StatefulSet invariants
		if identityMatches(cluster, replicas[i]) && storageMatches(cluster, replicas[i]) {
			continue
		}

		// Make a deep copy so we don't mutate the shared cache
		if err := r.updateStatefulPod(ctx, cluster, replicas[i].DeepCopy()); err != nil {
			return ctrl.Result{}, err
		}
	}

	// At this point, all of the current Replicas are Running and Ready, we can consider termination.
	// We will wait for all predecessors to be Running and Ready prior to attempting a deletion.
	// We will terminate Pods in a monotonically decreasing order over [len(pods),set.Spec.Replicas).
	// Note that we do not resurrect Pods in this interval. Also note that scaling will take precedence over
	// updates.
	target := len(condemned) - 1
	if target >= 0 {
		// wait for terminating pods to expire
		if isTerminating(condemned[target]) {
			return ctrl.Result{}, nil
		}
		if !isRunningAndReady(condemned[target]) && condemned[target] != firstUnhealthyPod {
			return ctrl.Result{}, nil
		}

		if err := r.Client.Delete(ctx, condemned[target]); err != nil {
			return ctrl.Result{}, err
		}
		if getPodRevision(condemned[target]) == currentRevision.Name {
			status.CurrentReplicas--
		}
		if getPodRevision(condemned[target]) == updateRevision.Name {
			status.UpdatedReplicas--
		}
		return ctrl.Result{}, nil
	}

	for target := len(replicas) - 1; target >= 0; target-- {
		if replicas[target] == nil {
			continue
		}

		// delete the Pod if it is not already terminating and does not match the update revision.
		if getPodRevision(replicas[target]) != updateRevision.Name && !isTerminating(replicas[target]) {
			if err := r.Client.Delete(ctx, replicas[target]); err != nil {
				return ctrl.Result{}, err
			}
			status.CurrentReplicas--
			return ctrl.Result{}, nil
		}

		// wait for unhealthy Pods on update
		if !isHealthy(replicas[target]) {
			return ctrl.Result{}, nil
		}

	}

	if err := r.truncateHistory(ctx, pods, revisions, currentRevision, updateRevision); err != nil {
		return ctrl.Result{}, err
	}

	cluster.SetClusterStatus(status)

	return ctrl.Result{}, nil
}

func (r *Reconciler[T, PT]) getClusterPods(ctx context.Context, clusterName string) ([]corev1.Pod, error) {
	var pods corev1.PodList
	if err := r.Client.List(ctx, &pods, &client.ListOptions{
		FieldSelector: fields.OneTermEqualSelector(r.podOwnerIndex(), clusterName),
	}); err != nil {
		return nil, err
	}

	return pods.Items, nil
}

func (r *Reconciler[T, PT]) getControllerRevisions(ctx context.Context, clusterName string) ([]*appsv1.ControllerRevision, error) {
	var revisions appsv1.ControllerRevisionList
	if err := r.Client.List(ctx, &revisions, &client.ListOptions{
		FieldSelector: fields.OneTermEqualSelector(r.controllerRevisionOwnerIndex(), clusterName),
	}); err != nil {
		return nil, err
	}

	res := []*appsv1.ControllerRevision{}
	for _, item := range revisions.Items {
		res = append(res, ptr.To(item))
	}

	sortControllerRevisions(res)

	return res, nil
}

func (r *Reconciler[T, PT]) getRevisions(ctx context.Context, cluster ClusterObject, revisions []*appsv1.ControllerRevision) (*appsv1.ControllerRevision, *appsv1.ControllerRevision, int32, error) {
	var currentRevision, updateRevision *appsv1.ControllerRevision

	revisionCount := len(revisions)

	var collisionCount int32
	status := cluster.GetClusterStatus()
	if status.CollisionCount != nil {
		collisionCount = *status.CollisionCount
	}

	updateRevision, err := newRevision(cluster, ptr.To(cluster.GetReplicatedPodSpec()), nextRevision(revisions), &collisionCount)
	if err != nil {
		return nil, nil, collisionCount, err
	}

	equalRevisions := findEqualRevisions(revisions, updateRevision)
	equalCount := len(equalRevisions)

	if equalCount > 0 && equalRevision(revisions[revisionCount-1], equalRevisions[equalCount-1]) {
		// if the equivalent revision is immediately prior the update revision has not changed
		updateRevision = revisions[revisionCount-1]
	} else if equalCount > 0 {
		// if the equivalent revision is not immediately prior we will roll back by incrementing the
		// Revision of the equivalent revision
		updateRevision, err = r.updateControllerRevision(ctx, equalRevisions[equalCount-1], updateRevision.Revision)
		if err != nil {
			return nil, nil, collisionCount, err
		}
	} else {
		//if there is no equivalent revision we create a new one
		updateRevision, err = r.createControllerRevision(ctx, cluster, updateRevision, &collisionCount)
		if err != nil {
			return nil, nil, collisionCount, err
		}
	}

	// attempt to find the revision that corresponds to the current revision
	for i := range revisions {
		if revisions[i].Name == status.CurrentRevision {
			currentRevision = revisions[i]
			break
		}
	}

	// if the current revision is nil we initialize the history by setting it to the update revision
	if currentRevision == nil {
		currentRevision = updateRevision
	}

	return currentRevision, updateRevision, collisionCount, nil
}

func (r *Reconciler[T, PT]) createPersistentVolumeClaims(ctx context.Context, cluster PT, pod *corev1.Pod) error {
	var errs []error
	for _, claim := range getPersistentVolumeClaims(cluster, pod) {
		err := r.Client.Get(ctx, client.ObjectKeyFromObject(&claim), &claim)
		switch {
		case k8sapierrors.IsNotFound(err):
			err := r.Client.Create(ctx, &claim)
			if err != nil {
				errs = append(errs, fmt.Errorf("failed to create PVC %s: %s", claim.Name, err))
			}
		case err != nil:
			errs = append(errs, fmt.Errorf("failed to retrieve PVC %s: %s", claim.Name, err))
		}
		// TODO: Check resource requirements and accessmodes, update if necessary
	}
	return errorutils.NewAggregate(errs)
}

func (r *Reconciler[T, PT]) updateStatefulPod(ctx context.Context, cluster PT, pod *corev1.Pod) error {
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		// assume the Pod is consistent
		consistent := true
		// if the Pod does not conform to its identity, update the identity and dirty the Pod
		if !identityMatches(cluster, pod) {
			updateIdentity(cluster, pod)
			consistent = false
		}

		// if the Pod does not conform to the StatefulSet's storage requirements, update the Pod's PVC's,
		// dirty the Pod, and create any missing PVCs
		if !storageMatches(cluster, pod) {
			updateStorage(cluster, pod)
			consistent = false
			if err := r.createPersistentVolumeClaims(ctx, cluster, pod); err != nil {
				return err
			}
		}

		// if the Pod is not dirty, do nothing
		if consistent {
			return nil
		}

		// commit the update, retrying on conflicts
		updateErr := r.Client.Update(ctx, pod)
		if updateErr == nil {
			return nil
		}

		var updated corev1.Pod
		if err := r.Client.Get(ctx, client.ObjectKeyFromObject(pod), &updated); err == nil {
			// make a copy so we don't mutate the shared cache
			pod = updated.DeepCopy()
		}

		return updateErr
	})
	return err
}

func (r *Reconciler[T, PT]) updateControllerRevision(ctx context.Context, revision *appsv1.ControllerRevision, newRevision int64) (*appsv1.ControllerRevision, error) {
	clone := revision.DeepCopy()
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		if clone.Revision == newRevision {
			return nil
		}
		clone.Revision = newRevision
		updateErr := r.Client.Update(ctx, clone)
		if updateErr == nil {
			return nil
		}

		var updated appsv1.ControllerRevision
		if err := r.Client.Get(ctx, client.ObjectKeyFromObject(clone), &updated); err == nil {
			// make a copy so we don't mutate the shared cache
			clone = updated.DeepCopy()
		}

		return updateErr
	})
	return clone, err
}

func (r *Reconciler[T, PT]) createControllerRevision(ctx context.Context, parent metav1.Object, revision *appsv1.ControllerRevision, collisionCount *int32) (*appsv1.ControllerRevision, error) {
	if collisionCount == nil {
		return nil, fmt.Errorf("collisionCount should not be nil")
	}

	// Clone the input
	clone := revision.DeepCopy()

	// Continue to attempt to create the revision updating the name with a new hash on each iteration
	for {
		hash := hashControllerRevision(revision, collisionCount)
		// Update the revisions name
		clone.Name = controllerRevisionName(parent.GetName(), hash)
		err := r.Client.Create(ctx, clone)
		if k8sapierrors.IsAlreadyExists(err) {
			var exists appsv1.ControllerRevision
			err := r.Client.Get(ctx, client.ObjectKeyFromObject(clone), &exists)
			if err != nil {
				return nil, err
			}
			if bytes.Equal(exists.Data.Raw, clone.Data.Raw) {
				return &exists, nil
			}
			*collisionCount++
			continue
		}
		return clone, err
	}
}

func (r *Reconciler[T, PT]) truncateHistory(
	ctx context.Context,
	pods []corev1.Pod,
	revisions []*appsv1.ControllerRevision,
	current *appsv1.ControllerRevision,
	update *appsv1.ControllerRevision) error {
	history := make([]*appsv1.ControllerRevision, 0, len(revisions))
	// mark all live revisions
	live := map[string]bool{current.Name: true, update.Name: true}
	for i := range pods {
		live[getPodRevision(&pods[i])] = true
	}
	// collect live revisions and historic revisions
	for i := range revisions {
		if !live[revisions[i].Name] {
			history = append(history, revisions[i])
		}
	}
	historyLen := len(history)
	historyLimit := r.keepNRevisions
	if historyLen <= historyLimit {
		return nil
	}
	// delete any non-live history to maintain the revision limit.
	history = history[:(historyLen - historyLimit)]
	for i := 0; i < len(history); i++ {
		if err := r.Client.Delete(ctx, history[i]); err != nil {
			return err
		}
	}
	return nil
}

func isRunningAndReady(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodRunning && kubernetes.IsPodReady(pod)
}

func isCreated(pod *corev1.Pod) bool {
	return pod.Status.Phase != ""
}

func isHealthy(pod *corev1.Pod) bool {
	return isRunningAndReady(pod) && !isTerminating(pod)
}

func isFailed(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodFailed
}

func isSucceeded(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodSucceeded
}

func isTerminating(pod *corev1.Pod) bool {
	return pod.DeletionTimestamp != nil
}

var statefulPodRegex = regexp.MustCompile("(.*)-([0-9]+)$")

func getParentNameAndOrdinal(pod *corev1.Pod) (string, int) {
	parent := ""
	ordinal := -1
	subMatches := statefulPodRegex.FindStringSubmatch(pod.Name)
	if len(subMatches) < 3 {
		return parent, ordinal
	}
	parent = subMatches[1]
	if i, err := strconv.ParseInt(subMatches[2], 10, 32); err == nil {
		ordinal = int(i)
	}
	return parent, ordinal
}

func getOrdinal(pod *corev1.Pod) int {
	_, ordinal := getParentNameAndOrdinal(pod)
	return ordinal
}

func getPodName[T ClusterObject](set T, ordinal int) string {
	return fmt.Sprintf("%s-%d", set.GetName(), ordinal)
}

func identityMatches[T ClusterObject](set T, pod *corev1.Pod) bool {
	parent, ordinal := getParentNameAndOrdinal(pod)
	return ordinal >= 0 &&
		set.GetName() == parent &&
		pod.Name == getPodName(set, ordinal) &&
		pod.Namespace == set.GetNamespace() &&
		pod.Labels[revisionLabel] == pod.Name
}

func updateIdentity[T ClusterObject](set T, pod *corev1.Pod) {
	pod.Name = getPodName(set, getOrdinal(pod))
	pod.Namespace = set.GetNamespace()
	if pod.Labels == nil {
		pod.Labels = make(map[string]string)
	}
	pod.Labels[revisionLabel] = pod.Name
}

func getPersistentVolumeClaimName[T ClusterObject](set T, claim *corev1.PersistentVolumeClaim, ordinal int) string {
	// NOTE: This name format is used by the heuristics for zone spreading in ChooseZoneForVolume
	return fmt.Sprintf("%s-%s-%d", claim.Name, set.GetName(), ordinal)
}

func getPersistentVolumeClaims[T ClusterObject](set T, pod *corev1.Pod) map[string]corev1.PersistentVolumeClaim {
	ordinal := getOrdinal(pod)
	templates := set.GetReplicatedVolumeClaims()
	claims := make(map[string]corev1.PersistentVolumeClaim, len(templates))
	for i := range templates {
		claim := templates[i]
		claim.Name = getPersistentVolumeClaimName(set, &claim, ordinal)
		claim.Namespace = set.GetNamespace()
		claims[templates[i].Name] = claim
	}
	return claims
}

func storageMatches[T ClusterObject](set T, pod *corev1.Pod) bool {
	ordinal := getOrdinal(pod)
	if ordinal < 0 {
		return false
	}
	volumes := make(map[string]corev1.Volume, len(pod.Spec.Volumes))
	for _, volume := range pod.Spec.Volumes {
		volumes[volume.Name] = volume
	}
	for _, claim := range set.GetReplicatedVolumeClaims() {
		volume, found := volumes[claim.Name]
		if !found ||
			volume.VolumeSource.PersistentVolumeClaim == nil ||
			volume.VolumeSource.PersistentVolumeClaim.ClaimName !=
				getPersistentVolumeClaimName(set, &claim, ordinal) {
			return false
		}
	}
	return true
}

func updateStorage[T ClusterObject](set T, pod *corev1.Pod) {
	currentVolumes := pod.Spec.Volumes
	claims := getPersistentVolumeClaims(set, pod)
	newVolumes := make([]corev1.Volume, 0, len(claims))
	for name, claim := range claims {
		newVolumes = append(newVolumes, corev1.Volume{
			Name: name,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: claim.Name,
					// TODO: Use source definition to set this value when we have one.
					ReadOnly: false,
				},
			},
		})
	}
	for i := range currentVolumes {
		if _, ok := claims[currentVolumes[i].Name]; !ok {
			newVolumes = append(newVolumes, currentVolumes[i])
		}
	}
	pod.Spec.Volumes = newVolumes
}

func getPodRevision(pod *corev1.Pod) string {
	if pod.Labels == nil {
		return ""
	}
	return pod.Labels[revisionLabel]
}
