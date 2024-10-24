package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"

	clusterv1alpha1 "github.com/andrewstucki/cluster-controller/api/v1alpha1"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	k8sapierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type ptrToClusterNode[T any] interface {
	ClusterNodeObject
	*T
}

type ClusterNodeObject interface {
	client.Object
	GetPodSpec() *corev1.PodTemplateSpec
	GetVolumes() []*corev1.PersistentVolume
	GetVolumeClaims() []*corev1.PersistentVolumeClaim
	GetClusterNodeStatus() clusterv1alpha1.ClusterNodeStatus
	SetClusterNodeStatus(status clusterv1alpha1.ClusterNodeStatus)
}

type LifecycleHook[T, U any] interface {
	Before(ctx context.Context, t T, u U) error
	After(ctx context.Context, t T, u U) error
}

type ModifiedNodeObjects struct {
	Pod  *corev1.Pod
	PVs  []*corev1.PersistentVolume
	PVCs []*corev1.PersistentVolumeClaim
}

type debugNodeHook[U any, PU ptrToClusterNode[U]] struct {
	operation string
	logger    logr.Logger
}

func (h *debugNodeHook[U, PU]) Before(ctx context.Context, node PU, modified *ModifiedNodeObjects) error {
	h.logger.Info("before "+h.operation, "modified", modified)
	return nil
}

func (h *debugNodeHook[U, PU]) After(ctx context.Context, node PU, modified *ModifiedNodeObjects) error {
	h.logger.Info("after "+h.operation, "modified", modified)
	return nil
}

func NewDebugNodeHooks[U any, PU ptrToClusterNode[U]](logger logr.Logger) *ClusterNodeHooks[U, PU] {
	return &ClusterNodeHooks[U, PU]{
		Create: &debugNodeHook[U, PU]{operation: "create", logger: logger},
		Delete: &debugNodeHook[U, PU]{operation: "delete", logger: logger},
	}
}

type ClusterNodeHooks[U any, PU ptrToClusterNode[U]] struct {
	Create LifecycleHook[PU, *ModifiedNodeObjects]
	Delete LifecycleHook[PU, *ModifiedNodeObjects]
}

func runNodeLifecycleBeforeHook[U any, PU ptrToClusterNode[U]](ctx context.Context, node PU, modified *ModifiedNodeObjects, hook LifecycleHook[PU, *ModifiedNodeObjects]) error {
	if hook == nil {
		return nil
	}
	return runNodeLifecycleHook(ctx, node, modified, hook.Before)
}

func runNodeLifecycleAfterHook[U any, PU ptrToClusterNode[U]](ctx context.Context, node PU, modified *ModifiedNodeObjects, hook LifecycleHook[PU, *ModifiedNodeObjects]) error {
	if hook == nil {
		return nil
	}
	return runNodeLifecycleHook(ctx, node, modified, hook.After)
}

func runNodeLifecycleHook[U any, PU ptrToClusterNode[U]](ctx context.Context, node PU, modified *ModifiedNodeObjects, hook func(ctx context.Context, node PU, modified *ModifiedNodeObjects) error) error {
	if hook == nil {
		return nil
	}
	return hook(ctx, node, modified)
}

type NodeReconciler[T any, PT ptrToClusterNode[T]] struct {
	client.Client
	Scheme *runtime.Scheme

	hooks       *ClusterNodeHooks[T, PT]
	indexPrefix string
}

func SetupNodeReconciler[T any, PT ptrToClusterNode[T]](ctx context.Context, mgr ctrl.Manager, hooks *ClusterNodeHooks[T, PT]) error {
	if hooks == nil {
		hooks = &ClusterNodeHooks[T, PT]{}
	}
	return (&NodeReconciler[T, PT]{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		hooks:  hooks,
	}).setupWithManager(ctx, mgr)
}

func (r *NodeReconciler[T, PT]) podOwnerIndex() string {
	return r.indexPrefix + ".pod" + ownerIndex
}

func (r *NodeReconciler[T, PT]) pvcOwnerIndex() string {
	return r.indexPrefix + ".pvc" + ownerIndex
}

func (r *NodeReconciler[T, PT]) pvOwnerIndex() string {
	return r.indexPrefix + ".pv" + ownerIndex
}

func (r *NodeReconciler[T, PT]) setupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	r.indexPrefix = rand.String(10)

	if err := indexOwner[corev1.Pod](ctx, mgr, r.podOwnerIndex()); err != nil {
		return err
	}

	if err := indexOwner[corev1.PersistentVolumeClaim](ctx, mgr, r.pvcOwnerIndex()); err != nil {
		return err
	}

	if err := indexOwner[corev1.PersistentVolume](ctx, mgr, r.pvOwnerIndex()); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(newKubeObject[T, PT]()).
		Owns(&corev1.Pod{}).
		Owns(&corev1.PersistentVolume{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Complete(r)
}

func (r *NodeReconciler[T, PT]) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	node := newKubeObject[T, PT]()
	if err := r.Get(ctx, req.NamespacedName, node); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	pods, err := r.getClusterPods(ctx, node.GetName())
	if err != nil {
		logger.Error(err, "fetching cluster pods")
		return ctrl.Result{}, err
	}

	pvcs, err := r.getPersistentVolumeClaims(ctx, node.GetName())
	if err != nil {
		logger.Error(err, "fetching persistent volume claims")
		return ctrl.Result{}, err
	}

	pvs, err := r.getPersistentVolumes(ctx, node.GetName())
	if err != nil {
		logger.Error(err, "fetching persistent volumes")
		return ctrl.Result{}, err
	}

	originalStatus := node.GetClusterNodeStatus()
	status := originalStatus.DeepCopy()
	status.ObservedGeneration = node.GetGeneration()

	// set our cluster version immediately
	status.ClusterVersion = getHash(node)
	// if we don't match, overwrite our current matching status
	// which will only get updated when we've fully stabilized
	if originalStatus.ClusterVersion != status.ClusterVersion {
		status.MatchesCluster = false
	}

	markPodIfReady := func(pod *corev1.Pod) {
		status.Healthy = isHealthy(pod)
		status.Running = isRunningAndReady(pod)

		if isHealthy(pod) {
			// we have a one way operation of setting this as fully ready for the current cluster
			status.MatchesCluster = true
			setPhase(node, status, readyPhase("cluster node ready"))
		}
	}

	syncStatus := func(err error) (ctrl.Result, error) {
		updated := isNodeStatusDirty(&originalStatus, status)
		for _, condition := range []metav1.Condition{
			synchronizationCondition(node, err),
		} {
			if meta.SetStatusCondition(&status.Conditions, condition) {
				updated = true
			}
		}
		if updated {
			node.SetClusterNodeStatus(*status)
			syncErr := r.Client.Status().Update(ctx, node)
			return ctrl.Result{}, errors.Join(syncErr, err)
		}

		return ctrl.Result{}, err
	}

	pvsStable, unusedPVs, err := r.ensurePersistentVolumes(ctx, status, node, pvs)
	if err != nil {
		logger.Error(err, "ensuring pvs")
		return syncStatus(err)
	}
	if !pvsStable {
		// we don't yet have stable persistent volumes, so wait until we're fully reconciled
		return syncStatus(nil)
	}

	if len(pods) == 0 {
		pod, err := r.createPod(ctx, status, node, pvcs)
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

	podVersion := getHash(pod)
	if podVersion == "" || podVersion != status.CurrentVersion {
		if err := r.decommissionPod(ctx, status, node, pod); err != nil {
			logger.Error(err, "decommissioning pod with non-current version", "version", podVersion)
			return syncStatus(err)
		}
		return syncStatus(nil)
	}

	// if we detect a pod as finished, attempt to restart it
	if isFailed(pod) || isSucceeded(pod) {
		if err := r.restartPod(ctx, status, node, pod); err != nil {
			logger.Error(err, "restarting stopped pod")
			return syncStatus(err)
		}
		return syncStatus(nil)
	}

	// delete any unused pvcs
	_, unusedPVCs, _, err := r.neededPersistentVolumeClaims(node, pvcs)
	if err != nil {
		logger.Error(err, "getting list of unused pvcs")
		return syncStatus(err)
	}
	if len(unusedPVCs) > 0 {
		if err := r.decommissionPVCs(ctx, status, node, unusedPVCs); err != nil {
			logger.Error(err, "decommissioning unused pvcs")
			return syncStatus(err)
		}
		// wait until all PVCs are deleted then attempt all the PVs
		return syncStatus(nil)
	}

	// finally delete any unused pvs
	if len(unusedPVs) > 0 {
		if err := r.decommissionPVs(ctx, status, node, unusedPVs); err != nil {
			logger.Error(err, "decommissioning unused pvs")
			return syncStatus(err)
		}
		// wait until all PVs are deleted then see if we're all up-to-date
		return syncStatus(nil)
	}

	// if everything worked out and we have a ready pod, then mark the node as such
	markPodIfReady(pod)

	return syncStatus(nil)
}

func (r *NodeReconciler[T, PT]) ensurePersistentVolumes(ctx context.Context, status *clusterv1alpha1.ClusterNodeStatus, node PT, pvs []*corev1.PersistentVolume) (bool, []*corev1.PersistentVolume, error) {
	needed, unused, _, err := r.neededPersistentVolumes(node, pvs)
	if err != nil {
		return false, nil, fmt.Errorf("getting needed persistent volumes: %w", err)
	}
	used := map[types.NamespacedName]*corev1.PersistentVolume{}
	for _, pv := range pvs {
		used[client.ObjectKeyFromObject(pv)] = pv
	}
	for _, pv := range unused {
		delete(used, client.ObjectKeyFromObject(pv))
	}

	if len(needed) == 0 {
		// make sure that all of our currently used PVs are not failed
		for _, pv := range used {
			if pv.Status.Phase == corev1.VolumePending || pv.Status.Phase == corev1.VolumeFailed {
				setPhase(node, status, initializingPhase("waiting for persistent volumes to be ready"))
				return false, nil, nil
			}
		}
		// if any other status signal that we're ready to move to the next reconciliation step
		return true, unused, nil
	}

	setPhase(node, status, initializingPhase("creating persistent volumes"))
	volumes := []*corev1.PersistentVolume{}
	for _, volume := range needed {
		volumes = append(volumes, volume)
	}
	sortCreation(volumes)

	modifying := &ModifiedNodeObjects{
		PVs: volumes,
	}

	if err := runNodeLifecycleBeforeHook(ctx, node, modifying, r.hooks.Create); err != nil {
		return false, nil, fmt.Errorf("running pvs before create hook: %w", err)
	}

	errs := []error{}
	for _, pv := range volumes {
		// attempt to create as many pvs in one pass as we can
		if err := r.Client.Patch(ctx, pv, client.Apply, fieldOwner, client.ForceOwnership); err != nil {
			errs = append(errs, err)
		}
	}

	if err := errors.Join(errs...); err != nil {
		return false, nil, err
	}

	if err := runNodeLifecycleAfterHook(ctx, node, modifying, r.hooks.Create); err != nil {
		return false, nil, fmt.Errorf("running pvs after create hook: %w", err)
	}

	// we just attempted to create some PVs, but some might already be ready, so check
	for _, pv := range volumes {
		if pv.Status.Phase == corev1.VolumePending || pv.Status.Phase == corev1.VolumeFailed {
			return false, nil, nil
		}
	}

	return true, nil, nil
}

func (r *NodeReconciler[T, PT]) createPod(ctx context.Context, status *clusterv1alpha1.ClusterNodeStatus, node PT, pvcs []*corev1.PersistentVolumeClaim) (*corev1.Pod, error) {
	// we set the phase here in case of an early return due to an error
	setPhase(node, status, initializingPhase("generating pod"))

	pod, err := getPodFromTemplate(node.GetPodSpec(), node, metav1.NewControllerRef(node, node.GetObjectKind().GroupVersionKind()))
	if err != nil {
		return nil, fmt.Errorf("initializing pod from template: %w", err)
	}

	volumes := []string{}
	needed, _, found, err := r.neededPersistentVolumeClaims(node, pvcs)
	if err != nil {
		return nil, fmt.Errorf("getting list of pvcs: %w", err)
	}

	version, err := initPod(node, pod, needed)
	if err != nil {
		return nil, fmt.Errorf("initializing pod template: %w", err)
	}

	for _, claim := range found {
		volumes = append(volumes, fmt.Sprintf("\"%s/%s\"", claim.Namespace, claim.Name))
	}

	message := fmt.Sprintf("initializing pod \"%s/%s\"", pod.Namespace, pod.Name)
	if len(volumes) != 0 {
		message = fmt.Sprintf("%s, with volumes (%s)", message, strings.Join(volumes, ", "))
	}

	setPhase(node, status, initializingPhase(message))

	claims := []*corev1.PersistentVolumeClaim{}
	for _, claim := range needed {
		claims = append(claims, claim)
	}
	claims = sortCreation(claims)

	modifying := &ModifiedNodeObjects{
		Pod:  pod,
		PVCs: claims,
	}

	if err := runNodeLifecycleBeforeHook(ctx, node, modifying, r.hooks.Create); err != nil {
		return nil, fmt.Errorf("running pod and pvcs before create hook: %w", err)
	}

	for _, claim := range claims {
		if err := r.Client.Patch(ctx, claim, client.Apply, fieldOwner, client.ForceOwnership); err != nil {
			return nil, fmt.Errorf("applying persistent volume claim: %w", err)
		}
	}

	if err := r.Client.Patch(ctx, pod, client.Apply, fieldOwner, client.ForceOwnership); err != nil {
		return nil, fmt.Errorf("applying pod: %w", err)
	}

	status.PreviousVersion = status.CurrentVersion
	status.CurrentVersion = version

	if err := runNodeLifecycleAfterHook(ctx, node, modifying, r.hooks.Create); err != nil {
		return nil, fmt.Errorf("running pod and pvcs after create hook: %w", err)
	}

	return pod, nil
}

func (r *NodeReconciler[T, PT]) decommissionPVCs(ctx context.Context, status *clusterv1alpha1.ClusterNodeStatus, node PT, pvcs []*corev1.PersistentVolumeClaim) error {
	claims := []string{}
	for _, claim := range pvcs {
		claims = append(claims, fmt.Sprintf("\"%s/%s\"", claim.Namespace, claim.Name))
	}

	setPhase(node, status, decommissioningPhase(fmt.Sprintf("decommissioning persistent volume claims: (%s)", strings.Join(claims, ", "))))

	modifying := &ModifiedNodeObjects{
		PVCs: pvcs,
	}
	if err := runNodeLifecycleBeforeHook(ctx, node, modifying, r.hooks.Delete); err != nil {
		return fmt.Errorf("running pvc before delete hook: %w", err)
	}

	errs := []error{}
	for _, claim := range pvcs {
		if err := r.Client.Delete(ctx, claim); err != nil {
			if !k8sapierrors.IsNotFound(err) {
				errs = append(errs, err)
			}
		}
	}

	if err := errors.Join(errs...); err != nil {
		return err
	}

	if err := runNodeLifecycleAfterHook(ctx, node, modifying, r.hooks.Delete); err != nil {
		return fmt.Errorf("running pvcs after delete hook: %w", err)
	}

	return nil
}

func (r *NodeReconciler[T, PT]) decommissionPVs(ctx context.Context, status *clusterv1alpha1.ClusterNodeStatus, node PT, pvs []*corev1.PersistentVolume) error {
	volumes := []string{}
	for _, volume := range pvs {
		volumes = append(volumes, fmt.Sprintf("\"%s/%s\"", volume.Namespace, volume.Name))
	}

	setPhase(node, status, decommissioningPhase(fmt.Sprintf("decommissioning persistent volumes: (%s)", strings.Join(volumes, ", "))))

	modifying := &ModifiedNodeObjects{
		PVs: pvs,
	}
	if err := runNodeLifecycleBeforeHook(ctx, node, modifying, r.hooks.Delete); err != nil {
		return fmt.Errorf("running pvs before delete hook: %w", err)
	}

	errs := []error{}
	for _, claim := range pvs {
		if err := r.Client.Delete(ctx, claim); err != nil {
			if !k8sapierrors.IsNotFound(err) {
				errs = append(errs, err)
			}
		}
	}

	if err := errors.Join(errs...); err != nil {
		return err
	}

	if err := runNodeLifecycleAfterHook(ctx, node, modifying, r.hooks.Delete); err != nil {
		return fmt.Errorf("running pvs after delete hook: %w", err)
	}

	return nil
}

func (r *NodeReconciler[T, PT]) decommissionPod(ctx context.Context, status *clusterv1alpha1.ClusterNodeStatus, node PT, pod *corev1.Pod) error {
	setPhase(node, status, decommissioningPhase(fmt.Sprintf("decommissioning pod: %s/%s", pod.Namespace, pod.Name)))

	modifying := &ModifiedNodeObjects{
		Pod: pod,
	}
	if err := runNodeLifecycleBeforeHook(ctx, node, modifying, r.hooks.Delete); err != nil {
		return fmt.Errorf("running pod before delete hook: %w", err)
	}

	if err := r.Client.Delete(ctx, pod); err != nil {
		if !k8sapierrors.IsNotFound(err) {
			return fmt.Errorf("deleting pod: %w", err)
		}
	}

	if err := runNodeLifecycleAfterHook(ctx, node, modifying, r.hooks.Delete); err != nil {
		return fmt.Errorf("running pod after delete hook: %w", err)
	}

	return nil
}

func (r *NodeReconciler[T, PT]) restartPod(ctx context.Context, status *clusterv1alpha1.ClusterNodeStatus, node PT, pod *corev1.Pod) error {
	setPhase(node, status, restartingPhase(fmt.Sprintf("restarting pod: %s/%s", pod.Namespace, pod.Name)))

	modifying := &ModifiedNodeObjects{
		Pod: pod,
	}
	if err := runNodeLifecycleBeforeHook(ctx, node, modifying, r.hooks.Delete); err != nil {
		return fmt.Errorf("running pod before delete hook on restart: %w", err)
	}

	if err := r.Client.Delete(ctx, pod); err != nil {
		if !k8sapierrors.IsNotFound(err) {
			return fmt.Errorf("restarting pod: %w", err)
		}
	}

	if err := runNodeLifecycleAfterHook(ctx, node, modifying, r.hooks.Delete); err != nil {
		return fmt.Errorf("running pod after delete hook on restart: %w", err)
	}

	return nil
}

func (r *NodeReconciler[T, PT]) getClusterPods(ctx context.Context, nodeName string) ([]*corev1.Pod, error) {
	var pods corev1.PodList
	if err := r.Client.List(ctx, &pods, &client.ListOptions{
		FieldSelector: fields.OneTermEqualSelector(r.podOwnerIndex(), nodeName),
	}); err != nil {
		return nil, err
	}

	items := []*corev1.Pod{}
	for _, item := range pods.Items {
		items = append(items, ptr.To(item))
	}

	return sortCreation(items), nil
}

func (r *NodeReconciler[T, PT]) getPersistentVolumeClaims(ctx context.Context, nodeName string) ([]*corev1.PersistentVolumeClaim, error) {
	var pvcs corev1.PersistentVolumeClaimList
	if err := r.Client.List(ctx, &pvcs, &client.ListOptions{
		FieldSelector: fields.OneTermEqualSelector(r.pvcOwnerIndex(), nodeName),
	}); err != nil {
		return nil, err
	}

	items := []*corev1.PersistentVolumeClaim{}
	for _, item := range pvcs.Items {
		items = append(items, ptr.To(item))
	}

	return sortCreation(items), nil
}

func (r *NodeReconciler[T, PT]) getPersistentVolumes(ctx context.Context, nodeName string) ([]*corev1.PersistentVolume, error) {
	var pvs corev1.PersistentVolumeList
	if err := r.Client.List(ctx, &pvs, &client.ListOptions{
		FieldSelector: fields.OneTermEqualSelector(r.pvOwnerIndex(), nodeName),
	}); err != nil {
		return nil, err
	}

	items := []*corev1.PersistentVolume{}
	for _, item := range pvs.Items {
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

func neededPersistentVolumeObjects[T client.Object](node ClusterNodeObject, existing, desired []T, initializer func(node ClusterNodeObject, o T) error) (map[string]T, []T, []types.NamespacedName, error) {
	neededSet := map[string]T{}
	unusedSet := map[types.NamespacedName]T{}
	foundSet := map[types.NamespacedName]struct{}{}
	referencedSet := map[string]struct{}{}
	for _, o := range existing {
		unusedSet[client.ObjectKeyFromObject(o)] = o
	}

	for _, volume := range node.GetPodSpec().Spec.Volumes {
		if volume.PersistentVolumeClaim != nil {
			referencedSet[volume.PersistentVolumeClaim.ClaimName] = struct{}{}
		}
	}

	for _, o := range desired {
		originalName := o.GetName()

		if err := initializer(node, o); err != nil {
			return nil, nil, nil, fmt.Errorf("initializing persistent volume object: %w", err)
		}

		_, referenced := referencedSet[originalName]
		if referenced {
			foundSet[client.ObjectKeyFromObject(o)] = struct{}{}
		}

		if _, has := unusedSet[client.ObjectKeyFromObject(o)]; has {
			if referenced {
				delete(unusedSet, client.ObjectKeyFromObject(o))
			}
			continue
		}
		if referenced {
			neededSet[originalName] = o
		}
	}

	unused := []T{}
	found := []types.NamespacedName{}
	for _, o := range unusedSet {
		unused = append(unused, o)
	}
	for o := range foundSet {
		found = append(found, o)
	}

	sort.SliceStable(found, func(i, j int) bool {
		a, b := found[i], found[j]
		return a.String() < b.String()
	})

	return neededSet, sortCreation(unused), found, nil
}

func (r *NodeReconciler[T, PT]) neededPersistentVolumes(node PT, existing []*corev1.PersistentVolume) (map[string]*corev1.PersistentVolume, []*corev1.PersistentVolume, []types.NamespacedName, error) {
	return neededPersistentVolumeObjects(node, existing, node.GetVolumes(), initVolume)
}

func (r *NodeReconciler[T, PT]) neededPersistentVolumeClaims(node PT, existing []*corev1.PersistentVolumeClaim) (map[string]*corev1.PersistentVolumeClaim, []*corev1.PersistentVolumeClaim, []types.NamespacedName, error) {
	return neededPersistentVolumeObjects(node, existing, node.GetVolumeClaims(), initClaim)
}

func initPod(node ClusterNodeObject, pod *corev1.Pod, claims map[string]*corev1.PersistentVolumeClaim) (string, error) {
	hash, err := hashPodTemplate(node)
	if err != nil {
		return "", fmt.Errorf("hashing node pod template: %w", err)
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
	pod.Labels[hashLabel] = hash

	volumes := []corev1.Volume{}
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim == nil {
			volumes = append(volumes, volume)
		}

		if pvc, found := claims[volume.PersistentVolumeClaim.ClaimName]; found {
			volume.PersistentVolumeClaim.ClaimName = pvc.GetName()
			volumes = append(volumes, volume)
			continue
		}

		return "", fmt.Errorf("invalid persistent volume claim: %s", volume.PersistentVolumeClaim.ClaimName)
	}
	pod.Spec.Volumes = volumes

	return hash, nil
}

func initClaim(node ClusterNodeObject, claim *corev1.PersistentVolumeClaim) error {
	originalName := claim.Name

	claim.TypeMeta = metav1.TypeMeta{
		Kind:       "PersistentVolumeClaim",
		APIVersion: corev1.SchemeGroupVersion.String(),
	}
	claim.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(node, node.GetObjectKind().GroupVersionKind())}
	claim.Name = fmt.Sprintf("%s-%s", node.GetName(), originalName)
	claim.Spec.VolumeName = claim.Name
	claim.Namespace = node.GetNamespace()

	return nil
}

func initVolume(node ClusterNodeObject, volume *corev1.PersistentVolume) error {
	originalName := volume.Name

	volume.TypeMeta = metav1.TypeMeta{
		Kind:       "PersistentVolume",
		APIVersion: corev1.SchemeGroupVersion.String(),
	}
	volume.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(node, node.GetObjectKind().GroupVersionKind())}
	volume.Name = fmt.Sprintf("%s-%s", node.GetName(), originalName)
	volume.Namespace = node.GetNamespace()

	return nil
}

func getHash(o client.Object) string {
	labels := o.GetLabels()
	if labels == nil {
		return ""
	}
	return labels[hashLabel]
}

func hashPodTemplate(node ClusterNodeObject) (string, error) {
	hf := fnv.New32()

	specData, err := json.Marshal(node.GetPodSpec())
	if err != nil {
		return "", fmt.Errorf("marshaling pod spec: %w", err)
	}
	_, err = hf.Write(specData)
	if err != nil {
		return "", fmt.Errorf("hashing pod spec: %w", err)
	}

	for _, volume := range node.GetVolumeClaims() {
		volumeData, err := json.Marshal(volume)
		if err != nil {
			return "", fmt.Errorf("marshaling volume claim: %w", err)
		}
		_, err = hf.Write(volumeData)
		if err != nil {
			return "", fmt.Errorf("hashing volume claim: %w", err)
		}
	}

	return rand.SafeEncodeString(fmt.Sprint(hf.Sum32())), nil
}

type phasedStatus interface {
	SetPhase(phase clusterv1alpha1.Phase)
	GetPhase() clusterv1alpha1.Phase
}

func setPhase(o client.Object, status phasedStatus, phase clusterv1alpha1.Phase) {
	if status.GetPhase().Name == phase.Name && status.GetPhase().Message == phase.Message && status.GetPhase().ObservedGeneration == o.GetGeneration() {
		return
	}

	phase.LastTransitionTime = metav1.Now()
	phase.ObservedGeneration = o.GetGeneration()
	status.SetPhase(phase)
}

func synchronizationCondition(node ClusterNodeObject, err error) metav1.Condition {
	conditionStatus := metav1.ConditionTrue
	conditionReason := "Synchronized"
	conditionMessage := "cluster node successfully synchronized"
	if err != nil {
		conditionStatus = metav1.ConditionFalse
		conditionReason = "Error"
		conditionMessage = err.Error()
	}

	return metav1.Condition{
		Type:               "Syncronized",
		Status:             conditionStatus,
		Reason:             conditionReason,
		Message:            conditionMessage,
		ObservedGeneration: node.GetGeneration(),
	}
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

func isNodeStatusDirty(a, b *clusterv1alpha1.ClusterNodeStatus) bool {
	return a.ObservedGeneration != b.ObservedGeneration || a.MatchesCluster != b.MatchesCluster || a.ClusterVersion != b.ClusterVersion || a.Phase != b.Phase || a.CurrentVersion != b.CurrentVersion || a.PreviousVersion != b.PreviousVersion
}
