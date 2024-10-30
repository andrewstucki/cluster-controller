package controller

import (
	clusterv1alpha1 "github.com/andrewstucki/cluster-controller/controller/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type virtualPool[T any, PT ptrToObject[T]] struct {
	cluster PT
}

func (v *virtualPool[T, PT]) DeepCopyObject() runtime.Object {
	return &virtualPool[T, PT]{cluster: v.cluster.DeepCopyObject().(PT)}
}
func (v *virtualPool[T, PT]) GetAnnotations() map[string]string {
	return v.cluster.GetAnnotations()
}
func (v *virtualPool[T, PT]) GetCreationTimestamp() metav1.Time {
	return v.cluster.GetCreationTimestamp()
}
func (v *virtualPool[T, PT]) GetDeletionGracePeriodSeconds() *int64 {
	return v.cluster.GetDeletionGracePeriodSeconds()
}
func (v *virtualPool[T, PT]) GetDeletionTimestamp() *metav1.Time {
	return v.cluster.GetDeletionTimestamp()
}
func (v *virtualPool[T, PT]) GetFinalizers() []string {
	return v.cluster.GetFinalizers()
}
func (v *virtualPool[T, PT]) GetGenerateName() string {
	return v.cluster.GetGenerateName()
}
func (v *virtualPool[T, PT]) GetGeneration() int64 {
	return v.cluster.GetGeneration()
}
func (v *virtualPool[T, PT]) GetLabels() map[string]string {
	return v.cluster.GetLabels()
}
func (v *virtualPool[T, PT]) GetManagedFields() []metav1.ManagedFieldsEntry {
	return v.cluster.GetManagedFields()
}
func (v *virtualPool[T, PT]) GetName() string {
	return v.cluster.GetName()
}
func (v *virtualPool[T, PT]) GetNamespace() string {
	return v.cluster.GetNamespace()
}
func (v *virtualPool[T, PT]) GetObjectKind() schema.ObjectKind {
	return v.cluster.GetObjectKind()
}
func (v *virtualPool[T, PT]) GetOwnerReferences() []metav1.OwnerReference {
	return v.cluster.GetOwnerReferences()
}
func (v *virtualPool[T, PT]) GetResourceVersion() string {
	return v.cluster.GetResourceVersion()
}
func (v *virtualPool[T, PT]) GetSelfLink() string {
	return v.cluster.GetSelfLink()
}
func (v *virtualPool[T, PT]) GetUID() types.UID {
	return v.cluster.GetUID()
}
func (v *virtualPool[T, PT]) SetAnnotations(annotations map[string]string) {
	v.cluster.SetAnnotations(annotations)
}
func (v *virtualPool[T, PT]) SetCreationTimestamp(timestamp metav1.Time) {
	v.cluster.SetCreationTimestamp(timestamp)
}
func (v *virtualPool[T, PT]) SetDeletionGracePeriodSeconds(gracePeriod *int64) {
	v.cluster.SetDeletionGracePeriodSeconds(gracePeriod)
}
func (v *virtualPool[T, PT]) SetDeletionTimestamp(timestamp *metav1.Time) {
	v.cluster.SetDeletionTimestamp(timestamp)
}
func (v *virtualPool[T, PT]) SetFinalizers(finalizers []string) {
	v.cluster.SetFinalizers(finalizers)
}
func (v *virtualPool[T, PT]) SetGenerateName(name string) {
	v.cluster.SetGenerateName(name)
}
func (v *virtualPool[T, PT]) SetGeneration(generation int64) {
	v.cluster.SetGeneration(generation)
}
func (v *virtualPool[T, PT]) SetLabels(labels map[string]string) {
	v.cluster.SetLabels(labels)
}
func (v *virtualPool[T, PT]) SetManagedFields(managedFields []metav1.ManagedFieldsEntry) {
	v.cluster.SetManagedFields(managedFields)
}
func (v *virtualPool[T, PT]) SetName(name string) {
	v.cluster.SetName(name)
}
func (v *virtualPool[T, PT]) SetNamespace(namespace string) {
	v.cluster.SetNamespace(namespace)
}
func (v *virtualPool[T, PT]) SetOwnerReferences(refs []metav1.OwnerReference) {
	v.cluster.SetOwnerReferences(refs)
}
func (v *virtualPool[T, PT]) SetResourceVersion(version string) {
	v.cluster.SetResourceVersion(version)
}
func (v *virtualPool[T, PT]) SetSelfLink(selfLink string) {
	v.cluster.SetSelfLink(selfLink)
}
func (v *virtualPool[T, PT]) SetUID(uid types.UID) {
	v.cluster.SetUID(uid)
}

type virtualPooledClusterFactory[T, V any, PT ptrToObject[T], PV ptrToObject[V]] struct {
	ClusterFactory[T, V, PT, PV]
}

func (v *virtualPooledClusterFactory[T, V, PT, PV]) HashClusterPool(pool *virtualPool[T, PT]) (string, error) {
	return v.ClusterFactory.HashCluster(pool.cluster)
}

func (v *virtualPooledClusterFactory[T, V, PT, PV]) GetClusterPoolNode(pool *virtualPool[T, PT]) PV {
	return v.ClusterFactory.GetClusterNode(pool.cluster)
}

func (v *virtualPooledClusterFactory[T, V, PT, PV]) GetClusterPoolReplicas(pool *virtualPool[T, PT]) int {
	return v.ClusterFactory.GetClusterReplicas(pool.cluster)
}

func (v *virtualPooledClusterFactory[T, V, PT, PV]) GetClusterPoolStatus(pool *virtualPool[T, PT]) clusterv1alpha1.ClusterPoolStatus {
	return clusterv1alpha1.ClusterPoolStatus{}
}

func (v *virtualPooledClusterFactory[T, V, PT, PV]) SetClusterPoolStatus(pool *virtualPool[T, PT], status clusterv1alpha1.ClusterPoolStatus) {
}

func (v *virtualPooledClusterFactory[T, V, PT, PV]) GetClusterForPool(pool *virtualPool[T, PT]) types.NamespacedName {
	return types.NamespacedName{}
}

func (v *virtualPooledClusterFactory[T, V, PT, PV]) GetClusterPoolsOptions(cluster PT) []client.ListOption {
	return nil
}

func wrapClusterFactory[T, V any, PT ptrToObject[T], PV ptrToObject[V]](factory ClusterFactory[T, V, PT, PV]) PooledClusterFactory[T, virtualPool[T, PT], V, PT, *virtualPool[T, PT], PV] {
	return &virtualPooledClusterFactory[T, V, PT, PV]{ClusterFactory: factory}
}
