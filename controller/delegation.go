package controller

import (
	clusterv1alpha1 "github.com/andrewstucki/cluster-controller/controller/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type SubresourceObject[T any] interface {
	client.Object

	ClusterScopedSubresources() []client.Object
	NamespaceScopedSubresources() []client.Object

	*T
}

type DelegatingResourceFactory[T any, PT SubresourceObject[T]] struct {
	clusterScopedResourceTypes   []client.Object
	namespaceScopedResourceTypes []client.Object
}

func NewDelegatingResourceFactory[T any, PT SubresourceObject[T]](clusterResources, namespaceResources []client.Object) *DelegatingResourceFactory[T, PT] {
	return &DelegatingResourceFactory[T, PT]{
		clusterScopedResourceTypes:   clusterResources,
		namespaceScopedResourceTypes: namespaceResources,
	}
}

func (d *DelegatingResourceFactory[T, PT]) ClusterScopedResourceTypes() []client.Object {
	return d.clusterScopedResourceTypes
}

func (d *DelegatingResourceFactory[T, PT]) NamespaceScopedResourceTypes() []client.Object {
	return d.namespaceScopedResourceTypes
}

func (d *DelegatingResourceFactory[T, PT]) ClusterScopedResources(owner PT) []client.Object {
	return owner.ClusterScopedSubresources()
}

func (d *DelegatingResourceFactory[T, PT]) NamespaceScopedResources(owner PT) []client.Object {
	return owner.NamespaceScopedSubresources()
}

type ClusterObject[T, U any, PU ClusterNodeObject[U]] interface {
	client.Object
	GetStatus() clusterv1alpha1.ClusterStatus
	SetStatus(status clusterv1alpha1.ClusterStatus)
	GetReplicas() int
	GetMinimumHealthyReplicas() int
	GetHash() (string, error)
	GetNode() PU

	*T
}

type PooledClusterObject[T any] interface {
	client.Object
	GetStatus() clusterv1alpha1.ClusterStatus
	SetStatus(status clusterv1alpha1.ClusterStatus)
	GetMinimumHealthyReplicas() int
	GetPoolsOptions() []client.ListOption

	*T
}

type ClusterPoolObject[T, U any, PU ClusterNodeObject[U]] interface {
	client.Object
	GetHash() (string, error)
	GetStatus() clusterv1alpha1.ClusterPoolStatus
	SetStatus(status clusterv1alpha1.ClusterPoolStatus)
	GetReplicas() int
	GetNode() PU
	GetCluster() types.NamespacedName

	*T
}

type ClusterNodeObject[U any] interface {
	client.Object
	GetHash() (string, error)
	GetStatus() clusterv1alpha1.ClusterNodeStatus
	SetStatus(status clusterv1alpha1.ClusterNodeStatus)
	GetPod() *corev1.Pod

	*U
}

type DelegatingClusterFactory[T, V any, PT ClusterObject[T, V, PV], PV ClusterNodeObject[V]] struct{}

func NewDelegatingClusterFactory[T, V any, PT ClusterObject[T, V, PV], PV ClusterNodeObject[V]]() ClusterFactory[T, V, PT, PV] {
	return &DelegatingClusterFactory[T, V, PT, PV]{}
}

func (m *DelegatingClusterFactory[T, V, PT, PV]) GetClusterStatus(cluster PT) clusterv1alpha1.ClusterStatus {
	return cluster.GetStatus()
}

func (m *DelegatingClusterFactory[T, V, PT, PV]) SetClusterStatus(cluster PT, status clusterv1alpha1.ClusterStatus) {
	cluster.SetStatus(status)
}

func (m *DelegatingClusterFactory[T, V, PT, PV]) GetClusterMinimumHealthyReplicas(cluster PT) int {
	return cluster.GetMinimumHealthyReplicas()
}

func (m *DelegatingClusterFactory[T, V, PT, PV]) HashCluster(cluster PT) (string, error) {
	return cluster.GetHash()
}

func (m *DelegatingClusterFactory[T, V, PT, PV]) GetClusterReplicas(cluster PT) int {
	return cluster.GetReplicas()
}

func (m *DelegatingClusterFactory[T, V, PT, PV]) GetClusterNode(cluster PT) PV {
	return cluster.GetNode()
}

func (m *DelegatingClusterFactory[T, V, PT, PV]) HashClusterNode(node PV) (string, error) {
	return node.GetHash()
}

func (m *DelegatingClusterFactory[T, V, PT, PV]) GetClusterNodeStatus(node PV) clusterv1alpha1.ClusterNodeStatus {
	return node.GetStatus()
}

func (m *DelegatingClusterFactory[T, V, PT, PV]) SetClusterNodeStatus(node PV, status clusterv1alpha1.ClusterNodeStatus) {
	node.SetStatus(status)
}

func (m *DelegatingClusterFactory[T, V, PT, PV]) GetClusterNodePod(node PV) *corev1.Pod {
	return node.GetPod()
}

type DelegatingPooledClusterFactory[T, U, V any, PT PooledClusterObject[T], PU ClusterPoolObject[U, V, PV], PV ClusterNodeObject[V]] struct{}

func NewDelegatingPooledClusterFactory[T, U, V any, PT PooledClusterObject[T], PU ClusterPoolObject[U, V, PV], PV ClusterNodeObject[V]]() PooledClusterFactory[T, U, V, PT, PU, PV] {
	return &DelegatingPooledClusterFactory[T, U, V, PT, PU, PV]{}
}

func (m *DelegatingPooledClusterFactory[T, U, V, PT, PU, PV]) GetClusterStatus(cluster PT) clusterv1alpha1.ClusterStatus {
	return cluster.GetStatus()
}

func (m *DelegatingPooledClusterFactory[T, U, V, PT, PU, PV]) SetClusterStatus(cluster PT, status clusterv1alpha1.ClusterStatus) {
	cluster.SetStatus(status)
}

func (m *DelegatingPooledClusterFactory[T, U, V, PT, PU, PV]) GetClusterMinimumHealthyReplicas(cluster PT) int {
	return cluster.GetMinimumHealthyReplicas()
}

func (m *DelegatingPooledClusterFactory[T, U, V, PT, PU, PV]) GetClusterPoolsOptions(cluster PT) []client.ListOption {
	return cluster.GetPoolsOptions()
}

func (m *DelegatingPooledClusterFactory[T, U, V, PT, PU, PV]) HashClusterPool(pool PU) (string, error) {
	return pool.GetHash()
}

func (m *DelegatingPooledClusterFactory[T, U, V, PT, PU, PV]) GetClusterPoolReplicas(pool PU) int {
	return pool.GetReplicas()
}

func (m *DelegatingPooledClusterFactory[T, U, V, PT, PU, PV]) GetClusterPoolNode(pool PU) PV {
	return pool.GetNode()
}

func (m *DelegatingPooledClusterFactory[T, U, V, PT, PU, PV]) GetClusterPoolStatus(pool PU) clusterv1alpha1.ClusterPoolStatus {
	return pool.GetStatus()
}

func (m *DelegatingPooledClusterFactory[T, U, V, PT, PU, PV]) SetClusterPoolStatus(pool PU, status clusterv1alpha1.ClusterPoolStatus) {
	pool.SetStatus(status)
}

func (m *DelegatingPooledClusterFactory[T, U, V, PT, PU, PV]) GetClusterForPool(pool PU) types.NamespacedName {
	return pool.GetCluster()
}

func (m *DelegatingPooledClusterFactory[T, U, V, PT, PU, PV]) HashClusterNode(node PV) (string, error) {
	return node.GetHash()
}

func (m *DelegatingPooledClusterFactory[T, U, V, PT, PU, PV]) GetClusterNodeStatus(node PV) clusterv1alpha1.ClusterNodeStatus {
	return node.GetStatus()
}

func (m *DelegatingPooledClusterFactory[T, U, V, PT, PU, PV]) SetClusterNodeStatus(node PV, status clusterv1alpha1.ClusterNodeStatus) {
	node.SetStatus(status)
}

func (m *DelegatingPooledClusterFactory[T, U, V, PT, PU, PV]) GetClusterNodePod(node PV) *corev1.Pod {
	return node.GetPod()
}
