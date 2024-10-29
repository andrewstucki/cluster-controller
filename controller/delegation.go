package controller

import (
	clusterv1alpha1 "github.com/andrewstucki/cluster-controller/controller/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
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
	GetHash() (string, error)
	GetStatus() clusterv1alpha1.ClusterStatus
	SetStatus(status clusterv1alpha1.ClusterStatus)
	GetReplicas() int
	GetMinimumHealthyReplicas() int
	GetNode() PU

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

type DelegatingClusterFactory[T, U any, PT ClusterObject[T, U, PU], PU ClusterNodeObject[U]] struct{}

func NewDelegatingClusterFactory[T, U any, PT ClusterObject[T, U, PU], PU ClusterNodeObject[U]]() ClusterFactory[T, U, PT, PU] {
	return &DelegatingClusterFactory[T, U, PT, PU]{}
}

func (m *DelegatingClusterFactory[T, U, PT, PU]) HashCluster(cluster PT) (string, error) {
	return cluster.GetHash()
}

func (m *DelegatingClusterFactory[T, U, PT, PU]) GetClusterStatus(cluster PT) clusterv1alpha1.ClusterStatus {
	return cluster.GetStatus()
}

func (m *DelegatingClusterFactory[T, U, PT, PU]) SetClusterStatus(cluster PT, status clusterv1alpha1.ClusterStatus) {
	cluster.SetStatus(status)
}

func (m *DelegatingClusterFactory[T, U, PT, PU]) GetClusterReplicas(cluster PT) int {
	return cluster.GetReplicas()
}

func (m *DelegatingClusterFactory[T, U, PT, PU]) GetClusterMinimumHealthyReplicas(cluster PT) int {
	return cluster.GetMinimumHealthyReplicas()
}

func (m *DelegatingClusterFactory[T, U, PT, PU]) GetClusterNode(cluster PT) PU {
	return cluster.GetNode()
}

func (m *DelegatingClusterFactory[T, U, PT, PU]) HashClusterNode(node PU) (string, error) {
	return node.GetHash()
}

func (m *DelegatingClusterFactory[T, U, PT, PU]) GetClusterNodeStatus(node PU) clusterv1alpha1.ClusterNodeStatus {
	return node.GetStatus()
}

func (m *DelegatingClusterFactory[T, U, PT, PU]) SetClusterNodeStatus(node PU, status clusterv1alpha1.ClusterNodeStatus) {
	node.SetStatus(status)
}

func (m *DelegatingClusterFactory[T, U, PT, PU]) GetClusterNodePod(node PU) *corev1.Pod {
	return node.GetPod()
}
