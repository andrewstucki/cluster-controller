package controller

import (
	clusterv1alpha1 "github.com/andrewstucki/cluster-controller/controller/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type ClusterObject[T, U any, PU ClusterNodeObject[U]] interface {
	client.Object
	GetHash() (string, error)
	GetStatus() clusterv1alpha1.ClusterStatus
	SetStatus(status clusterv1alpha1.ClusterStatus)
	GetReplicas() int
	GetMinimumHealthyReplicas() int
	GetNodeTemplate() PU

	*T
}

type ClusterNodeObject[U any] interface {
	client.Object
	GetHash() (string, error)
	GetStatus() clusterv1alpha1.ClusterNodeStatus
	SetStatus(status clusterv1alpha1.ClusterNodeStatus)
	GetPodSpec() *corev1.PodTemplateSpec
	GetVolumes() []*corev1.PersistentVolume
	GetVolumeClaims() []*corev1.PersistentVolumeClaim

	*U
}

type DelegatingClusterManager[T, U any, PT ClusterObject[T, U, PU], PU ClusterNodeObject[U]] struct {
	UnimplementedCallbacks[T, U, PT, PU]
}

func (m *DelegatingClusterManager[T, U, PT, PU]) HashCluster(cluster PT) (string, error) {
	return cluster.GetHash()
}

func (m *DelegatingClusterManager[T, U, PT, PU]) GetClusterStatus(cluster PT) clusterv1alpha1.ClusterStatus {
	return cluster.GetStatus()
}

func (m *DelegatingClusterManager[T, U, PT, PU]) SetClusterStatus(cluster PT, status clusterv1alpha1.ClusterStatus) {
	cluster.SetStatus(status)
}

func (m *DelegatingClusterManager[T, U, PT, PU]) GetClusterReplicas(cluster PT) int {
	return cluster.GetReplicas()
}

func (m *DelegatingClusterManager[T, U, PT, PU]) GetClusterMinimumHealthyReplicas(cluster PT) int {
	return cluster.GetMinimumHealthyReplicas()
}

func (m *DelegatingClusterManager[T, U, PT, PU]) GetClusterNodeTemplate(cluster PT) PU {
	return cluster.GetNodeTemplate()
}

func (m *DelegatingClusterManager[T, U, PT, PU]) HashClusterNode(node PU) (string, error) {
	return node.GetHash()
}

func (m *DelegatingClusterManager[T, U, PT, PU]) GetClusterNodeStatus(node PU) clusterv1alpha1.ClusterNodeStatus {
	return node.GetStatus()
}

func (m *DelegatingClusterManager[T, U, PT, PU]) SetClusterNodeStatus(node PU, status clusterv1alpha1.ClusterNodeStatus) {
	node.SetStatus(status)
}

func (m *DelegatingClusterManager[T, U, PT, PU]) GetClusterNodePodSpec(node PU) *corev1.PodTemplateSpec {
	return node.GetPodSpec()
}

func (m *DelegatingClusterManager[T, U, PT, PU]) GetClusterNodeVolumes(node PU) []*corev1.PersistentVolume {
	return node.GetVolumes()
}

func (m *DelegatingClusterManager[T, U, PT, PU]) GetClusterNodeVolumeClaims(node PU) []*corev1.PersistentVolumeClaim {
	return node.GetVolumeClaims()
}
