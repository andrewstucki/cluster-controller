package controller

import (
	clusterv1alpha1 "github.com/andrewstucki/cluster-controller/controller/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type ClusterFactory[T, V any, PT ptrToObject[T], PV ptrToObject[V]] interface {
	// Clusters
	HashCluster(cluster PT) (string, error)
	GetClusterStatus(cluster PT) clusterv1alpha1.ClusterStatus
	SetClusterStatus(cluster PT, status clusterv1alpha1.ClusterStatus)
	GetClusterReplicas(cluster PT) int
	GetClusterMinimumHealthyReplicas(cluster PT) int
	GetClusterNode(cluster PT) PV

	// Nodes
	HashClusterNode(node PV) (string, error)
	GetClusterNodeStatus(node PV) clusterv1alpha1.ClusterNodeStatus
	SetClusterNodeStatus(node PV, status clusterv1alpha1.ClusterNodeStatus)
	GetClusterNodePod(node PV) *corev1.Pod
}

type PooledClusterFactory[T, U, V any, PT ptrToObject[T], PU ptrToObject[U], PV ptrToObject[V]] interface {
	// Clusters
	GetClusterStatus(cluster PT) clusterv1alpha1.ClusterStatus
	SetClusterStatus(cluster PT, status clusterv1alpha1.ClusterStatus)
	GetClusterMinimumHealthyReplicas(cluster PT) int
	GetClusterPoolsOptions(cluster PT) []client.ListOption

	// Pools
	HashClusterPool(pool PU) (string, error)
	GetClusterPoolNode(pool PU) PV
	GetClusterPoolStatus(pool PU) clusterv1alpha1.ClusterPoolStatus
	SetClusterPoolStatus(pool PU, status clusterv1alpha1.ClusterPoolStatus)
	GetClusterPoolReplicas(pool PU) int
	GetClusterForPool(pool PU) types.NamespacedName

	// Nodes
	HashClusterNode(node PV) (string, error)
	GetClusterNodeStatus(node PV) clusterv1alpha1.ClusterNodeStatus
	SetClusterNodeStatus(node PV, status clusterv1alpha1.ClusterNodeStatus)
	GetClusterNodePod(node PV) *corev1.Pod
}
