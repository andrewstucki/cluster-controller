package controller

import (
	clusterv1alpha1 "github.com/andrewstucki/cluster-controller/controller/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

type ClusterFactory[T, U any, PT ptrToObject[T], PU ptrToObject[U]] interface {
	// Clusters
	HashCluster(cluster PT) (string, error)
	GetClusterStatus(cluster PT) clusterv1alpha1.ClusterStatus
	SetClusterStatus(cluster PT, status clusterv1alpha1.ClusterStatus)
	GetClusterReplicas(cluster PT) int
	GetClusterMinimumHealthyReplicas(cluster PT) int
	GetClusterNode(cluster PT) PU

	// Nodes
	HashClusterNode(node PU) (string, error)
	GetClusterNodeStatus(node PU) clusterv1alpha1.ClusterNodeStatus
	SetClusterNodeStatus(node PU, status clusterv1alpha1.ClusterNodeStatus)
	GetClusterNodePod(node PU) *corev1.Pod
}
