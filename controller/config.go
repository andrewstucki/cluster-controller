package controller

import (
	"context"
	"fmt"

	clusterv1alpha1 "github.com/andrewstucki/cluster-controller/controller/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/rand"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type ClusterManager[T, U any, PT ptrToObject[T], PU ptrToObject[U]] interface {
	// Clusters
	HashCluster(cluster PT) (string, error)
	GetClusterStatus(cluster PT) clusterv1alpha1.ClusterStatus
	SetClusterStatus(cluster PT, status clusterv1alpha1.ClusterStatus)
	GetClusterReplicas(cluster PT) int
	GetClusterMinimumHealthyReplicas(cluster PT) int
	GetClusterNodeTemplate(cluster PT) PU

	// Nodes
	HashClusterNode(node PU) (string, error)
	GetClusterNodeStatus(node PU) clusterv1alpha1.ClusterNodeStatus
	SetClusterNodeStatus(node PU, status clusterv1alpha1.ClusterNodeStatus)
	GetClusterNodePodSpec(node PU) *corev1.PodTemplateSpec
	GetClusterNodeVolumes(node PU) []*corev1.PersistentVolume
	GetClusterNodeVolumeClaims(node PU) []*corev1.PersistentVolumeClaim
}

const (
	defaultFieldOwner     = "cluster-controller"
	defaultHashLabel      = "cluster-hash"
	defaultClusterLabel   = "cluster-name"
	defaultNodeLabel      = "cluster-node-name"
	defaultNamespaceLabel = "cluster-namespace"
	defaultFinalizer      = "cluster-finalizer"
)

type Config[T, U any, PT ptrToObject[T], PU ptrToObject[U]] struct {
	Scheme         *runtime.Scheme
	Client         client.Client
	Manager        ClusterManager[T, U, PT, PU]
	FieldOwner     client.FieldOwner
	HashLabel      string
	NamespaceLabel string
	NodeLabel      string
	ClusterLabel   string
	Finalizer      string
	IndexPrefix    string
	Testing        bool
}

type ConfigBuilder[T, U any, PT ptrToObject[T], PU ptrToObject[U]] struct {
	runtimeManager ctrl.Manager
	manager        ClusterManager[T, U, PT, PU]
	fieldOwner     client.FieldOwner
	hashLabel      string
	clusterLabel   string
	nodeLabel      string
	namespaceLabel string
	finalizer      string
	indexPrefix    string
	testing        bool
}

func New[T, U any, PT ptrToObject[T], PU ptrToObject[U]](runtimeManager ctrl.Manager, manager ClusterManager[T, U, PT, PU]) *ConfigBuilder[T, U, PT, PU] {
	return &ConfigBuilder[T, U, PT, PU]{
		runtimeManager: runtimeManager,
		manager:        manager,
		fieldOwner:     defaultFieldOwner,
		hashLabel:      defaultHashLabel,
		clusterLabel:   defaultClusterLabel,
		nodeLabel:      defaultNodeLabel,
		namespaceLabel: defaultNamespaceLabel,
		finalizer:      defaultFinalizer,
		indexPrefix:    rand.String(4),
		testing:        false,
	}
}

func (c *ConfigBuilder[T, U, PT, PU]) WithFieldOwner(owner client.FieldOwner) *ConfigBuilder[T, U, PT, PU] {
	c.fieldOwner = owner
	return c
}

func (c *ConfigBuilder[T, U, PT, PU]) WithFinalizer(finalizer string) *ConfigBuilder[T, U, PT, PU] {
	c.finalizer = finalizer
	return c
}

func (c *ConfigBuilder[T, U, PT, PU]) WithHashLabel(label string) *ConfigBuilder[T, U, PT, PU] {
	c.hashLabel = label
	return c
}

func (c *ConfigBuilder[T, U, PT, PU]) WithClusterLabel(label string) *ConfigBuilder[T, U, PT, PU] {
	c.clusterLabel = label
	return c
}

func (c *ConfigBuilder[T, U, PT, PU]) WithNodeLabel(label string) *ConfigBuilder[T, U, PT, PU] {
	c.nodeLabel = label
	return c
}

func (c *ConfigBuilder[T, U, PT, PU]) WithNamespaceLabel(label string) *ConfigBuilder[T, U, PT, PU] {
	c.namespaceLabel = label
	return c
}

func (c *ConfigBuilder[T, U, PT, PU]) Testing() *ConfigBuilder[T, U, PT, PU] {
	c.testing = true
	return c
}

func (c *ConfigBuilder[T, U, PT, PU]) Setup(ctx context.Context) error {

	config := &Config[T, U, PT, PU]{
		Scheme:         c.runtimeManager.GetScheme(),
		Client:         c.runtimeManager.GetClient(),
		Manager:        c.manager,
		HashLabel:      c.hashLabel,
		ClusterLabel:   c.clusterLabel,
		NodeLabel:      c.nodeLabel,
		NamespaceLabel: c.namespaceLabel,
		FieldOwner:     c.fieldOwner,
		Finalizer:      c.finalizer,
		IndexPrefix:    c.indexPrefix,
		Testing:        c.testing,
	}

	if err := SetupClusterReconciler(c.runtimeManager, config); err != nil {
		return fmt.Errorf("setting up cluster reconciler: %w", err)
	}

	if err := SetupClusterNodeReconciler(ctx, c.runtimeManager, config); err != nil {
		return fmt.Errorf("setting up cluster node reconciler: %w", err)
	}

	return nil
}
