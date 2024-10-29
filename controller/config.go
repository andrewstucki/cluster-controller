package controller

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	defaultFieldOwner     = "cluster-controller"
	defaultHashLabel      = "cluster-hash"
	defaultClusterLabel   = "cluster-name"
	defaultOwnerTypeLabel = "cluster-owner-type"
	defaultNodeLabel      = "cluster-node-name"
	defaultNamespaceLabel = "cluster-namespace"
	defaultFinalizer      = "cluster-finalizer"
)

type ClusterLabels struct {
	HashLabel      string
	NamespaceLabel string
	OwnerTypeLabel string
	NodeLabel      string
	ClusterLabel   string
}

type Config[T, U any, PT ptrToObject[T], PU ptrToObject[U]] struct {
	Scheme                 *runtime.Scheme
	Client                 client.Client
	Subscriber             LifecycleSubscriber[T, U, PT, PU]
	Factory                ClusterFactory[T, U, PT, PU]
	Labels                 ClusterLabels
	ClusterResourceFactory ResourceFactory[T, PT]
	NodeResourceFactory    ResourceFactory[U, PU]
	FieldOwner             client.FieldOwner
	Finalizer              string
	Testing                bool
}

func (c *Config[T, U, PT, PU]) clusterNormalizer() *Normalizer[T, PT] {
	return &Normalizer[T, PT]{
		labels: func(owner PT) map[string]string {
			return map[string]string{
				c.Labels.ClusterLabel:   owner.GetName(),
				c.Labels.NamespaceLabel: owner.GetNamespace(),
				c.Labels.OwnerTypeLabel: "cluster",
			}
		},
		scheme: c.Scheme,
		mapper: c.Client.RESTMapper(),
	}
}

func (c *Config[T, U, PT, PU]) nodeNormalizer() *Normalizer[U, PU] {
	return &Normalizer[U, PU]{
		labels: func(owner PU) map[string]string {
			return map[string]string{
				c.Labels.ClusterLabel:   owner.GetLabels()[c.Labels.ClusterLabel],
				c.Labels.NodeLabel:      owner.GetName(),
				c.Labels.NamespaceLabel: owner.GetNamespace(),
				c.Labels.OwnerTypeLabel: "node",
			}
		},
		scheme: c.Scheme,
		mapper: c.Client.RESTMapper(),
	}
}

func (c *Config[T, U, PT, PU]) clusterResourceClient() *ResourceClient[T, PT] {
	return &ResourceClient[T, PT]{
		normalizer: c.clusterNormalizer(),
		client:     c.Client,
		fieldOwner: c.FieldOwner,
	}
}

func (c *Config[T, U, PT, PU]) nodeResourceClient() *ResourceClient[U, PU] {
	return &ResourceClient[U, PU]{
		normalizer: c.nodeNormalizer(),
		client:     c.Client,
		fieldOwner: c.FieldOwner,
	}
}

func (c *Config[T, U, PT, PU]) clusterNodeManager() *NodeManager[T, U, PT, PU] {
	return &NodeManager[T, U, PT, PU]{
		ResourceClient: c.clusterResourceClient(),
		normalizer:     c.clusterNormalizer(),
		hashLabel:      c.Labels.HashLabel,
		getNode:        c.Factory.GetClusterNode,
		hasher:         c.Factory.HashCluster,
		subscriber:     c.Subscriber,
	}
}

func (c *Config[T, U, PT, PU]) clusterResourceManager() *ResourceManager[T, PT] {
	return &ResourceManager[T, PT]{
		factory: c.ClusterResourceFactory,
		client:  c.clusterResourceClient(),
		matchesType: func(m map[string]string) bool {
			return m != nil && m[c.Labels.OwnerTypeLabel] == "cluster"
		},
		ownerFromLabels: func(m map[string]string) types.NamespacedName {
			if m == nil {
				return types.NamespacedName{}
			}
			return types.NamespacedName{Namespace: m[c.Labels.NamespaceLabel], Name: m[c.Labels.ClusterLabel]}
		},
	}
}

func (c *Config[T, U, PT, PU]) nodeResourceManager() *ResourceManager[U, PU] {
	return &ResourceManager[U, PU]{
		factory: c.NodeResourceFactory,
		client:  c.nodeResourceClient(),
		matchesType: func(m map[string]string) bool {
			return m != nil && m[c.Labels.OwnerTypeLabel] == "node"
		},
		ownerFromLabels: func(m map[string]string) types.NamespacedName {
			if m == nil {
				return types.NamespacedName{}
			}
			return types.NamespacedName{Namespace: m[c.Labels.NamespaceLabel], Name: m[c.Labels.NodeLabel]}
		},
	}
}

func (c *Config[T, U, PT, PU]) podManager() *PodManager[U, PU] {
	return &PodManager[U, PU]{
		ResourceClient: c.nodeResourceClient(),
		hashFn:         c.clusterNodeManager().GetHash,
		hashLabel:      c.Labels.HashLabel,
		podFn:          c.Factory.GetClusterNodePod,
		testing:        c.Testing,
	}
}

type ConfigBuilder[T, U any, PT ptrToObject[T], PU ptrToObject[U]] struct {
	runtimeManager ctrl.Manager
	factory        ClusterFactory[T, U, PT, PU]

	subscriber             LifecycleSubscriber[T, U, PT, PU]
	clusterResourceFactory ResourceFactory[T, PT]
	nodeResourceFactory    ResourceFactory[U, PU]

	fieldOwner client.FieldOwner
	finalizer  string

	hashLabel      string
	clusterLabel   string
	nodeLabel      string
	namespaceLabel string
	ownerTypeLabel string

	testing bool
}

func New[T, U any, PT ptrToObject[T], PU ptrToObject[U]](runtimeManager ctrl.Manager, factory ClusterFactory[T, U, PT, PU]) *ConfigBuilder[T, U, PT, PU] {
	return &ConfigBuilder[T, U, PT, PU]{
		runtimeManager:         runtimeManager,
		factory:                factory,
		subscriber:             &UnimplementedLifecycleSubscriber[T, U, PT, PU]{},
		clusterResourceFactory: &EmptyResourceFactory[T, PT]{},
		nodeResourceFactory:    &EmptyResourceFactory[U, PU]{},
		fieldOwner:             defaultFieldOwner,
		finalizer:              defaultFinalizer,
		hashLabel:              defaultHashLabel,
		clusterLabel:           defaultClusterLabel,
		nodeLabel:              defaultNodeLabel,
		namespaceLabel:         defaultNamespaceLabel,
		ownerTypeLabel:         defaultOwnerTypeLabel,
		testing:                false,
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

func (c *ConfigBuilder[T, U, PT, PU]) WithOwnerTypeLabel(label string) *ConfigBuilder[T, U, PT, PU] {
	c.ownerTypeLabel = label
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

func (c *ConfigBuilder[T, U, PT, PU]) WithSubscriber(subscriber LifecycleSubscriber[T, U, PT, PU]) *ConfigBuilder[T, U, PT, PU] {
	c.subscriber = subscriber
	return c
}

func (c *ConfigBuilder[T, U, PT, PU]) WithClusterResourceFactory(factory ResourceFactory[T, PT]) *ConfigBuilder[T, U, PT, PU] {
	c.clusterResourceFactory = factory
	return c
}

func (c *ConfigBuilder[T, U, PT, PU]) WithNodeResourceFactory(factory ResourceFactory[U, PU]) *ConfigBuilder[T, U, PT, PU] {
	c.nodeResourceFactory = factory
	return c
}

func (c *ConfigBuilder[T, U, PT, PU]) Testing() *ConfigBuilder[T, U, PT, PU] {
	c.testing = true
	return c
}

func (c *ConfigBuilder[T, U, PT, PU]) Setup(ctx context.Context) error {

	config := &Config[T, U, PT, PU]{
		Scheme:                 c.runtimeManager.GetScheme(),
		Client:                 c.runtimeManager.GetClient(),
		Factory:                c.factory,
		Subscriber:             c.subscriber,
		ClusterResourceFactory: c.clusterResourceFactory,
		NodeResourceFactory:    c.nodeResourceFactory,
		FieldOwner:             c.fieldOwner,
		Finalizer:              c.finalizer,
		Labels: ClusterLabels{
			HashLabel:      c.hashLabel,
			ClusterLabel:   c.clusterLabel,
			NodeLabel:      c.nodeLabel,
			NamespaceLabel: c.namespaceLabel,
			OwnerTypeLabel: c.ownerTypeLabel,
		},
		Testing: c.testing,
	}

	if err := setupClusterReconciler(c.runtimeManager, config); err != nil {
		return fmt.Errorf("setting up cluster reconciler: %w", err)
	}

	if err := setupClusterNodeReconciler(c.runtimeManager, config); err != nil {
		return fmt.Errorf("setting up cluster node reconciler: %w", err)
	}

	return nil
}
