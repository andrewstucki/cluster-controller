package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	defaultFieldOwner               = "cluster-controller"
	defaultHashLabel                = "cluster-hash"
	defaultClusterLabel             = "cluster-name"
	defaultPoolLabel                = "cluster-pool-name"
	defaultOwnerTypeLabel           = "cluster-owner-type"
	defaultNodeLabel                = "cluster-node-name"
	defaultNamespaceLabel           = "cluster-namespace"
	defaultFinalizer                = "cluster-finalizer"
	defaultGarbageCollectionTimeout = 5 * time.Minute
)

type ClusterLabels struct {
	HashLabel      string
	NamespaceLabel string
	OwnerTypeLabel string
	NodeLabel      string
	ClusterLabel   string
	PoolLabel      string
}

type Config[T, U, V any, PT ptrToObject[T], PU ptrToObject[U], PV ptrToObject[V]] struct {
	Logger                   logr.Logger
	GarbageCollectionTimeout time.Duration
	Scheme                   *runtime.Scheme
	Client                   client.Client
	Subscriber               LifecycleSubscriber[T, V, PT, PV]
	Factory                  PooledClusterFactory[T, U, V, PT, PU, PV]
	Labels                   ClusterLabels
	ClusterResourceFactory   ResourceFactory[T, PT]
	PoolResourceFactory      ResourceFactory[U, PU]
	NodeResourceFactory      ResourceFactory[V, PV]
	FieldOwner               client.FieldOwner
	Finalizer                string
	Testing                  bool
	Pooled                   bool
	PoolManager              PoolManager[T, U, PT, PU]
}

func (c *Config[T, U, V, PT, PU, PV]) clusterNormalizer() *Normalizer[T, PT] {
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

func (c *Config[T, U, V, PT, PU, PV]) poolRelatedLabels(pool PU) map[string]string {
	return map[string]string{
		c.Labels.ClusterLabel:   c.Factory.GetClusterForPool(pool).Name,
		c.Labels.PoolLabel:      pool.GetName(),
		c.Labels.NamespaceLabel: pool.GetNamespace(),
	}
}

func (c *Config[T, U, V, PT, PU, PV]) poolNormalizer() *Normalizer[U, PU] {
	return &Normalizer[U, PU]{
		labels: func(owner PU) map[string]string {
			return map[string]string{
				c.Labels.ClusterLabel:   owner.GetLabels()[c.Labels.ClusterLabel],
				c.Labels.PoolLabel:      owner.GetName(),
				c.Labels.NamespaceLabel: owner.GetNamespace(),
				c.Labels.OwnerTypeLabel: "pool",
			}
		},
		scheme: c.Scheme,
		mapper: c.Client.RESTMapper(),
	}
}

func (c *Config[T, U, V, PT, PU, PV]) nodeNormalizer() *Normalizer[V, PV] {
	return &Normalizer[V, PV]{
		labels: func(owner PV) map[string]string {
			return map[string]string{
				c.Labels.ClusterLabel:   owner.GetLabels()[c.Labels.ClusterLabel],
				c.Labels.PoolLabel:      owner.GetLabels()[c.Labels.PoolLabel],
				c.Labels.NodeLabel:      owner.GetName(),
				c.Labels.NamespaceLabel: owner.GetNamespace(),
				c.Labels.OwnerTypeLabel: "node",
			}
		},
		scheme: c.Scheme,
		mapper: c.Client.RESTMapper(),
	}
}

func (c *Config[T, U, V, PT, PU, PV]) clusterResourceClient() *ResourceClient[T, PT] {
	return &ResourceClient[T, PT]{
		normalizer: c.clusterNormalizer(),
		client:     c.Client,
		fieldOwner: c.FieldOwner,
	}
}

func (c *Config[T, U, V, PT, PU, PV]) poolResourceClient() *ResourceClient[U, PU] {
	return &ResourceClient[U, PU]{
		normalizer: c.poolNormalizer(),
		client:     c.Client,
		fieldOwner: c.FieldOwner,
	}
}

func (c *Config[T, U, V, PT, PU, PV]) nodeResourceClient() *ResourceClient[V, PV] {
	return &ResourceClient[V, PV]{
		normalizer: c.nodeNormalizer(),
		client:     c.Client,
		fieldOwner: c.FieldOwner,
	}
}

func (c *Config[T, U, V, PT, PU, PV]) clusterNodeManager() *NodeManager[T, U, V, PT, PU, PV] {
	return &NodeManager[T, U, V, PT, PU, PV]{
		ResourceClient: c.clusterResourceClient(),
		normalizer:     c.clusterNormalizer(),
		hashLabel:      c.Labels.HashLabel,
		poolLabel:      c.Labels.PoolLabel,
		getNode:        c.Factory.GetClusterPoolNode,
		hasher:         c.Factory.HashClusterPool,
		subscriber:     c.Subscriber,
	}
}

func (c *Config[T, U, V, PT, PU, PV]) clusterResourceManager() *ResourceManager[T, PT] {
	return &ResourceManager[T, PT]{
		factory: c.ClusterResourceFactory,
		client:  c.clusterResourceClient(),
		matchesType: func(m map[string]string) bool {
			return m != nil && m[c.Labels.OwnerTypeLabel] == "cluster"
		},
		ownerFromLabels: c.clusterOwnerFromLabels,
	}
}

func (c *Config[T, U, V, PT, PU, PV]) clusterOwnerFromLabels(m map[string]string) types.NamespacedName {
	if m == nil {
		return types.NamespacedName{}
	}
	return types.NamespacedName{Namespace: m[c.Labels.NamespaceLabel], Name: m[c.Labels.ClusterLabel]}
}

func (c *Config[T, U, V, PT, PU, PV]) poolResourceManager() *ResourceManager[U, PU] {
	return &ResourceManager[U, PU]{
		factory: c.PoolResourceFactory,
		client:  c.poolResourceClient(),
		matchesType: func(m map[string]string) bool {
			return m != nil && m[c.Labels.OwnerTypeLabel] == "pool"
		},
		ownerFromLabels: c.poolOwnerFromLabels,
	}
}

func (c *Config[T, U, V, PT, PU, PV]) poolOwnerFromLabels(m map[string]string) types.NamespacedName {
	if m == nil {
		return types.NamespacedName{}
	}
	return types.NamespacedName{Namespace: m[c.Labels.NamespaceLabel], Name: m[c.Labels.PoolLabel]}
}

func (c *Config[T, U, V, PT, PU, PV]) nodeResourceManager() *ResourceManager[V, PV] {
	return &ResourceManager[V, PV]{
		factory: c.NodeResourceFactory,
		client:  c.nodeResourceClient(),
		matchesType: func(m map[string]string) bool {
			return m != nil && m[c.Labels.OwnerTypeLabel] == "node"
		},
		ownerFromLabels: c.nodeOwnerFromLabels,
	}
}

func (c *Config[T, U, V, PT, PU, PV]) nodeOwnerFromLabels(m map[string]string) types.NamespacedName {
	if m == nil {
		return types.NamespacedName{}
	}
	return types.NamespacedName{Namespace: m[c.Labels.NamespaceLabel], Name: m[c.Labels.NodeLabel]}
}

func (c *Config[T, U, V, PT, PU, PV]) podManager() *PodManager[V, PV] {
	return &PodManager[V, PV]{
		ResourceClient: c.nodeResourceClient(),
		hashFn:         c.clusterNodeManager().GetHash,
		hashLabel:      c.Labels.HashLabel,
		podFn:          c.Factory.GetClusterNodePod,
		testing:        c.Testing,
	}
}

type ConfigBuilder[T, U, V any, PT ptrToObject[T], PU ptrToObject[U], PV ptrToObject[V]] struct {
	runtimeManager ctrl.Manager
	factory        PooledClusterFactory[T, U, V, PT, PU, PV]

	garbageCollectionTimeout time.Duration

	subscriber             LifecycleSubscriber[T, V, PT, PV]
	clusterResourceFactory ResourceFactory[T, PT]
	poolResourceFactory    ResourceFactory[U, PU]
	nodeResourceFactory    ResourceFactory[V, PV]

	fieldOwner client.FieldOwner
	finalizer  string

	hashLabel      string
	clusterLabel   string
	nodeLabel      string
	poolLabel      string
	namespaceLabel string
	ownerTypeLabel string

	poolManagerFn func(c *Config[T, U, V, PT, PU, PV]) PoolManager[T, U, PT, PU]
	pooled        bool
	testing       bool
}

func New[T, V any, PT ptrToObject[T], PV ptrToObject[V]](runtimeManager ctrl.Manager, factory ClusterFactory[T, V, PT, PV]) *ConfigBuilder[T, virtualPool[T, PT], V, PT, *virtualPool[T, PT], PV] {
	return &ConfigBuilder[T, virtualPool[T, PT], V, PT, *virtualPool[T, PT], PV]{
		runtimeManager:           runtimeManager,
		factory:                  wrapClusterFactory(factory),
		subscriber:               &UnimplementedLifecycleSubscriber[T, V, PT, PV]{},
		clusterResourceFactory:   &EmptyResourceFactory[T, PT]{},
		poolResourceFactory:      &EmptyResourceFactory[virtualPool[T, PT], *virtualPool[T, PT]]{},
		nodeResourceFactory:      &EmptyResourceFactory[V, PV]{},
		garbageCollectionTimeout: defaultGarbageCollectionTimeout,
		fieldOwner:               defaultFieldOwner,
		finalizer:                defaultFinalizer,
		hashLabel:                defaultHashLabel,
		clusterLabel:             defaultClusterLabel,
		poolLabel:                defaultPoolLabel,
		nodeLabel:                defaultNodeLabel,
		namespaceLabel:           defaultNamespaceLabel,
		ownerTypeLabel:           defaultOwnerTypeLabel,
		testing:                  false,
		pooled:                   false,
		poolManagerFn: func(_ *Config[T, virtualPool[T, PT], V, PT, *virtualPool[T, PT], PV]) PoolManager[T, virtualPool[T, PT], PT, *virtualPool[T, PT]] {
			return &virtualPoolManager[T, PT]{}
		},
	}
}

func Pooled[T, U, V any, PT ptrToObject[T], PU ptrToObject[U], PV ptrToObject[V]](runtimeManager ctrl.Manager, factory PooledClusterFactory[T, U, V, PT, PU, PV]) *ConfigBuilder[T, U, V, PT, PU, PV] {
	return &ConfigBuilder[T, U, V, PT, PU, PV]{
		runtimeManager:           runtimeManager,
		factory:                  factory,
		subscriber:               &UnimplementedLifecycleSubscriber[T, V, PT, PV]{},
		clusterResourceFactory:   &EmptyResourceFactory[T, PT]{},
		poolResourceFactory:      &EmptyResourceFactory[U, PU]{},
		nodeResourceFactory:      &EmptyResourceFactory[V, PV]{},
		garbageCollectionTimeout: defaultGarbageCollectionTimeout,
		fieldOwner:               defaultFieldOwner,
		finalizer:                defaultFinalizer,
		hashLabel:                defaultHashLabel,
		clusterLabel:             defaultClusterLabel,
		poolLabel:                defaultPoolLabel,
		nodeLabel:                defaultNodeLabel,
		namespaceLabel:           defaultNamespaceLabel,
		ownerTypeLabel:           defaultOwnerTypeLabel,
		testing:                  false,
		pooled:                   true,
		poolManagerFn: func(c *Config[T, U, V, PT, PU, PV]) PoolManager[T, U, PT, PU] {
			return &concretePoolManager[T, U, PT, PU]{
				ResourceClient: c.clusterResourceClient(),
			}
		},
	}
}

func (c *ConfigBuilder[T, U, V, PT, PU, PV]) WithFieldOwner(owner client.FieldOwner) *ConfigBuilder[T, U, V, PT, PU, PV] {
	c.fieldOwner = owner
	return c
}

func (c *ConfigBuilder[T, U, V, PT, PU, PV]) WithFinalizer(finalizer string) *ConfigBuilder[T, U, V, PT, PU, PV] {
	c.finalizer = finalizer
	return c
}

func (c *ConfigBuilder[T, U, V, PT, PU, PV]) WithHashLabel(label string) *ConfigBuilder[T, U, V, PT, PU, PV] {
	c.hashLabel = label
	return c
}

func (c *ConfigBuilder[T, U, V, PT, PU, PV]) WithClusterLabel(label string) *ConfigBuilder[T, U, V, PT, PU, PV] {
	c.clusterLabel = label
	return c
}

func (c *ConfigBuilder[T, U, V, PT, PU, PV]) WithOwnerTypeLabel(label string) *ConfigBuilder[T, U, V, PT, PU, PV] {
	c.ownerTypeLabel = label
	return c
}

func (c *ConfigBuilder[T, U, V, PT, PU, PV]) WithNodeLabel(label string) *ConfigBuilder[T, U, V, PT, PU, PV] {
	c.nodeLabel = label
	return c
}

func (c *ConfigBuilder[T, U, V, PT, PU, PV]) WithNamespaceLabel(label string) *ConfigBuilder[T, U, V, PT, PU, PV] {
	c.namespaceLabel = label
	return c
}

func (c *ConfigBuilder[T, U, V, PT, PU, PV]) WithSubscriber(subscriber LifecycleSubscriber[T, V, PT, PV]) *ConfigBuilder[T, U, V, PT, PU, PV] {
	c.subscriber = subscriber
	return c
}

func (c *ConfigBuilder[T, U, V, PT, PU, PV]) WithClusterResourceFactory(factory ResourceFactory[T, PT]) *ConfigBuilder[T, U, V, PT, PU, PV] {
	c.clusterResourceFactory = factory
	return c
}

func (c *ConfigBuilder[T, U, V, PT, PU, PV]) WithPoolResourceFactory(factory ResourceFactory[U, PU]) *ConfigBuilder[T, U, V, PT, PU, PV] {
	c.poolResourceFactory = factory
	return c
}

func (c *ConfigBuilder[T, U, V, PT, PU, PV]) WithNodeResourceFactory(factory ResourceFactory[V, PV]) *ConfigBuilder[T, U, V, PT, PU, PV] {
	c.nodeResourceFactory = factory
	return c
}

func (c *ConfigBuilder[T, U, V, PT, PU, PV]) WithGarbageCollectionTimeout(timeout time.Duration) *ConfigBuilder[T, U, V, PT, PU, PV] {
	c.garbageCollectionTimeout = timeout
	return c
}

func (c *ConfigBuilder[T, U, V, PT, PU, PV]) Testing() *ConfigBuilder[T, U, V, PT, PU, PV] {
	c.testing = true
	return c
}

func (c *ConfigBuilder[T, U, V, PT, PU, PV]) Setup(ctx context.Context) error {
	config := &Config[T, U, V, PT, PU, PV]{
		Logger:                   c.runtimeManager.GetLogger(),
		Scheme:                   c.runtimeManager.GetScheme(),
		Client:                   c.runtimeManager.GetClient(),
		GarbageCollectionTimeout: c.garbageCollectionTimeout,
		Factory:                  c.factory,
		Subscriber:               c.subscriber,
		ClusterResourceFactory:   c.clusterResourceFactory,
		PoolResourceFactory:      c.poolResourceFactory,
		NodeResourceFactory:      c.nodeResourceFactory,
		FieldOwner:               c.fieldOwner,
		Finalizer:                c.finalizer,
		Labels: ClusterLabels{
			HashLabel:      c.hashLabel,
			ClusterLabel:   c.clusterLabel,
			PoolLabel:      c.poolLabel,
			NodeLabel:      c.nodeLabel,
			NamespaceLabel: c.namespaceLabel,
			OwnerTypeLabel: c.ownerTypeLabel,
		},
		Testing: c.testing,
		Pooled:  c.pooled,
	}

	config.PoolManager = c.poolManagerFn(config)

	if err := setupClusterReconciler(c.runtimeManager, config); err != nil {
		return fmt.Errorf("setting up cluster reconciler: %w", err)
	}

	if c.pooled {
		if err := setupClusterPoolReconciler(ctx, c.runtimeManager, config); err != nil {
			return fmt.Errorf("setting up cluster pool reconciler: %w", err)
		}
	}

	if err := setupClusterNodeReconciler(c.runtimeManager, config); err != nil {
		return fmt.Errorf("setting up cluster node reconciler: %w", err)
	}

	if err := c.runtimeManager.Add(NewGarbageCollector(config)); err != nil {
		return fmt.Errorf("setting up garbage collector: %w", err)
	}

	return nil
}
