# cluster-controller

This is a generic controller implementation that acts as a replacement for
StatefulSets for "Clusters" that require more specific sub-resource and lifecycle
management.

There are two main entrypoints to the controller with two slightly different
modeling paradigms, but the same underlying code driving reconciliation:

1. A Cluster --> Node paradigm where a user directly specifies replica and
node templating information at the cluster level.
2. A Cluster --> Pool --> Node paradigm where a cluster derives the replica and
templating information for a node based on an intermediary CRD defining a "Pool".

## Usage

The main implementation entrypoint to use this controller are
`controller.ClusterFactory[ClusterType, NodeType]` and
`controller.ClusterFactory[ClusterType, PoolType, NodeType]`. In addition, if
someone wants to define the interfaces for managing Cluster/Pool/Node information
directly on the CRD structures themselves, convenience implementations are given
in the form of `controller.DelegatingClusterFactory[ClusterType, NodeType]` and
`controller.DelegatingPooledClusterFactory[ClusterType, PoolType, NodeType]`.

Once these interfaces are defined (see `./controller/cluster_factory.go` for
details), just pass them in to the builder functions `controller.New` and
`controller.Pooled` respectively.

Additional sub-resources per-object can be created by passing in a
`controller.ResourceFactory` for the type. This allows for creating cluster and
namespace-scoped objects that are tied to a given Cluster/Pool/Node. The objects
are cleaned up when the owning resource is deleted, and in the case of 
cluster-scoped resources that cannot have a valid owner reference tied to them,
a garbage collector is run in the background to ensure any stray resources are
pruned from the cluster. As with other factory methods, you can define some of
this behavior directly on the CRD itself with
`controller.DelegatingResourceFactory` as seen in the example folder.

Finally, lifecycle management of individual cluster nodes is handled via
implementing a `controller.LifecycleSubscriber` if you only want to handle a
subset of lifecycle events, embed `controller.UnimplementedLifecycleSubscriber`
into your implementing structure.