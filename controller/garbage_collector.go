package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// GarbageCollector acts as an additional cluster-scoped garbage collection mechanism.
// This is mainly due to the fact that you can't set owner refs for a cluster-scoped
// resource to a namespace-scoped resource. As a result, we can't be certain that all of
// our resources were actually cleaned up when a node/cluster was deleted.
type GarbageCollector[T, U, V any, PT ptrToObject[T], PU ptrToObject[U], PV ptrToObject[V]] struct {
	logger  logr.Logger
	timeout time.Duration

	pooled                 bool
	ownerTypeLabel         string
	clusterOwnerFromLabels func(map[string]string) types.NamespacedName
	poolOwnerFromLabels    func(map[string]string) types.NamespacedName
	nodeOwnerFromLabels    func(map[string]string) types.NamespacedName
	clusterResources       []client.Object
	poolResources          []client.Object
	nodeResources          []client.Object
	resourceClient         *ResourceClient[T, PT]
}

func NewGarbageCollector[T, U, V any, PT ptrToObject[T], PU ptrToObject[U], PV ptrToObject[V]](config *Config[T, U, V, PT, PU, PV]) *GarbageCollector[T, U, V, PT, PU, PV] {
	return &GarbageCollector[T, U, V, PT, PU, PV]{
		timeout:                config.GarbageCollectionTimeout,
		logger:                 config.Logger,
		ownerTypeLabel:         config.Labels.OwnerTypeLabel,
		clusterOwnerFromLabels: config.clusterOwnerFromLabels,
		poolOwnerFromLabels:    config.poolOwnerFromLabels,
		nodeOwnerFromLabels:    config.nodeOwnerFromLabels,
		resourceClient:         config.clusterResourceClient(),
		clusterResources:       config.ClusterResourceFactory.ClusterScopedResourceTypes(),
		pooled:                 config.Pooled,
		poolResources:          config.PoolResourceFactory.ClusterScopedResourceTypes(),
		nodeResources:          config.NodeResourceFactory.ClusterScopedResourceTypes(),
	}
}

func (g *GarbageCollector[T, U, V, PT, PU, PV]) Start(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(g.timeout):
			if err := g.runOnce(ctx); err != nil {
				select {
				case <-ctx.Done():
					return nil
				default:
					g.logger.Error(err, "running garbage collector")
				}
			}
		}
	}
}

func (g *GarbageCollector[T, U, V, PT, PU, PV]) NeedLeaderElection() bool {
	return true
}

func (g *GarbageCollector[T, U, V, PT, PU, PV]) runOnce(ctx context.Context) error {
	started := time.Now()

	g.logger.V(4).Info("running garbage collection routine")
	defer func() {
		g.logger.V(4).Info("finished running garbage collection routine", "duration", time.Since(started))
	}()

	existingClusters, err := g.resourceClient.ListResources(ctx, newKubeObject[T, PT]())
	if err != nil {
		return err
	}

	existingNodes, err := g.resourceClient.ListResources(ctx, newKubeObject[V, PV]())
	if err != nil {
		return err
	}

	referencedClusters := map[types.NamespacedName]struct{}{}
	referencedNodes := map[types.NamespacedName]struct{}{}

	for _, cluster := range existingClusters {
		referencedClusters[client.ObjectKeyFromObject(cluster)] = struct{}{}
	}
	for _, node := range existingNodes {
		referencedNodes[client.ObjectKeyFromObject(node)] = struct{}{}
	}

	itemsToDelete := []client.Object{}

	errs := []error{}
	for _, resourceType := range g.clusterResources {
		objects, err := g.resourceClient.ListMatchingResources(ctx, resourceType, map[string]string{g.ownerTypeLabel: "cluster"})
		if err != nil {
			errs = append(errs, err)
			continue
		}
		for _, object := range objects {
			objectID := client.ObjectKeyFromObject(object)
			owner := g.clusterOwnerFromLabels(object.GetLabels())

			if owner.Namespace != "" && owner.Name != "" {
				g.logger.V(4).Info("found cluster owned resource", "owner", owner, "resourceType", fmt.Sprintf("%T", resourceType), "resource", objectID)
			}

			if _, found := referencedClusters[owner]; !found {
				g.logger.V(4).Info("owner cluster not found, deleting resource", "owner", owner, "resourceType", fmt.Sprintf("%T", resourceType), "resource", objectID)
				itemsToDelete = append(itemsToDelete, object)
			}
		}
	}

	for _, resourceType := range g.nodeResources {
		objects, err := g.resourceClient.ListMatchingResources(ctx, resourceType, map[string]string{g.ownerTypeLabel: "node"})
		if err != nil {
			errs = append(errs, err)
			continue
		}
		for _, object := range objects {
			objectID := client.ObjectKeyFromObject(object)
			owner := g.nodeOwnerFromLabels(object.GetLabels())

			if owner.Namespace != "" && owner.Name != "" {
				g.logger.V(4).Info("found node owned resource", "owner", owner, "resourceType", fmt.Sprintf("%T", resourceType), "resource", objectID)
			}

			if _, found := referencedNodes[owner]; !found {
				g.logger.V(4).Info("owner node not found, deleting resource", "owner", owner, "resourceType", fmt.Sprintf("%T", resourceType), "resource", objectID)
				itemsToDelete = append(itemsToDelete, object)
			}
		}
	}

	if g.pooled {
		existingPools, err := g.resourceClient.ListResources(ctx, newKubeObject[U, PU]())
		if err != nil {
			return err
		}

		referencedPools := map[types.NamespacedName]struct{}{}
		for _, pool := range existingPools {
			referencedPools[client.ObjectKeyFromObject(pool)] = struct{}{}
		}

		for _, resourceType := range g.poolResources {
			objects, err := g.resourceClient.ListMatchingResources(ctx, resourceType, map[string]string{g.ownerTypeLabel: "pool"})
			if err != nil {
				errs = append(errs, err)
				continue
			}
			for _, object := range objects {
				objectID := client.ObjectKeyFromObject(object)
				owner := g.poolOwnerFromLabels(object.GetLabels())

				if owner.Namespace != "" && owner.Name != "" {
					g.logger.V(4).Info("found pool owned resource", "owner", owner, "resourceType", fmt.Sprintf("%T", resourceType), "resource", objectID)
				}

				if _, found := referencedPools[owner]; !found {
					g.logger.V(4).Info("owner pool not found, deleting resource", "owner", owner, "resourceType", fmt.Sprintf("%T", resourceType), "resource", objectID)
					itemsToDelete = append(itemsToDelete, object)
				}
			}
		}
	}

	for _, item := range itemsToDelete {
		if err := g.resourceClient.Delete(ctx, item); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}
