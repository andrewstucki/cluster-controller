package controller

import (
	"context"
	"errors"

	k8sapierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type ResourceManager[T any, PT ptrToObject[T]] struct {
	factory         ResourceFactory[T, PT]
	client          *ResourceClient[T, PT]
	matchesType     func(map[string]string) bool
	ownerFromLabels func(map[string]string) types.NamespacedName
}

func (r *ResourceManager[T, PT]) listOwnedResources(ctx context.Context, owner PT) ([]client.Object, error) {
	if r.factory == nil {
		return nil, nil
	}

	resources := []client.Object{}
	for _, resourceType := range append(r.factory.ClusterScopedResourceTypes(), r.factory.NamespaceScopedResourceTypes()...) {
		matching, err := r.client.ListOwnedResources(ctx, owner, resourceType)
		if err != nil {
			return nil, err
		}
		resources = append(resources, matching...)
	}
	return resources, nil
}

func (r *ResourceManager[T, PT]) SyncAll(ctx context.Context, owner PT) error {
	if r.factory == nil {
		return nil
	}

	resources, err := r.listOwnedResources(ctx, owner)
	if err != nil {
		return err
	}
	toDelete := map[types.NamespacedName]client.Object{}
	for _, resource := range resources {
		toDelete[client.ObjectKeyFromObject(resource)] = resource
	}

	// attempt to create as many resources in one pass as we can
	errs := []error{}

	for _, resource := range append(r.factory.ClusterScopedResources(owner), r.factory.NamespaceScopedResources(owner)...) {
		if err := r.client.PatchOwnedResource(ctx, owner, resource); err != nil {
			errs = append(errs, err)
		}
		delete(toDelete, client.ObjectKeyFromObject(resource))
	}

	for _, resource := range toDelete {
		if err := r.client.Delete(ctx, resource); err != nil {
			if !k8sapierrors.IsNotFound(err) {
				errs = append(errs, err)
			}
		}
	}

	return errors.Join(errs...)
}

func (r *ResourceManager[T, PT]) DeleteAll(ctx context.Context, owner PT) (bool, error) {
	if r.factory == nil {
		return false, nil
	}

	resources, err := r.listOwnedResources(ctx, owner)
	if err != nil {
		return false, err
	}

	alive := []client.Object{}
	for _, o := range resources {
		if o.GetDeletionTimestamp() == nil {
			alive = append(alive, o)
		}
	}

	// attempt to delete as many resources in one pass as we can
	errs := []error{}
	for _, resource := range alive {
		if err := r.client.Delete(ctx, resource); err != nil {
			errs = append(errs, err)
		}
	}

	return len(alive) > 0, errors.Join(errs...)
}

func (r *ResourceManager[T, PT]) WatchResources(builder *builder.Builder) *builder.Builder {
	if r.factory == nil {
		return builder
	}

	for _, resourceType := range r.factory.NamespaceScopedResourceTypes() {
		builder = builder.Owns(resourceType)
	}

	for _, resourceType := range r.factory.ClusterScopedResourceTypes() {
		// since resources are cluster-scoped we need to call a Watch on them with some
		// custom mappings
		builder = builder.Watches(resourceType, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
			if labels := o.GetLabels(); labels != nil && r.matchesType(labels) {
				nn := r.ownerFromLabels(labels)
				if nn.Namespace != "" && nn.Name != "" {
					return []reconcile.Request{{
						NamespacedName: nn,
					}}
				}
			}
			return nil
		}))
	}

	return builder
}
