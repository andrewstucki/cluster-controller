package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	k8sapierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type ResourceKind int

const (
	ResourceKindCluster ResourceKind = iota
	ResourceKindNode
)

type ResourceManager interface {
	ClusterScopedResourceTypes(kind ResourceKind) []client.Object
	NamespaceScopedResourceTypes(kind ResourceKind) []client.Object
	ClusterScopedResources(owner client.Object) []client.Object
	NamespaceScopedResources(owner client.Object) []client.Object
}

func getTypedResourcesMatching(ctx context.Context, cl client.Client, o client.Object, matchLabels map[string]string) ([]client.Object, error) {
	kinds, _, err := cl.Scheme().ObjectKinds(o)
	if err != nil {
		return nil, fmt.Errorf("fetching object kind: %w", err)
	}
	if len(kinds) == 0 {
		return nil, fmt.Errorf("unable to determine object kind")
	}
	kind := kinds[0]
	kind.Kind += "List"

	olist, err := cl.Scheme().New(kind)
	if err != nil {
		return nil, fmt.Errorf("initializing list: %w", err)
	}
	list, ok := olist.(client.ObjectList)
	if !ok {
		return nil, fmt.Errorf("invalid object list type: %T", o)
	}

	if err := cl.List(ctx, list, client.MatchingLabels(matchLabels)); err != nil {
		return nil, fmt.Errorf("listing resources: %w", err)
	}

	converted := []client.Object{}
	items := reflect.ValueOf(list).Elem().FieldByName("Items")
	if items.IsZero() {
		return nil, fmt.Errorf("unable to get items")
	}
	for i := 0; i < items.Len(); i++ {
		item := items.Index(i).Addr().Interface().(client.Object)
		converted = append(converted, item)
	}

	return sortCreation(converted), nil
}

func getAllResourcesMatching(ctx context.Context, cl client.Client, manager any, kind ResourceKind, matchLabels map[string]string) ([]client.Object, error) {
	resourceManager, ok := manager.(ResourceManager)
	if !ok {
		return nil, nil
	}

	resources := []client.Object{}
	for _, resourceType := range append(resourceManager.ClusterScopedResourceTypes(kind), resourceManager.NamespaceScopedResourceTypes(kind)...) {
		matching, err := getTypedResourcesMatching(ctx, cl, resourceType, matchLabels)
		if err != nil {
			return nil, err
		}
		resources = append(resources, matching...)
	}
	return resources, nil
}

func syncAllResources(ctx context.Context, cl client.Client, manager any, kind ResourceKind, fieldOwner client.FieldOwner, owner client.Object, matchLabels map[string]string) error {
	resourceManager, ok := manager.(ResourceManager)
	if !ok {
		return nil
	}

	resources, err := getAllResourcesMatching(ctx, cl, manager, kind, matchLabels)
	if err != nil {
		return err
	}
	toDelete := map[types.NamespacedName]client.Object{}
	for _, resource := range resources {
		toDelete[client.ObjectKeyFromObject(resource)] = resource
	}

	// attempt to create as many resources in one pass as we can
	errs := []error{}

	for _, resource := range resourceManager.ClusterScopedResources(owner) {
		fixTypes(cl, resource)
		addLabels(resource, matchLabels)
		if err := cl.Patch(ctx, resource, client.Apply, fieldOwner, client.ForceOwnership); err != nil {
			errs = append(errs, err)
		}
		delete(toDelete, client.ObjectKeyFromObject(resource))
	}

	for _, resource := range resourceManager.NamespaceScopedResources(owner) {
		fixTypes(cl, resource)
		addOwner(resource, owner)
		addLabels(resource, matchLabels)
		if err := cl.Patch(ctx, resource, client.Apply, fieldOwner, client.ForceOwnership); err != nil {
			errs = append(errs, err)
		}
		delete(toDelete, client.ObjectKeyFromObject(resource))
	}

	for _, resource := range toDelete {
		if err := cl.Delete(ctx, resource); err != nil {
			if !k8sapierrors.IsNotFound(err) {
				errs = append(errs, err)
			}
		}
	}

	return errors.Join(errs...)
}

func deleteAllResources(ctx context.Context, cl client.Client, manager any, kind ResourceKind, matchLabels map[string]string) (bool, error) {
	resources, err := getAllResourcesMatching(ctx, cl, manager, kind, matchLabels)
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
		if err := cl.Delete(ctx, resource); err != nil {
			if !k8sapierrors.IsNotFound(err) {
				errs = append(errs, err)
			}
		}
	}

	return len(alive) > 0, errors.Join(errs...)
}

func fixTypes(cl client.Client, resource client.Object) {
	kinds, _, err := cl.Scheme().ObjectKinds(resource)
	if err != nil || len(kinds) == 0 {
		// if we don't have a registered kind for this, just error out
		panic(err)
	}
	resource.GetObjectKind().SetGroupVersionKind(kinds[0])
}

func addOwner(resource, owner client.Object) {
	resource.SetOwnerReferences([]metav1.OwnerReference{*metav1.NewControllerRef(owner, owner.GetObjectKind().GroupVersionKind())})
}

func addLabels(resource client.Object, matchLabels map[string]string) {
	labels := resource.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	for name, label := range matchLabels {
		labels[name] = label
	}
	resource.SetLabels(labels)
}

func watchResources(builder *builder.Builder, manager any, kind ResourceKind, namespaceLabel, nameLabel string, matchingLabel map[string]string) *builder.Builder {
	resourceManager, ok := manager.(ResourceManager)
	if !ok {
		return builder
	}

	for _, resourceType := range resourceManager.NamespaceScopedResourceTypes(kind) {
		builder = builder.Owns(resourceType)
	}

	for _, resourceType := range resourceManager.ClusterScopedResourceTypes(kind) {
		// since resources are cluster-scoped we need to call a Watch on them with some
		// custom mappings
		builder = builder.Watches(resourceType, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
			if labels := o.GetLabels(); labels != nil {
				for name, value := range matchingLabel {
					matching, ok := labels[name]
					if !ok || matching != value {
						return nil
					}
				}
				namespace := labels[namespaceLabel]
				name := labels[nameLabel]
				if namespace != "" && name != "" {
					return []reconcile.Request{{
						NamespacedName: types.NamespacedName{Namespace: namespace, Name: name},
					}}
				}
			}
			return nil
		}))
	}

	return builder
}
