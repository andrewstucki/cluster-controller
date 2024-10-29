package controller

import (
	"context"
	"fmt"
	"reflect"

	k8sapierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type ResourceClient[T any, PT ptrToObject[T]] struct {
	normalizer *Normalizer[T, PT]
	client     client.Client
	fieldOwner client.FieldOwner
}

func (c *ResourceClient[T, PT]) Delete(ctx context.Context, object client.Object) error {
	if err := c.client.Delete(ctx, object); err != nil {
		if !k8sapierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func (c *ResourceClient[T, PT]) CreateOwnedResource(ctx context.Context, owner PT, object client.Object, extraLabels ...map[string]string) error {
	if err := c.normalizer.Normalize(object, owner, extraLabels...); err != nil {
		return err
	}
	if err := c.client.Create(ctx, object); err != nil {
		if !k8sapierrors.IsAlreadyExists(err) {
			return err
		}
	}
	return nil
}

func (c *ResourceClient[T, PT]) PatchOwnedResource(ctx context.Context, owner PT, object client.Object, extraLabels ...map[string]string) error {
	if err := c.normalizer.Normalize(object, owner, extraLabels...); err != nil {
		return err
	}
	return c.client.Patch(ctx, object, client.Apply, c.fieldOwner, client.ForceOwnership)
}

func (c *ResourceClient[T, PT]) ListResources(ctx context.Context, object client.Object, opts ...client.ListOption) ([]client.Object, error) {
	kind, err := getGroupVersionKind(c.client.Scheme(), object)
	if err != nil {
		return nil, err
	}
	kind.Kind += "List"

	olist, err := c.client.Scheme().New(*kind)
	if err != nil {
		return nil, fmt.Errorf("initializing list: %w", err)
	}
	list, ok := olist.(client.ObjectList)
	if !ok {
		return nil, fmt.Errorf("invalid object list type: %T", object)
	}

	if err := c.client.List(ctx, list, opts...); err != nil {
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

func (c *ResourceClient[T, PT]) ListMatchingResources(ctx context.Context, object client.Object, labels map[string]string) ([]client.Object, error) {
	return c.ListResources(ctx, object, client.MatchingLabels(labels))
}

func (c *ResourceClient[T, PT]) ListOwnedResources(ctx context.Context, owner PT, object client.Object) ([]client.Object, error) {
	return c.ListMatchingResources(ctx, object, c.normalizer.labels(owner))
}
