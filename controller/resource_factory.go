package controller

import "sigs.k8s.io/controller-runtime/pkg/client"

type ResourceFactory[T any, PT ptrToObject[T]] interface {
	ClusterScopedResourceTypes() []client.Object
	NamespaceScopedResourceTypes() []client.Object
	ClusterScopedResources(owner PT) []client.Object
	NamespaceScopedResources(owner PT) []client.Object
}

type EmptyResourceFactory[T any, PT ptrToObject[T]] struct{}

func (f *EmptyResourceFactory[T, PT]) ClusterScopedResourceTypes() []client.Object {
	return nil
}

func (f *EmptyResourceFactory[T, PT]) NamespaceScopedResourceTypes() []client.Object {
	return nil
}

func (f *EmptyResourceFactory[T, PT]) ClusterScopedResources(owner PT) []client.Object {
	return nil
}

func (f *EmptyResourceFactory[T, PT]) NamespaceScopedResources(owner PT) []client.Object {
	return nil
}
