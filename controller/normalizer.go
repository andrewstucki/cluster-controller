package controller

import (
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Normalizer[T any, PT ptrToObject[T]] struct {
	labels func(owner PT) map[string]string
	scheme *runtime.Scheme
	mapper meta.RESTMapper
}

func (n *Normalizer[T, PT]) Normalize(object client.Object, owner PT, extraLabels ...map[string]string) error {
	kind, err := getGroupVersionKind(n.scheme, object)
	if err != nil {
		return err
	}
	mapping, err := getResourceScope(n.mapper, n.scheme, object)
	if err != nil {
		return err
	}

	object.GetObjectKind().SetGroupVersionKind(*kind)

	labels := object.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}

	for name, value := range n.labels(owner) {
		labels[name] = value
	}
	for _, extra := range extraLabels {
		for name, value := range extra {
			labels[name] = value
		}
	}

	object.SetLabels(labels)

	if mapping.Name() == meta.RESTScopeNamespace.Name() {
		object.SetOwnerReferences([]metav1.OwnerReference{*metav1.NewControllerRef(owner, owner.GetObjectKind().GroupVersionKind())})
	}

	return nil
}

func getResourceScope(mapper meta.RESTMapper, scheme *runtime.Scheme, object client.Object) (meta.RESTScope, error) {
	gvk, err := getGroupVersionKind(scheme, object)
	if err != nil {
		return nil, err
	}

	mapping, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return nil, fmt.Errorf("unable to get REST mapping: %w", err)
	}

	return mapping.Scope, nil
}
