package controller

import (
	"context"
	"errors"

	k8sapierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const ownerIndex = ".metadata.controller"

type ptrToObject[T any] interface {
	client.Object
	*T
}

func newKubeObject[T any, PT ptrToObject[T]]() PT {
	var t T
	return PT(&t)
}

func indexOwner[T any, PT ptrToObject[T]](ctx context.Context, mgr ctrl.Manager, ownerKind *schema.GroupVersionKind, index string) error {
	return mgr.GetFieldIndexer().IndexField(ctx, newKubeObject[T, PT](), index, func(o client.Object) []string {
		if ownerName, ok := isOwnedBy(ownerKind, o); ok {
			return []string{types.NamespacedName{Namespace: o.GetNamespace(), Name: ownerName}.String()}
		}

		return nil
	})
}

func isOwnedBy(ownerKind *schema.GroupVersionKind, obj client.Object) (string, bool) {
	owner := metav1.GetControllerOf(obj)
	if owner == nil {
		return "", false
	}

	if owner.Kind != ownerKind.Kind {
		return "", false
	}

	if owner.APIVersion != ownerKind.GroupVersion().String() {
		return "", false
	}

	return owner.Name, true
}

func ignoreConflict(err error) (ctrl.Result, error) {
	if k8sapierrors.IsConflict(err) {
		return ctrl.Result{Requeue: true}, nil
	}
	return ctrl.Result{}, err
}

func getRegisteredSchemaKind(scheme *runtime.Scheme, o client.Object) (*schema.GroupVersionKind, error) {
	groupKinds, _, err := scheme.ObjectKinds(o)
	if err != nil {
		return nil, err
	}
	if len(groupKinds) == 0 {
		return nil, errors.New("unable to get object kind")
	}
	return &groupKinds[0], nil
}
