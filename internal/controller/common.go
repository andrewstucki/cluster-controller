package controller

import (
	"context"

	clusterv1alpha1 "github.com/andrewstucki/cluster-controller/api/v1alpha1"
	k8sapierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

func indexOwner[T any, PT ptrToObject[T]](ctx context.Context, mgr ctrl.Manager, index string) error {
	return mgr.GetFieldIndexer().IndexField(ctx, newKubeObject[T, PT](), index, func(o client.Object) []string {
		if ownerName, ok := isOwnedByCluster(o); ok {
			return []string{ownerName}
		}

		return nil
	})
}

func isOwnedByCluster(obj client.Object) (string, bool) {
	owner := metav1.GetControllerOf(obj)
	if owner == nil {
		return "", false
	}

	if owner.Kind != clusterv1alpha1.ClusterKind {
		return "", false
	}

	if owner.APIVersion != clusterv1alpha1.GroupVersion.String() {
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
