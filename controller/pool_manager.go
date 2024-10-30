package controller

import (
	"context"

	"k8s.io/apimachinery/pkg/fields"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type PoolManager[T, U any, PT ptrToObject[T], PU ptrToObject[U]] interface {
	ListPools(ctx context.Context, cluster PT) ([]PU, error)
}

type concretePoolManager[T, U any, PT ptrToObject[T], PU ptrToObject[U]] struct {
	*ResourceClient[T, PT]
}

func (p *concretePoolManager[T, U, PT, PU]) ListPools(ctx context.Context, cluster PT) ([]PU, error) {
	objects, err := p.ListResources(ctx, newKubeObject[U, PU](), client.MatchingFieldsSelector{Selector: fields.OneTermEqualSelector(poolClusterIndex, client.ObjectKeyFromObject(cluster).String())})
	if err != nil {
		return nil, err
	}

	return mapObjectsTo[PU](objects), nil
}

type virtualPoolManager[T any, PT ptrToObject[T]] struct{}

func (p *virtualPoolManager[T, PT]) ListPools(ctx context.Context, cluster PT) ([]*virtualPool[T, PT], error) {
	return []*virtualPool[T, PT]{{cluster: cluster}}, nil
}
