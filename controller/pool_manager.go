package controller

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

type PoolManager[T, U any, PT ptrToObject[T], PU ptrToObject[U]] interface {
	ListPools(ctx context.Context, cluster PT) ([]PU, error)
}

type concretePoolManager[T, U any, PT ptrToObject[T], PU ptrToObject[U]] struct {
	*ResourceClient[T, PT]

	poolOptions func(cluster PT) []client.ListOption
}

func (p *concretePoolManager[T, U, PT, PU]) ListPools(ctx context.Context, cluster PT) ([]PU, error) {
	objects, err := p.ListResources(ctx, newKubeObject[U, PU](), p.poolOptions(cluster)...)
	if err != nil {
		return nil, err
	}

	return mapObjectsTo[PU](objects), nil
}

type virtualPoolManager[T any, PT ptrToObject[T]] struct{}

func (p *virtualPoolManager[T, PT]) ListPools(ctx context.Context, cluster PT) ([]*virtualPool[T, PT], error) {
	return []*virtualPool[T, PT]{{cluster: cluster}}, nil
}
