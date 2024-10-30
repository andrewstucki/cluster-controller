package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cenkalti/backoff"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type TestClusterFactory[T, V any, PT ClusterObject[T, V, PV], PV ClusterNodeObject[V]] struct {
	DelegatingClusterFactory[T, V, PT, PV]

	Client         client.Client
	ResourceClient *ResourceClient[T, PT]
}

func NewTestClusterFactory[T, V any, PT ClusterObject[T, V, PV], PV ClusterNodeObject[V]](cl client.Client) *TestClusterFactory[T, V, PT, PV] {
	return &TestClusterFactory[T, V, PT, PV]{
		Client: cl,
		ResourceClient: &ResourceClient[T, PT]{
			normalizer: &Normalizer[T, PT]{
				labels: func(owner PT) map[string]string {
					return map[string]string{
						defaultNamespaceLabel: owner.GetNamespace(),
						defaultClusterLabel:   owner.GetName(),
					}
				},
			},
			client: cl,
		},
	}
}

func (f *TestClusterFactory[T, V, PT, PV]) WaitForStableNodes(ctx context.Context, expected int, timeout time.Duration, cluster PT) ([]PV, error) {
	retryFor := backoff.WithMaxRetries(backoff.NewConstantBackOff(1*time.Second), uint64(timeout/time.Second))
	var nodes []PV
	err := backoff.Retry(func() error {
		objects, err := f.ResourceClient.ListOwnedResources(ctx, cluster, newKubeObject[V, PV]())
		if err != nil {
			return err
		}
		nodes = mapObjectsTo[PV](objects)

		if len(nodes) != expected {
			return fmt.Errorf("cluster node length doesn't match: %d != %d", len(nodes), expected)
		}

		for _, node := range nodes {
			if !node.GetStatus().Healthy {
				return errors.New("cluster nodes not yet healthy")
			}
		}

		return nil
	}, backoff.WithContext(retryFor, ctx))

	if err != nil {
		return nil, err
	}

	return nodes, nil
}
