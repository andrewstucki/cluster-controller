package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cenkalti/backoff"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type TestClusterFactory[T, U any, PT ClusterObject[T, U, PU], PU ClusterNodeObject[U]] struct {
	DelegatingClusterFactory[T, U, PT, PU]

	Client         client.Client
	ResourceClient *ResourceClient[T, PT]
}

func NewTestClusterFactory[T, U any, PT ClusterObject[T, U, PU], PU ClusterNodeObject[U]](cl client.Client) *TestClusterFactory[T, U, PT, PU] {
	return &TestClusterFactory[T, U, PT, PU]{
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

func (f *TestClusterFactory[T, U, PT, PU]) WaitForStableNodes(ctx context.Context, timeout time.Duration, cluster PT) ([]PU, error) {
	retryFor := backoff.WithMaxRetries(backoff.NewConstantBackOff(1*time.Second), uint64(timeout/time.Second))
	var nodes []PU
	err := backoff.Retry(func() error {
		objects, err := f.ResourceClient.ListOwnedResources(ctx, cluster, newKubeObject[U, PU]())
		if err != nil {
			return err
		}
		nodes = mapObjectsTo[PU](objects)

		if len(nodes) != cluster.GetReplicas() {
			return fmt.Errorf("cluster node length doesn't match: %d != %d", len(nodes), cluster.GetReplicas())
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
