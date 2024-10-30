package controller

import (
	"context"
	"fmt"
	"reflect"

	clusterv1alpha1 "github.com/andrewstucki/cluster-controller/controller/api/v1alpha1"
)

type NodeManager[T, U, V any, PT ptrToObject[T], PU ptrToObject[U], PV ptrToObject[V]] struct {
	*ResourceClient[T, PT]

	normalizer *Normalizer[T, PT]
	getNode    func(pool PU) PV
	hashLabel  string
	poolLabel  string
	hasher     func(pool PU) (string, error)
	subscriber LifecycleSubscriber[T, V, PT, PV]
}

func (n *NodeManager[T, U, V, PT, PU, PV]) ListNodes(ctx context.Context, owner PT) ([]PV, error) {
	objects, err := n.ListOwnedResources(ctx, owner, newKubeObject[V, PV]())
	if err != nil {
		return nil, err
	}

	return mapObjectsTo[PV](objects), nil
}

func (n *NodeManager[T, U, V, PT, PU, PV]) Hash(pool PU) (string, error) {
	return n.hasher(pool)
}

func (n *NodeManager[T, U, V, PT, PU, PV]) GetHash(node PV) string {
	if labels := node.GetLabels(); labels != nil {
		return labels[n.hashLabel]
	}
	return ""
}

func (n *NodeManager[T, U, V, PT, PU, PV]) VersionMatches(hash string, node PV) bool {
	if labels := node.GetLabels(); labels != nil {
		return labels[n.hashLabel] == hash
	}
	return false
}

func (n *NodeManager[T, U, V, PT, PU, PV]) CreateNode(ctx context.Context, status *clusterv1alpha1.ClusterStatus, owner PT, pool PU, hash string) (PV, error) {
	node := n.getNode(pool)
	node.SetName(generateNextNode(owner.GetName(), status))
	node.SetNamespace(owner.GetNamespace())

	if err := n.CreateOwnedResource(ctx, owner, node, map[string]string{n.poolLabel: pool.GetName(), n.hashLabel: hash}); err != nil {
		return node, err
	}

	if err := n.subscriber.AfterNodeCreated(ctx, owner, node); err != nil {
		return node, fmt.Errorf("running node after create hook: %w", err)
	}

	return node, nil
}

func (n *NodeManager[T, U, V, PT, PU, PV]) UpdateNode(ctx context.Context, owner PT, hash string, pool PU, node PV) error {
	newNode := n.getNode(pool)

	// set the spec from the updated node if the node has a Spec field
	specValue := reflect.ValueOf(node).Elem().FieldByName("Spec")
	if specValue.IsValid() && specValue.CanSet() {
		specValue.Set(reflect.ValueOf(newNode).Elem().FieldByName("Spec"))
	}

	n.normalizer.Normalize(node, owner, map[string]string{n.poolLabel: pool.GetName(), n.hashLabel: hash})

	if err := n.subscriber.BeforeNodeUpdated(ctx, owner, node); err != nil {
		return fmt.Errorf("running node before update hook: %w", err)
	}

	return n.client.Update(ctx, node)
}
