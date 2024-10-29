package controller

import (
	"context"
	"fmt"
	"reflect"

	clusterv1alpha1 "github.com/andrewstucki/cluster-controller/controller/api/v1alpha1"
)

type NodeManager[T, U any, PT ptrToObject[T], PU ptrToObject[U]] struct {
	*ResourceClient[T, PT]

	normalizer *Normalizer[T, PT]
	getNode    func(cluster PT) PU
	hashLabel  string
	hasher     func(cluster PT) (string, error)
	subscriber LifecycleSubscriber[T, U, PT, PU]
}

func (n *NodeManager[T, U, PT, PU]) ListNodes(ctx context.Context, owner PT) ([]PU, error) {
	objects, err := n.ListOwnedResources(ctx, owner, newKubeObject[U, PU]())
	if err != nil {
		return nil, err
	}

	return mapObjectsTo[PU](objects), nil
}

func (n *NodeManager[T, U, PT, PU]) Hash(owner PT) (string, error) {
	return n.hasher(owner)
}

func (n *NodeManager[T, U, PT, PU]) GetHash(node PU) string {
	if labels := node.GetLabels(); labels != nil {
		return labels[n.hashLabel]
	}
	return ""
}

func (n *NodeManager[T, U, PT, PU]) VersionMatches(hash string, node PU) bool {
	if labels := node.GetLabels(); labels != nil {
		return labels[n.hashLabel] == hash
	}
	return false
}

func (n *NodeManager[T, U, PT, PU]) CreateNode(ctx context.Context, status *clusterv1alpha1.ClusterStatus, owner PT, hash string) (PU, error) {
	node := n.getNode(owner)
	node.SetName(generateNextNode(owner.GetName(), status))
	node.SetNamespace(owner.GetNamespace())

	if err := n.CreateOwnedResource(ctx, owner, node, map[string]string{n.hashLabel: hash}); err != nil {
		return node, err
	}

	if err := n.subscriber.AfterNodeCreated(ctx, owner, node); err != nil {
		return node, fmt.Errorf("running node after create hook: %w", err)
	}

	return node, nil
}

func (n *NodeManager[T, U, PT, PU]) UpdateNode(ctx context.Context, owner PT, hash string, node PU) error {
	newNode := n.getNode(owner)

	// set the spec from the updated node if the node has a Spec field
	specValue := reflect.ValueOf(node).Elem().FieldByName("Spec")
	if specValue.IsValid() && specValue.CanSet() {
		specValue.Set(reflect.ValueOf(newNode).Elem().FieldByName("Spec"))
	}

	n.normalizer.Normalize(node, owner, map[string]string{n.hashLabel: hash})

	if err := n.subscriber.BeforeNodeUpdated(ctx, owner, node); err != nil {
		return fmt.Errorf("running node before update hook: %w", err)
	}

	return n.client.Update(ctx, node)
}
