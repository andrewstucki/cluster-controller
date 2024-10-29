package main

import (
	"context"

	"github.com/andrewstucki/cluster-controller/controller"
	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Subscriber struct {
	logger logr.Logger
}

var _ controller.LifecycleSubscriber[Cluster, Broker, *Cluster, *Broker] = (*Subscriber)(nil)

func (s *Subscriber) AfterClusterCreated(ctx context.Context, cluster *Cluster) error {
	clusterID := client.ObjectKeyFromObject(cluster).String()

	s.logger.Info("after cluster created", "cluster", clusterID)
	return nil
}

func (s *Subscriber) AfterNodeCreated(ctx context.Context, cluster *Cluster, node *Broker) error {
	clusterID := client.ObjectKeyFromObject(cluster).String()
	nodeID := client.ObjectKeyFromObject(node).String()

	s.logger.Info("after node created", "cluster", clusterID, "node", nodeID)
	return nil
}

func (s *Subscriber) BeforeNodeUpdated(ctx context.Context, cluster *Cluster, node *Broker) error {
	clusterID := client.ObjectKeyFromObject(cluster).String()
	nodeID := client.ObjectKeyFromObject(node).String()

	s.logger.Info("before node updated", "cluster", clusterID, "node", nodeID)
	return nil
}

func (s *Subscriber) AfterNodeDeployed(ctx context.Context, cluster *Cluster, node *Broker) error {
	clusterID := client.ObjectKeyFromObject(cluster).String()
	nodeID := client.ObjectKeyFromObject(node).String()

	s.logger.Info("after node deployed", "cluster", clusterID, "node", nodeID)
	return nil
}

func (s *Subscriber) BeforeNodeDecommissioned(ctx context.Context, cluster *Cluster, node *Broker) error {
	clusterID := client.ObjectKeyFromObject(cluster).String()
	nodeID := client.ObjectKeyFromObject(node).String()

	s.logger.Info("before node decommissioned", "cluster", clusterID, "node", nodeID)
	return nil
}

func (s *Subscriber) AfterNodeDeleted(ctx context.Context, cluster *Cluster, node *Broker) error {
	clusterID := client.ObjectKeyFromObject(cluster).String()
	nodeID := client.ObjectKeyFromObject(node).String()

	s.logger.Info("after node deleted", "cluster", clusterID, "node", nodeID)
	return nil
}

func (s *Subscriber) BeforeClusterDeleted(ctx context.Context, cluster *Cluster) error {
	clusterID := client.ObjectKeyFromObject(cluster).String()

	s.logger.Info("before cluster deleted", "cluster", clusterID)
	return nil
}

func (s *Subscriber) AfterClusterDeleted(ctx context.Context, cluster *Cluster) error {
	clusterID := client.ObjectKeyFromObject(cluster).String()

	s.logger.Info("after cluster deleted", "cluster", clusterID)
	return nil
}
