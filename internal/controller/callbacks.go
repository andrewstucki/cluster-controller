package controller

import (
	"context"
	"errors"
	"strings"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type ClusterObjects[T, U any, PT ptrToObject[T], PU ptrToObject[U]] struct {
	Cluster                PT
	Node                   PU
	Pod                    *corev1.Pod
	PersistentVolumes      []*corev1.PersistentVolume
	PersistentVolumeClaims []*corev1.PersistentVolumeClaim
}

func dumpIDString(o client.Object) string {
	kind := o.GetObjectKind().GroupVersionKind().String()
	return kind + ", ID=" + o.GetNamespace() + "/" + o.GetName()
}

func (c *ClusterObjects[T, U, PT, PU]) String() string {
	values := []string{}
	if c.Cluster != nil {
		values = append(values, "Cluster:")
		values = append(values, "\t"+dumpIDString(c.Cluster))
	}
	if c.Node != nil {
		values = append(values, "Node:")
		values = append(values, "\t"+dumpIDString(c.Node))
	}
	if c.Pod != nil {
		values = append(values, "Pod:")
		values = append(values, "\t"+dumpIDString(c.Pod))
	}
	if len(c.PersistentVolumes) != 0 {
		values = append(values, "Persistent Volumes:")
		for _, volume := range c.PersistentVolumes {
			values = append(values, "\t"+dumpIDString(volume))
		}
	}
	if len(c.PersistentVolumeClaims) != 0 {
		values = append(values, "Persistent Volume Claims:")
		for _, claim := range c.PersistentVolumeClaims {
			values = append(values, "\t"+dumpIDString(claim))
		}
	}
	return strings.Join(values, "\n")
}

type callback int

const (
	beforeCreateCallback callback = iota
	afterCreateCallback
	beforeDeleteCallback
	afterDeleteCallback
	beforeUpdateCallback
	afterUpdateCallback
	beforeDecommissionCallback
	afterDecommissionCallback
)

var (
	errUnknownCallback = errors.New("callback not supported")
)

type ClusterSubscriber[T, U any, PT ptrToObject[T], PU ptrToObject[U]] interface {
	BeforeClusterDelete(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error
	AfterClusterDelete(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error
}

func runSubscriberCallback[T, U any, PT ptrToObject[T], PU ptrToObject[U]](ctx context.Context, manager ClusterManager[T, U, PT, PU], objects *ClusterObjects[T, U, PT, PU], name callback) error {
	subscriber, ok := manager.(ClusterSubscriber[T, U, PT, PU])
	if !ok {
		return nil
	}

	switch name {
	case beforeDeleteCallback:
		return subscriber.BeforeClusterDelete(ctx, objects)
	case afterDeleteCallback:
		return subscriber.AfterClusterDelete(ctx, objects)
	}

	return errUnknownCallback
}

type ClusterNodeSubscriber[T, U any, PT ptrToObject[T], PU ptrToObject[U]] interface {
	BeforeClusterNodeCreate(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error
	AfterClusterNodeCreate(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error
	BeforeClusterNodeUpdate(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error
	AfterClusterNodeUpdate(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error
	BeforeClusterNodeDecommission(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error
	AfterClusterNodeDecommission(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error
	BeforeClusterNodeDelete(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error
	AfterClusterNodeDelete(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error
}

func runNodeSubscriberCallback[T, U any, PT ptrToObject[T], PU ptrToObject[U]](ctx context.Context, manager ClusterManager[T, U, PT, PU], objects *ClusterObjects[T, U, PT, PU], name callback) error {
	subscriber, ok := manager.(ClusterNodeSubscriber[T, U, PT, PU])
	if !ok {
		return nil
	}

	switch name {
	case beforeCreateCallback:
		return subscriber.BeforeClusterNodeCreate(ctx, objects)
	case afterCreateCallback:
		return subscriber.AfterClusterNodeCreate(ctx, objects)
	case beforeDeleteCallback:
		return subscriber.BeforeClusterNodeDelete(ctx, objects)
	case afterDeleteCallback:
		return subscriber.AfterClusterNodeDelete(ctx, objects)
	case beforeUpdateCallback:
		return subscriber.BeforeClusterNodeUpdate(ctx, objects)
	case afterUpdateCallback:
		return subscriber.AfterClusterNodeUpdate(ctx, objects)
	case beforeDecommissionCallback:
		return subscriber.BeforeClusterNodeDecommission(ctx, objects)
	case afterDecommissionCallback:
		return subscriber.AfterClusterNodeDecommission(ctx, objects)
	}

	return errUnknownCallback
}

type ClusterNodePodSubscriber[T, U any, PT ptrToObject[T], PU ptrToObject[U]] interface {
	BeforeClusterNodePodCreate(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error
	AfterClusterNodePodCreate(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error
	BeforeClusterNodePodDelete(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error
	AfterClusterNodePodDelete(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error
}

func runNodePodSubscriberCallback[T, U any, PT ptrToObject[T], PU ptrToObject[U]](ctx context.Context, manager ClusterManager[T, U, PT, PU], objects *ClusterObjects[T, U, PT, PU], name callback) error {
	subscriber, ok := manager.(ClusterNodePodSubscriber[T, U, PT, PU])
	if !ok {
		return nil
	}

	switch name {
	case beforeCreateCallback:
		return subscriber.BeforeClusterNodePodCreate(ctx, objects)
	case afterCreateCallback:
		return subscriber.AfterClusterNodePodCreate(ctx, objects)
	case beforeDeleteCallback:
		return subscriber.BeforeClusterNodePodDelete(ctx, objects)
	case afterDeleteCallback:
		return subscriber.AfterClusterNodePodDelete(ctx, objects)
	}

	return errUnknownCallback
}

type ClusterNodePersistentVolumeSubscriber[T, U any, PT ptrToObject[T], PU ptrToObject[U]] interface {
	BeforeClusterNodePersistentVolumeCreate(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error
	AfterClusterNodePersistentVolumeCreate(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error
	BeforeClusterNodePersistentVolumeDelete(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error
	AfterClusterNodePersistentVolumeDelete(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error
}

func runNodePersistentVolumeSubscriberCallback[T, U any, PT ptrToObject[T], PU ptrToObject[U]](ctx context.Context, manager ClusterManager[T, U, PT, PU], objects *ClusterObjects[T, U, PT, PU], name callback) error {
	subscriber, ok := manager.(ClusterNodePersistentVolumeSubscriber[T, U, PT, PU])
	if !ok {
		return nil
	}

	switch name {
	case beforeCreateCallback:
		return subscriber.BeforeClusterNodePersistentVolumeCreate(ctx, objects)
	case afterCreateCallback:
		return subscriber.AfterClusterNodePersistentVolumeCreate(ctx, objects)
	case beforeDeleteCallback:
		return subscriber.BeforeClusterNodePersistentVolumeDelete(ctx, objects)
	case afterDeleteCallback:
		return subscriber.AfterClusterNodePersistentVolumeDelete(ctx, objects)
	}

	return errUnknownCallback
}

type ClusterNodePersistentVolumeClaimSubscriber[T, U any, PT ptrToObject[T], PU ptrToObject[U]] interface {
	BeforeClusterNodePersistentVolumeClaimCreate(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error
	AfterClusterNodePersistentVolumeClaimCreate(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error
	BeforeClusterNodePersistentVolumeClaimDelete(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error
	AfterClusterNodePersistentVolumeClaimDelete(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error
}

func runNodePersistentVolumeClaimSubscriberCallback[T, U any, PT ptrToObject[T], PU ptrToObject[U]](ctx context.Context, manager ClusterManager[T, U, PT, PU], objects *ClusterObjects[T, U, PT, PU], name callback) error {
	subscriber, ok := manager.(ClusterNodePersistentVolumeClaimSubscriber[T, U, PT, PU])
	if !ok {
		return nil
	}

	switch name {
	case beforeCreateCallback:
		return subscriber.BeforeClusterNodePersistentVolumeClaimCreate(ctx, objects)
	case afterCreateCallback:
		return subscriber.AfterClusterNodePersistentVolumeClaimCreate(ctx, objects)
	case beforeDeleteCallback:
		return subscriber.BeforeClusterNodePersistentVolumeClaimDelete(ctx, objects)
	case afterDeleteCallback:
		return subscriber.AfterClusterNodePersistentVolumeClaimDelete(ctx, objects)
	}

	return errUnknownCallback
}

type debuggingCallbacks[T, U any, PT ptrToObject[T], PU ptrToObject[U]] struct {
	ClusterManager[T, U, PT, PU]
	logger logr.Logger
}

func (c *debuggingCallbacks[T, U, PT, PU]) BeforeClusterDelete(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	c.logger.Info("before cluster deletion", "objects", objects.String())
	return runSubscriberCallback(ctx, c.ClusterManager, objects, beforeDeleteCallback)
}

func (c *debuggingCallbacks[T, U, PT, PU]) AfterClusterDelete(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	c.logger.Info("after cluster deletion", "objects", objects.String())
	return runSubscriberCallback(ctx, c.ClusterManager, objects, afterDeleteCallback)
}

func (c *debuggingCallbacks[T, U, PT, PU]) BeforeClusterNodeCreate(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	c.logger.Info("before cluster node creation", "objects", objects.String())
	return runNodeSubscriberCallback(ctx, c.ClusterManager, objects, beforeCreateCallback)
}

func (c *debuggingCallbacks[T, U, PT, PU]) AfterClusterNodeCreate(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	c.logger.Info("after cluster node creation", "objects", objects.String())
	return runNodeSubscriberCallback(ctx, c.ClusterManager, objects, afterCreateCallback)
}

func (c *debuggingCallbacks[T, U, PT, PU]) BeforeClusterNodeUpdate(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	c.logger.Info("before cluster node update", "objects", objects.String())
	return runNodeSubscriberCallback(ctx, c.ClusterManager, objects, beforeUpdateCallback)
}

func (c *debuggingCallbacks[T, U, PT, PU]) AfterClusterNodeUpdate(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	c.logger.Info("after cluster node update", "objects", objects.String())
	return runNodeSubscriberCallback(ctx, c.ClusterManager, objects, afterUpdateCallback)
}

func (c *debuggingCallbacks[T, U, PT, PU]) BeforeClusterNodeDecommission(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	c.logger.Info("before cluster node decommission", "objects", objects.String())
	return runNodeSubscriberCallback(ctx, c.ClusterManager, objects, beforeDecommissionCallback)
}

func (c *debuggingCallbacks[T, U, PT, PU]) AfterClusterNodeDecommission(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	c.logger.Info("after cluster node decommission", "objects", objects.String())
	return runNodeSubscriberCallback(ctx, c.ClusterManager, objects, afterDecommissionCallback)
}

func (c *debuggingCallbacks[T, U, PT, PU]) BeforeClusterNodeDelete(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	c.logger.Info("before cluster node delete", "objects", objects.String())
	return runNodeSubscriberCallback(ctx, c.ClusterManager, objects, beforeDeleteCallback)
}

func (c *debuggingCallbacks[T, U, PT, PU]) AfterClusterNodeDelete(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	c.logger.Info("after cluster node delete", "objects", objects.String())
	return runNodeSubscriberCallback(ctx, c.ClusterManager, objects, afterDeleteCallback)
}

func (c *debuggingCallbacks[T, U, PT, PU]) BeforeClusterNodePodCreate(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	c.logger.Info("before cluster node pod create", "objects", objects.String())
	return runNodePodSubscriberCallback(ctx, c.ClusterManager, objects, beforeCreateCallback)
}

func (c *debuggingCallbacks[T, U, PT, PU]) AfterClusterNodePodCreate(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	c.logger.Info("after cluster node pod create", "objects", objects.String())
	return runNodePodSubscriberCallback(ctx, c.ClusterManager, objects, afterCreateCallback)
}

func (c *debuggingCallbacks[T, U, PT, PU]) BeforeClusterNodePodDelete(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	c.logger.Info("before cluster node pod delete", "objects", objects.String())
	return runNodePodSubscriberCallback(ctx, c.ClusterManager, objects, beforeDeleteCallback)
}

func (c *debuggingCallbacks[T, U, PT, PU]) AfterClusterNodePodDelete(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	c.logger.Info("after cluster node pod delete", "objects", objects.String())
	return runNodePodSubscriberCallback(ctx, c.ClusterManager, objects, afterDeleteCallback)
}

func (c *debuggingCallbacks[T, U, PT, PU]) BeforeClusterNodePersistentVolumeCreate(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	c.logger.Info("before cluster node persistent volume create", "objects", objects.String())
	return runNodePersistentVolumeSubscriberCallback(ctx, c.ClusterManager, objects, beforeCreateCallback)
}

func (c *debuggingCallbacks[T, U, PT, PU]) AfterClusterNodePersistentVolumeCreate(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	c.logger.Info("after cluster node persistent volume create", "objects", objects.String())
	return runNodePersistentVolumeSubscriberCallback(ctx, c.ClusterManager, objects, afterCreateCallback)
}

func (c *debuggingCallbacks[T, U, PT, PU]) BeforeClusterNodePersistentVolumeDelete(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	c.logger.Info("before cluster node persistent volume delete", "objects", objects.String())
	return runNodePersistentVolumeSubscriberCallback(ctx, c.ClusterManager, objects, beforeDeleteCallback)
}

func (c *debuggingCallbacks[T, U, PT, PU]) AfterClusterNodePersistentVolumeDelete(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	c.logger.Info("after cluster node persistent volume delete", "objects", objects.String())
	return runNodePersistentVolumeSubscriberCallback(ctx, c.ClusterManager, objects, afterDeleteCallback)
}

func (c *debuggingCallbacks[T, U, PT, PU]) BeforeClusterNodePersistentVolumeClaimCreate(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	c.logger.Info("before cluster node persistent volume claim create", "objects", objects.String())
	return runNodePersistentVolumeClaimSubscriberCallback(ctx, c.ClusterManager, objects, beforeCreateCallback)
}

func (c *debuggingCallbacks[T, U, PT, PU]) AfterClusterNodePersistentVolumeClaimCreate(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	c.logger.Info("after cluster node persistent volume claim create", "objects", objects.String())
	return runNodePersistentVolumeClaimSubscriberCallback(ctx, c.ClusterManager, objects, afterCreateCallback)
}

func (c *debuggingCallbacks[T, U, PT, PU]) BeforeClusterNodePersistentVolumeClaimDelete(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	c.logger.Info("before cluster node persistent volume claim delete", "objects", objects.String())
	return runNodePersistentVolumeClaimSubscriberCallback(ctx, c.ClusterManager, objects, beforeDeleteCallback)
}

func (c *debuggingCallbacks[T, U, PT, PU]) AfterClusterNodePersistentVolumeClaimDelete(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	c.logger.Info("after cluster node persistent volume claim delete", "objects", objects.String())
	return runNodePersistentVolumeClaimSubscriberCallback(ctx, c.ClusterManager, objects, afterDeleteCallback)
}

func DebugClusterManager[T, U any, PT ptrToObject[T], PU ptrToObject[U]](logger logr.Logger, manager ClusterManager[T, U, PT, PU]) ClusterManager[T, U, PT, PU] {
	return &debuggingCallbacks[T, U, PT, PU]{ClusterManager: manager, logger: logger}
}

// UnimplementedCallbacks is embeddable so that you only have to implement some of the given callbacks
type UnimplementedCallbacks[T, U any, PT ptrToObject[T], PU ptrToObject[U]] struct{}

func (c *UnimplementedCallbacks[T, U, PT, PU]) BeforeClusterDelete(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	return nil
}

func (c *UnimplementedCallbacks[T, U, PT, PU]) AfterClusterDelete(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	return nil
}

func (c *UnimplementedCallbacks[T, U, PT, PU]) BeforeClusterNodeCreate(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	return nil
}

func (c *UnimplementedCallbacks[T, U, PT, PU]) AfterClusterNodeCreate(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	return nil
}

func (c *UnimplementedCallbacks[T, U, PT, PU]) BeforeClusterNodeUpdate(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	return nil
}

func (c *UnimplementedCallbacks[T, U, PT, PU]) AfterClusterNodeUpdate(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	return nil
}

func (c *UnimplementedCallbacks[T, U, PT, PU]) BeforeClusterNodeDecommission(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	return nil
}

func (c *UnimplementedCallbacks[T, U, PT, PU]) AfterClusterNodeDecommission(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	return nil
}

func (c *UnimplementedCallbacks[T, U, PT, PU]) BeforeClusterNodeDelete(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	return nil
}

func (c *UnimplementedCallbacks[T, U, PT, PU]) AfterClusterNodeDelete(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	return nil
}

func (c *UnimplementedCallbacks[T, U, PT, PU]) BeforeClusterNodePodCreate(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	return nil
}

func (c *UnimplementedCallbacks[T, U, PT, PU]) AfterClusterNodePodCreate(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	return nil
}

func (c *UnimplementedCallbacks[T, U, PT, PU]) BeforeClusterNodePodDelete(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	return nil
}

func (c *UnimplementedCallbacks[T, U, PT, PU]) AfterClusterNodePodDelete(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	return nil
}

func (c *UnimplementedCallbacks[T, U, PT, PU]) BeforeClusterNodePersistentVolumeCreate(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	return nil
}

func (c *UnimplementedCallbacks[T, U, PT, PU]) AfterClusterNodePersistentVolumeCreate(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	return nil
}

func (c *UnimplementedCallbacks[T, U, PT, PU]) BeforeClusterNodePersistentVolumeDelete(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	return nil
}

func (c *UnimplementedCallbacks[T, U, PT, PU]) AfterClusterNodePersistentVolumeDelete(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	return nil
}

func (c *UnimplementedCallbacks[T, U, PT, PU]) BeforeClusterNodePersistentVolumeClaimCreate(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	return nil
}

func (c *UnimplementedCallbacks[T, U, PT, PU]) AfterClusterNodePersistentVolumeClaimCreate(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	return nil
}

func (c *UnimplementedCallbacks[T, U, PT, PU]) BeforeClusterNodePersistentVolumeClaimDelete(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	return nil
}

func (c *UnimplementedCallbacks[T, U, PT, PU]) AfterClusterNodePersistentVolumeClaimDelete(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	return nil
}
