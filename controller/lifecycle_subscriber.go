package controller

import "context"

type LifecycleSubscriber[T, U any, PT ptrToObject[T], PU ptrToObject[U]] interface {
	AfterClusterCreated(ctx context.Context, cluster PT) error
	AfterNodeCreated(ctx context.Context, cluster PT, node PU) error
	BeforeNodeUpdated(ctx context.Context, cluster PT, node PU) error
	AfterNodeDeployed(ctx context.Context, cluster PT, node PU) error
	BeforeNodeDecommissioned(ctx context.Context, cluster PT, node PU) error
	AfterNodeDeleted(ctx context.Context, cluster PT, node PU) error
	BeforeClusterDeleted(ctx context.Context, cluster PT) error
	AfterClusterDeleted(ctx context.Context, cluster PT) error
}

type UnimplementedLifecycleSubscriber[T, U any, PT ptrToObject[T], PU ptrToObject[U]] struct{}

func (s *UnimplementedLifecycleSubscriber[T, U, PT, PU]) AfterClusterCreated(ctx context.Context, cluster PT) error {
	return nil
}

func (s *UnimplementedLifecycleSubscriber[T, U, PT, PU]) AfterNodeCreated(ctx context.Context, cluster PT, node PU) error {
	return nil
}

func (s *UnimplementedLifecycleSubscriber[T, U, PT, PU]) BeforeNodeUpdated(ctx context.Context, cluster PT, node PU) error {
	return nil
}

func (s *UnimplementedLifecycleSubscriber[T, U, PT, PU]) AfterNodeDeployed(ctx context.Context, cluster PT, node PU) error {
	return nil
}

func (s *UnimplementedLifecycleSubscriber[T, U, PT, PU]) BeforeNodeDecommissioned(ctx context.Context, cluster PT, node PU) error {
	return nil
}

func (s *UnimplementedLifecycleSubscriber[T, U, PT, PU]) AfterNodeDeleted(ctx context.Context, cluster PT, node PU) error {
	return nil
}

func (s *UnimplementedLifecycleSubscriber[T, U, PT, PU]) BeforeClusterDeleted(ctx context.Context, cluster PT) error {
	return nil
}

func (s *UnimplementedLifecycleSubscriber[T, U, PT, PU]) AfterClusterDeleted(ctx context.Context, cluster PT) error {
	return nil
}
