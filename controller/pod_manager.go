package controller

import (
	"context"

	corev1 "k8s.io/api/core/v1"
)

type PodManager[T any, PT ptrToObject[T]] struct {
	*ResourceClient[T, PT]

	hashLabel string
	hashFn    func(node PT) string
	podFn     func(node PT) *corev1.Pod

	testing bool
}

func (p *PodManager[T, PT]) VersionMatches(version string, pod *corev1.Pod) bool {
	if labels := pod.GetLabels(); labels != nil {
		return version == labels[p.hashLabel]
	}
	return false
}

func (p *PodManager[T, PT]) CreatePod(ctx context.Context, owner PT) (string, *corev1.Pod, error) {
	pod := p.podFn(owner)
	hash := p.hashFn(owner)
	if err := p.PatchOwnedResource(ctx, owner, pod, map[string]string{p.hashLabel: hash}); err != nil {
		return "", nil, err
	}

	return hash, pod, nil
}

func (p *PodManager[T, PT]) ListPods(ctx context.Context, owner PT) ([]*corev1.Pod, error) {
	objects, err := p.ListOwnedResources(ctx, owner, &corev1.Pod{})
	if err != nil {
		return nil, err
	}

	return mapObjectsTo[*corev1.Pod](objects), nil
}

func (p *PodManager[T, PT]) IsRunningAndReady(pod *corev1.Pod) bool {
	return p.testing || (pod.Status.Phase == corev1.PodRunning && p.IsPodReady(pod))
}

func (p *PodManager[T, PT]) IsHealthy(pod *corev1.Pod) bool {
	return p.testing || (p.IsRunningAndReady(pod) && !p.IsTerminating(pod))
}

func (p *PodManager[T, PT]) IsFailed(pod *corev1.Pod) bool {
	return !p.testing && pod.Status.Phase == corev1.PodFailed
}

func (p *PodManager[T, PT]) IsSucceeded(pod *corev1.Pod) bool {
	return !p.testing && pod.Status.Phase == corev1.PodSucceeded
}

func (p *PodManager[T, PT]) IsTerminating(pod *corev1.Pod) bool {
	return !p.testing && pod.DeletionTimestamp != nil
}

func (p *PodManager[T, PT]) IsPodReady(pod *corev1.Pod) bool {
	return p.testing || isPodReadyConditionTrue(pod.Status)
}

func isPodReadyConditionTrue(status corev1.PodStatus) bool {
	condition := getPodReadyCondition(status)
	return condition != nil && condition.Status == corev1.ConditionTrue
}

func getPodReadyCondition(status corev1.PodStatus) *corev1.PodCondition {
	_, condition := getPodCondition(&status, corev1.PodReady)
	return condition
}

func getPodCondition(status *corev1.PodStatus, conditionType corev1.PodConditionType) (int, *corev1.PodCondition) {
	if status == nil {
		return -1, nil
	}
	return getPodConditionFromList(status.Conditions, conditionType)
}

func getPodConditionFromList(conditions []corev1.PodCondition, conditionType corev1.PodConditionType) (int, *corev1.PodCondition) {
	if conditions == nil {
		return -1, nil
	}
	for i := range conditions {
		if conditions[i].Type == conditionType {
			return i, &conditions[i]
		}
	}
	return -1, nil
}
