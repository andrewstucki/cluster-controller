package controller

import (
	corev1 "k8s.io/api/core/v1"
)

func isRunningAndReady(testing bool, pod *corev1.Pod) bool {
	return testing || (pod.Status.Phase == corev1.PodRunning && isPodReady(testing, pod))
}

func isHealthy(testing bool, pod *corev1.Pod) bool {
	return testing || (isRunningAndReady(testing, pod) && !isTerminating(testing, pod))
}

func isFailed(testing bool, pod *corev1.Pod) bool {
	return !testing && pod.Status.Phase == corev1.PodFailed
}

func isSucceeded(testing bool, pod *corev1.Pod) bool {
	return !testing && pod.Status.Phase == corev1.PodSucceeded
}

func isTerminating(testing bool, pod *corev1.Pod) bool {
	return !testing && pod.DeletionTimestamp != nil
}

func isPodReady(testing bool, pod *corev1.Pod) bool {
	return testing || isPodReadyConditionTrue(pod.Status)
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
