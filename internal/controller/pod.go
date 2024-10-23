package controller

import (
	"fmt"

	clusterv1alpha1 "github.com/andrewstucki/cluster-controller/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	apimachineryvalidation "k8s.io/apimachinery/pkg/api/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
)

func initIdentity[T ClusterObject](cluster T, pod *corev1.Pod) {
	updateIdentity(cluster, pod)
	pod.Spec.Hostname = pod.Name
}

func newPod[T ClusterObject](parent T, spec *clusterv1alpha1.ReplicatedPodSpec, ordinal int) *corev1.Pod {
	pod, _ := getPodFromTemplate(&spec.PodSpec, parent, metav1.NewControllerRef(parent, parent.GetObjectKind().GroupVersionKind()))
	pod.Name = getPodName(parent, ordinal)
	pod.Namespace = parent.GetNamespace()
	initIdentity(parent, pod)
	updateStorage(parent, pod)
	return pod
}

func setPodRevision(pod *corev1.Pod, revision string) {
	if pod.Labels == nil {
		pod.Labels = make(map[string]string)
	}
	pod.Labels[revisionLabel] = revision
}

func newVersionedPod[T ClusterObject](cluster T, currentSpec, updateSpec *clusterv1alpha1.ReplicatedPodSpec, currentRevision, updateRevision string, replicas, ordinal int) *corev1.Pod {
	if ordinal < int(replicas) {
		pod := newPod(cluster, currentSpec, ordinal)
		setPodRevision(pod, currentRevision)
		return pod
	}
	pod := newPod(cluster, updateSpec, ordinal)
	setPodRevision(pod, updateRevision)
	return pod
}

func isRunningAndReady(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodRunning && isPodReady(pod)
}

func isCreated(pod *corev1.Pod) bool {
	return pod.Status.Phase != ""
}

func isHealthy(pod *corev1.Pod) bool {
	return isRunningAndReady(pod) && !isTerminating(pod)
}

func isFailed(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodFailed
}

func isSucceeded(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodSucceeded
}

func isTerminating(pod *corev1.Pod) bool {
	return pod.DeletionTimestamp != nil
}

func isPodReady(pod *corev1.Pod) bool {
	return isPodReadyConditionTrue(pod.Status)
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

func getPodFromTemplate(template *corev1.PodTemplateSpec, parentObject runtime.Object, controllerRef *metav1.OwnerReference) (*corev1.Pod, error) {
	desiredLabels := getPodsLabelSet(template)
	desiredFinalizers := getPodsFinalizers(template)
	desiredAnnotations := getPodsAnnotationSet(template)
	accessor, err := meta.Accessor(parentObject)
	if err != nil {
		return nil, fmt.Errorf("parentObject does not have ObjectMeta, %v", err)
	}
	prefix := getPodsPrefix(accessor.GetName())

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels:       desiredLabels,
			Annotations:  desiredAnnotations,
			GenerateName: prefix,
			Finalizers:   desiredFinalizers,
		},
	}
	if controllerRef != nil {
		pod.OwnerReferences = append(pod.OwnerReferences, *controllerRef)
	}
	pod.Spec = *template.Spec.DeepCopy()
	return pod, nil
}

func getPodsLabelSet(template *corev1.PodTemplateSpec) labels.Set {
	desiredLabels := make(labels.Set)
	for k, v := range template.Labels {
		desiredLabels[k] = v
	}
	return desiredLabels
}

func getPodsFinalizers(template *corev1.PodTemplateSpec) []string {
	desiredFinalizers := make([]string, len(template.Finalizers))
	copy(desiredFinalizers, template.Finalizers)
	return desiredFinalizers
}

func getPodsAnnotationSet(template *corev1.PodTemplateSpec) labels.Set {
	desiredAnnotations := make(labels.Set)
	for k, v := range template.Annotations {
		desiredAnnotations[k] = v
	}
	return desiredAnnotations
}

func getPodsPrefix(controllerName string) string {
	prefix := fmt.Sprintf("%s-", controllerName)
	if len(apimachineryvalidation.NameIsDNSSubdomain(prefix, true)) != 0 {
		prefix = controllerName
	}
	return prefix
}
