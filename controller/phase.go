package controller

import (
	clusterv1alpha1 "github.com/andrewstucki/cluster-controller/controller/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func isTerminatingPhase(status phasedStatus) bool {
	return status.GetPhase().Name == "Terminating"
}

type phasedStatus interface {
	SetPhase(phase clusterv1alpha1.Phase)
	GetPhase() clusterv1alpha1.Phase
}

func setPhase(o client.Object, status phasedStatus, phase clusterv1alpha1.Phase) {
	if isTerminatingPhase(status) {
		// Terminating is a final phase, so nothing can override it
		return
	}

	if status.GetPhase().Name == phase.Name && status.GetPhase().Message == phase.Message && status.GetPhase().ObservedGeneration == o.GetGeneration() {
		return
	}

	phase.LastTransitionTime = metav1.Now()
	phase.ObservedGeneration = o.GetGeneration()
	status.SetPhase(phase)
}

func decommissioningPhase(message string) clusterv1alpha1.Phase {
	return clusterv1alpha1.Phase{
		Name:    "Decommissioning",
		Message: message,
	}
}

func gatedPhase(message string) clusterv1alpha1.Phase {
	return clusterv1alpha1.Phase{
		Name:    "Gated",
		Message: message,
	}
}

func terminatingPhase(message string) clusterv1alpha1.Phase {
	return clusterv1alpha1.Phase{
		Name:    "Terminating",
		Message: message,
	}
}

func restartingPhase(message string) clusterv1alpha1.Phase {
	return clusterv1alpha1.Phase{
		Name:    "Restarting",
		Message: message,
	}
}

func updatingPhase(message string) clusterv1alpha1.Phase {
	return clusterv1alpha1.Phase{
		Name:    "Updating",
		Message: message,
	}
}

func readyPhase(message string) clusterv1alpha1.Phase {
	return clusterv1alpha1.Phase{
		Name:    "Ready",
		Message: message,
	}
}

func initializingPhase(message string) clusterv1alpha1.Phase {
	return clusterv1alpha1.Phase{
		Name:    "Initializing",
		Message: message,
	}
}
