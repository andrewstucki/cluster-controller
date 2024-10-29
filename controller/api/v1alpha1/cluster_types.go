// +kubebuilder:object:generate=true
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type Phase struct {
	// +required
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxLength=32768
	Name string `json:"name"`
	// +optional
	// +kubebuilder:validation:Minimum=0
	ObservedGeneration int64 `json:"observedGeneration,omitempty" protobuf:"varint,3,opt,name=observedGeneration"`
	// +required
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:Format=date-time
	LastTransitionTime metav1.Time `json:"lastTransitionTime"`
	// +required
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxLength=32768
	Message string `json:"message"`
}

type ClusterNodeStatus struct {
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +optional
	ClusterVersion string `json:"clusterVersion,omitempty"`
	// +optional
	PreviousVersion string `json:"previousVersion,omitempty"`
	// +optional
	CurrentVersion string `json:"currentRevision,omitempty"`
	// +optional
	Phase *Phase `json:"phase,omitempty"`
	// +optional
	Running bool `json:"running,omitempty"`
	// +optional
	Healthy bool `json:"healthy,omitempty"`
	// +optional
	MatchesCluster bool `json:"matchesCluster,omitempty"`
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

func (s *ClusterNodeStatus) SetPhase(phase Phase) {
	s.Phase = &phase
}

func (s *ClusterNodeStatus) GetPhase() Phase {
	if s.Phase == nil {
		return Phase{}
	}
	return *s.Phase
}

// ClusterStatus defines the observed state of Cluster
type ClusterStatus struct {
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +optional
	NextNode string `json:"nextNode,omitempty"`
	// +optional
	Nodes []string `json:"nodes,omitempty"`
	// +optional
	Replicas int `json:"replicas,omitempty"`
	// +optional
	OutOfDateReplicas int `json:"outOfDateReplicas,omitempty"`
	// +optional
	UpToDateReplicas int `json:"upToDateReplicas,omitempty"`
	// +optional
	HealthyReplicas int `json:"healthyReplicas,omitempty"`
	// +optional
	RunningReplicas int `json:"runningReplicas,omitempty"`
	// +optional
	Selector string `json:"selector,omitempty"`
	// +optional
	Phase *Phase `json:"phase,omitempty"`
}

func (s *ClusterStatus) SetPhase(phase Phase) {
	s.Phase = &phase
}

func (s *ClusterStatus) GetPhase() Phase {
	if s.Phase == nil {
		return Phase{}
	}
	return *s.Phase
}
