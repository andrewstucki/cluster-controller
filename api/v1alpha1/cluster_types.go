/*
Copyright 2024.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

const (
	// ClusterKind is the name of the cluster kind
	ClusterKind = "Cluster"
)

// ClusterSpec defines the desired state of Cluster
type ClusterSpec struct {
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default:=1
	Replicas int `json:"replicas"`
	// +optional
	MinimumHealthyReplicas int `json:"minimumHealthyReplicas,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Cluster",type="string",JSONPath=`.metadata.labels['example-cluster-name']`
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=`.status.phase.name`
// +kubebuilder:printcolumn:name="Message",type="string",priority=1,JSONPath=`.status.phase.message`
// +kubebuilder:printcolumn:name="Running",type="boolean",JSONPath=`.status.running`
// +kubebuilder:printcolumn:name="Healthy",type="boolean",JSONPath=`.status.healthy`
type Broker struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Status ClusterNodeStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Replicas",type="integer",JSONPath=`.status.replicas`
// +kubebuilder:printcolumn:name="Running Nodes",type="integer",JSONPath=`.status.runningReplicas`
// +kubebuilder:printcolumn:name="Healthy Nodes",type="integer",JSONPath=`.status.healthyReplicas`
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=`.status.phase.name`
// +kubebuilder:printcolumn:name="Message",type="string",priority=1,JSONPath=`.status.phase.message`
// +kubebuilder:printcolumn:name="Up-to-date Nodes",type="integer",priority=1,JSONPath=`.status.upToDateReplicas`
// +kubebuilder:printcolumn:name="Out-of-date Nodes",type="integer",priority=1,JSONPath=`.status.outOfDateReplicas`
// Cluster is the Schema for the Clusters API
type Cluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ClusterSpec   `json:"spec,omitempty"`
	Status ClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ClusterList contains a list of Cluster
type ClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Cluster `json:"items"`
}

// +kubebuilder:object:root=true

// BrokerList contains a list of Broker
type BrokerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Broker `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Cluster{}, &ClusterList{})
	SchemeBuilder.Register(&Broker{}, &BrokerList{})
}

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
	Phase *Phase `json:"phase,omitempty"`
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
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
