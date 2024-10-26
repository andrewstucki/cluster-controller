// +versionName=v1alpha1
// +groupName=cluster.lambda.coffee
package main

import (
	clusterv1alpha1 "github.com/andrewstucki/cluster-controller/controller/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	runtimescheme "sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	// GroupVersion is group version used to register these objects
	GroupVersion = schema.GroupVersion{Group: "cluster.lambda.coffee", Version: "v1alpha1"}

	// SchemeBuilder is used to add go types to the GroupVersionKind scheme
	SchemeBuilder = &runtimescheme.Builder{GroupVersion: GroupVersion}

	// AddToScheme adds the types in this group-version to the given scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)

// +kubebuilder:object:generate=true
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

	Status clusterv1alpha1.ClusterNodeStatus `json:"status,omitempty"`
}

type ClusterSpec struct {
	// +required
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Minimum=1
	Replicas int `json:"replicas,omitempty"`
	// +optional
	MinimumHealthyReplicas int `json:"minimumHealthyReplicas,omitempty"`
}

// +kubebuilder:object:generate=true
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

	// +optional
	Spec   ClusterSpec                   `json:"spec,omitempty"`
	Status clusterv1alpha1.ClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:generate=true
// +kubebuilder:object:root=true
// ClusterList contains a list of Cluster
type ClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Cluster `json:"items"`
}

// +kubebuilder:object:generate=true
// +kubebuilder:object:root=true
// BrokerList contains a list of Broker
type BrokerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Broker `json:"items"`
}
