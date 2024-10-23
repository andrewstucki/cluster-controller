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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

const (
	defaultImage = "bhargavshah86/kube-test:v0.1"

	// ClusterKind is the name of the cluster kind
	ClusterKind = "Cluster"
)

// ClusterSpec defines the desired state of Cluster
type ClusterSpec struct {
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default:=1
	Replicas int `json:"replicas"`
	// +optional
	SchedulerName string `json:"schedulerName,omitempty"`
	// +optional
	Image string `json:"image,omitempty"`
}

func (cluster *Cluster) GetImageName() string {
	// If the image is specified in the status, use that one
	// It should be there since the first reconciliation
	if len(cluster.Status.Image) > 0 {
		return cluster.Status.Image
	}

	// Fallback to the information we have in the spec
	if len(cluster.Spec.Image) > 0 {
		return cluster.Spec.Image
	}

	// finally use what the current controller defaults to
	return defaultImage
}

type ReplicatedPodSpec struct {
	Replicas int                    `json:"replicas"`
	PodSpec  corev1.PodTemplateSpec `json:"podSpec"`
}

// ClusterStatus defines the observed state of Cluster
type ClusterStatus struct {
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +optional
	Replicas int `json:"replicas,omitempty"`
	// +optional
	CurrentReplicas int `json:"currentReplicas,omitempty"`
	// +optional
	UpdatedReplicas int `json:"updatedReplicas,omitempty"`
	// +optional
	ReadyReplicas int `json:"readyReplicas,omitempty"`
	// +optional
	LatestGeneratedNode int `json:"latestGeneratedNode,omitempty"`
	// +optional
	Image string `json:"image,omitempty"`
	// +optional
	CollisionCount *int32 `json:"collisionCount,omitempty"`
	// +optional
	CurrentRevision string `json:"currentRevision,omitempty"`
	// +optional
	UpdateRevision string `json:"updateRevision,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Children",type="integer",JSONPath=`.status.children`
// Cluster is the Schema for the Clusters API
type Cluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ClusterSpec   `json:"spec,omitempty"`
	Status ClusterStatus `json:"status,omitempty"`
}

func (c *Cluster) GetClusterStatus() ClusterStatus {
	return c.Status
}

func (c *Cluster) SetClusterStatus(status ClusterStatus) {
	c.Status = status
}

func (c *Cluster) GetReplicatedPodSpec() ReplicatedPodSpec {
	return ReplicatedPodSpec{
		Replicas: c.Spec.Replicas,
		PodSpec: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod",
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{
					Name:  "container",
					Image: c.GetImageName(),
				}},
				RestartPolicy: corev1.RestartPolicyNever,
			},
		},
	}
}

func (c *Cluster) GetReplicatedVolumeClaims() []corev1.PersistentVolumeClaim {
	return nil
}

// +kubebuilder:object:root=true

// ClusterList contains a list of Cluster
type ClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Cluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Cluster{}, &ClusterList{})
}
