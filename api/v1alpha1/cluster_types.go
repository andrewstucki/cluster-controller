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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
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

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type Broker struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Status ClusterNodeStatus `json:"status,omitempty"`
}

func (b *Broker) GetClusterNodeStatus() ClusterNodeStatus {
	return b.Status
}

func (b *Broker) SetClusterNodeStatus(status ClusterNodeStatus) {
	b.Status = status
}

func (b *Broker) GetPodSpec() *corev1.PodTemplateSpec {
	return &corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod",
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "container",
				Image: defaultImage,
			}},
			RestartPolicy: corev1.RestartPolicyNever,
			Volumes: []corev1.Volume{{
				Name: "tmp",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: "volume",
						ReadOnly:  true,
					},
				},
			}},
		},
	}
}

func (b *Broker) GetVolumes() []*corev1.PersistentVolume {
	return []*corev1.PersistentVolume{{
		ObjectMeta: metav1.ObjectMeta{
			Name: "volume",
		},
		Spec: corev1.PersistentVolumeSpec{
			StorageClassName:              "manual",
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("1Mi"),
			},
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/tmp/foo",
				},
			},
		},
	}}
}

func (b *Broker) GetVolumeClaims() []*corev1.PersistentVolumeClaim {
	return []*corev1.PersistentVolumeClaim{{
		ObjectMeta: metav1.ObjectMeta{
			Name: "volume",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName:       "volume",
			StorageClassName: ptr.To("manual"),
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Mi"),
				},
			},
		},
	}}
}

type NodePhase struct {
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
	PreviousVersion string `json:"previousVersion,omitempty"`
	// +optional
	CurrentVersion string `json:"currentRevision,omitempty"`
	// +optional
	Phase NodePhase `json:"phase,omitempty"`
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
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
