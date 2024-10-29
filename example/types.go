// +versionName=v1alpha1
// +groupName=cluster.lambda.coffee
package main

import (
	"encoding/json"
	"fmt"
	"hash/fnv"

	clusterv1alpha1 "github.com/andrewstucki/cluster-controller/controller/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
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

func (b *Broker) GetStatus() clusterv1alpha1.ClusterNodeStatus {
	return b.Status
}

func (b *Broker) SetStatus(status clusterv1alpha1.ClusterNodeStatus) {
	b.Status = status
}

func (b *Broker) GetPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: b.GetNamespace(),
			Name:      b.GetName(),
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "container",
				Image: "bhargavshah86/kube-test:v0.1",
			}},
			RestartPolicy: corev1.RestartPolicyNever,
			Volumes: []corev1.Volume{{
				Name: "tmp",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: b.GetName() + "-volume",
						ReadOnly:  true,
					},
				},
			}},
		},
	}
}

func (b *Broker) GetHash() (string, error) {
	hf := fnv.New32()

	specData, err := json.Marshal(b.GetPod())
	if err != nil {
		return "", fmt.Errorf("marshaling pod: %w", err)
	}
	_, err = hf.Write(specData)
	if err != nil {
		return "", fmt.Errorf("hashing pod: %w", err)
	}

	return rand.SafeEncodeString(fmt.Sprint(hf.Sum32())), nil
}

func (b *Broker) ClusterScopedSubresources() []client.Object {
	return []client.Object{
		&corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name: b.GetNamespace() + "-" + b.GetName() + "-volume",
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
		},
	}
}

func (b *Broker) NamespaceScopedSubresources() []client.Object {
	return []client.Object{
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: b.GetNamespace(),
				Name:      b.GetName() + "-volume",
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				VolumeName:       b.GetNamespace() + "-" + b.GetName() + "-volume",
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
		},
	}
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

func (c *Cluster) GetHash() (string, error) {
	return "static", nil
}

func (c *Cluster) GetStatus() clusterv1alpha1.ClusterStatus {
	return c.Status
}

func (c *Cluster) SetStatus(status clusterv1alpha1.ClusterStatus) {
	c.Status = status
}

func (c *Cluster) GetReplicas() int {
	return c.Spec.Replicas
}

func (c *Cluster) GetMinimumHealthyReplicas() int {
	return c.Spec.MinimumHealthyReplicas
}

func (c *Cluster) GetNode() *Broker {
	return &Broker{
		ObjectMeta: metav1.ObjectMeta{
			Name: "broker",
		},
	}
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
