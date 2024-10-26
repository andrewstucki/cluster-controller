package main

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"

	"github.com/andrewstucki/cluster-controller/controller"
	clusterv1alpha1 "github.com/andrewstucki/cluster-controller/controller/api/v1alpha1"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Manager struct {
	controller.UnimplementedCallbacks[Cluster, Broker, *Cluster, *Broker]

	logger logr.Logger
}

func (m *Manager) AfterClusterNodeCreate(ctx context.Context, objects *controller.ClusterObjects[Cluster, Broker, *Cluster, *Broker]) error {
	cluster := client.ObjectKeyFromObject(objects.Cluster).String()
	node := client.ObjectKeyFromObject(objects.Node).String()

	m.logger.Info("cluster node created", "cluster", cluster, "node", node)
	return nil
}

func (m *Manager) BeforeClusterNodeUpdate(ctx context.Context, objects *controller.ClusterObjects[Cluster, Broker, *Cluster, *Broker]) error {
	cluster := client.ObjectKeyFromObject(objects.Cluster).String()
	node := client.ObjectKeyFromObject(objects.Node).String()

	m.logger.Info("cluster node updating", "cluster", cluster, "node", node)
	return nil
}

func (m *Manager) AfterClusterNodeStabilized(ctx context.Context, objects *controller.ClusterObjects[Cluster, Broker, *Cluster, *Broker]) error {
	cluster := client.ObjectKeyFromObject(objects.Cluster).String()
	node := client.ObjectKeyFromObject(objects.Node).String()

	m.logger.Info("cluster node stabilized", "cluster", cluster, "node", node)
	return nil
}

func (m *Manager) BeforeClusterNodeDecommission(ctx context.Context, objects *controller.ClusterObjects[Cluster, Broker, *Cluster, *Broker]) error {
	cluster := client.ObjectKeyFromObject(objects.Cluster).String()
	node := client.ObjectKeyFromObject(objects.Node).String()

	m.logger.Info("cluster node decommissioning", "cluster", cluster, "node", node)
	return nil
}

func (m *Manager) BeforeClusterNodeDelete(ctx context.Context, objects *controller.ClusterObjects[Cluster, Broker, *Cluster, *Broker]) error {
	cluster := client.ObjectKeyFromObject(objects.Cluster).String()
	node := client.ObjectKeyFromObject(objects.Node).String()

	m.logger.Info("cluster node deleted", "cluster", cluster, "node", node)
	return nil
}

func (m *Manager) HashCluster(cluster *Cluster) (string, error) {
	return "static", nil
}

func (m *Manager) GetClusterStatus(cluster *Cluster) clusterv1alpha1.ClusterStatus {
	return cluster.Status
}

func (m *Manager) SetClusterStatus(cluster *Cluster, status clusterv1alpha1.ClusterStatus) {
	cluster.Status = status
}

func (m *Manager) GetClusterReplicas(cluster *Cluster) int {
	return cluster.Spec.Replicas
}

func (m *Manager) GetClusterMinimumHealthyReplicas(cluster *Cluster) int {
	return cluster.Spec.MinimumHealthyReplicas
}

func (m *Manager) GetClusterNodeTemplate(cluster *Cluster) *Broker {
	return &Broker{
		ObjectMeta: metav1.ObjectMeta{
			Name: "broker",
		},
	}
}

func (m *Manager) HashClusterNode(node *Broker) (string, error) {
	hf := fnv.New32()

	specData, err := json.Marshal(m.GetClusterNodePodSpec(node))
	if err != nil {
		return "", fmt.Errorf("marshaling pod spec: %w", err)
	}
	_, err = hf.Write(specData)
	if err != nil {
		return "", fmt.Errorf("hashing pod spec: %w", err)
	}

	for _, volume := range m.GetClusterNodeVolumeClaims(node) {
		volumeData, err := json.Marshal(volume)
		if err != nil {
			return "", fmt.Errorf("marshaling volume claim: %w", err)
		}
		_, err = hf.Write(volumeData)
		if err != nil {
			return "", fmt.Errorf("hashing volume claim: %w", err)
		}
	}

	return rand.SafeEncodeString(fmt.Sprint(hf.Sum32())), nil
}

func (m *Manager) GetClusterNodeStatus(node *Broker) clusterv1alpha1.ClusterNodeStatus {
	return node.Status
}

func (m *Manager) SetClusterNodeStatus(node *Broker, status clusterv1alpha1.ClusterNodeStatus) {
	node.Status = status
}

func (m *Manager) GetClusterNodePodSpec(node *Broker) *corev1.PodTemplateSpec {
	return &corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod",
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
						ClaimName: "volume",
						ReadOnly:  true,
					},
				},
			}},
		},
	}
}

func (m *Manager) GetClusterNodeVolumes(node *Broker) []*corev1.PersistentVolume {
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

func (m *Manager) GetClusterNodeVolumeClaims(node *Broker) []*corev1.PersistentVolumeClaim {
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
