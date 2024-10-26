package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"os"

	corev1 "k8s.io/api/core/v1"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	"k8s.io/utils/ptr"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/rand"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	clusterv1alpha1 "github.com/andrewstucki/cluster-controller/api/v1alpha1"
	"github.com/andrewstucki/cluster-controller/controller"
	"github.com/go-logr/logr"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	SchemeBuilder.Register(&Cluster{}, &ClusterList{})
	SchemeBuilder.Register(&Broker{}, &BrokerList{})
	utilruntime.Must(AddToScheme(scheme))
}

func main() {
	ctx := ctrl.SetupSignalHandler()

	var metricsAddr string
	var enableLeaderElection bool
	var probeAddr string
	var secureMetrics bool
	var enableHTTP2 bool
	flag.StringVar(&metricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to. "+
		"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.BoolVar(&secureMetrics, "metrics-secure", true,
		"If set, the metrics endpoint is served securely via HTTPS. Use --metrics-secure=false to use HTTP instead.")
	flag.BoolVar(&enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")
	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "10134cf3.lambda.coffee",
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	// manager := controller.DebugClusterManager(mgr.GetLogger(), &Manager{logger: mgr.GetLogger()})
	manager := &Manager{logger: mgr.GetLogger()}
	if err := controller.New(mgr, manager).
		WithFieldOwner("example-owner").
		WithHashLabel("example-cluster-hash").
		WithClusterLabel("example-cluster-name").
		Setup(ctx); err != nil {
		setupLog.Error(err, "unable to setup controllers")
		os.Exit(1)
	}

	// +kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctx); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}

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
