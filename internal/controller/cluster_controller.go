package controller

import (
	"context"
	"fmt"
	"time"

	clusterv1alpha1 "github.com/andrewstucki/cluster-controller/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	k8sapierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// For update implementation, see:
// 	https://github.com/pingcap/advanced-statefulset/blob/af926cc6da0de6138d66d0da9cae144bd54b9885/pkg/controller/statefulset/stateful_set_control.go#L90

const (
	podOwnerKey = ".metadata.controller"
)

var (
	nodesPredicate = predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldNode, oldOk := e.ObjectOld.(*corev1.Node)
			newNode, newOk := e.ObjectNew.(*corev1.Node)
			return oldOk && newOk && oldNode.Spec.Unschedulable != newNode.Spec.Unschedulable
		},
		CreateFunc: func(_ event.CreateEvent) bool {
			return false
		},
		DeleteFunc: func(_ event.DeleteEvent) bool {
			return false
		},
		GenericFunc: func(_ event.GenericEvent) bool {
			return false
		},
	}
)

type ClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=cluster.lambda.coffee,resources=clusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cluster.lambda.coffee,resources=clusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cluster.lambda.coffee,resources=clusters/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete

func (r *ClusterReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	if err := r.createFieldIndexes(ctx, mgr); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&clusterv1alpha1.Cluster{}).
		Owns(&corev1.Pod{}).
		Watches(
			&corev1.Node{},
			handler.EnqueueRequestsFromMapFunc(r.mapNodeToClusters()),
			builder.WithPredicates(nodesPredicate),
		).
		Complete(r)
}

func (r *ClusterReconciler) createFieldIndexes(ctx context.Context, mgr ctrl.Manager) error {
	// Create a new indexed field on Pods. This field will be used to easily
	// find all the Pods created by this controller
	if err := mgr.GetFieldIndexer().IndexField(
		ctx,
		&corev1.Pod{},
		podOwnerKey, func(rawObj client.Object) []string {
			pod := rawObj.(*corev1.Pod)

			if ownerName, ok := IsOwnedByCluster(pod); ok {
				return []string{ownerName}
			}

			return nil
		}); err != nil {
		return err
	}

	// Create a new indexed field on Pods. This field will be used to easily
	// find all the Pods created by node
	if err := mgr.GetFieldIndexer().IndexField(
		ctx,
		&corev1.Pod{},
		".spec.nodeName", func(rawObj client.Object) []string {
			pod := rawObj.(*corev1.Pod)
			if pod.Spec.NodeName == "" {
				return nil
			}

			return []string{pod.Spec.NodeName}
		}); err != nil {
		return err
	}

	return nil
}

func (r *ClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("reconciling cluster", "cluster", req.NamespacedName)

	cluster := &clusterv1alpha1.Cluster{}
	if err := r.Get(ctx, req.NamespacedName, cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !cluster.DeletionTimestamp.IsZero() {
		// deleting
		return ctrl.Result{}, nil
	}

	pods, err := GetClusterPods(ctx, r.Client, cluster.Name)
	if err != nil {
		return ctrl.Result{}, err
	}

	if result, err := r.deleteTerminatedPods(ctx, cluster, pods); err != nil {
		logger.Error(err, "While deleting terminated pods")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	} else if result != nil {
		return *result, nil
	}

	if result, err := r.processUnschedulablePods(ctx, cluster, pods); err != nil {
		logger.Error(err, "While processing unschedulable pods")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	} else if result != nil {
		return *result, err
	}

	// TODO: rollout first

	if cluster.Status.Replicas < cluster.Spec.Replicas {
		logger.Info("scaling up cluster")
		if pods, err = r.scaleUpCluster(ctx, cluster, pods); err != nil {
			return ctrl.Result{}, fmt.Errorf("cannot scale up cluster: %w", err)
		}
	}

	if cluster.Status.Replicas > cluster.Spec.Replicas {
		logger.Info("scaling down cluster")
		if pods, err = r.scaleDownCluster(ctx, cluster, pods); err != nil {
			return ctrl.Result{}, fmt.Errorf("cannot scale down cluster: %w", err)
		}
	}

	// if we are now here we can set our status updates
	cluster.Status.Replicas = cluster.Spec.Replicas
	cluster.Status.ReadyReplicas = CountReadyPods(pods)
	if err := r.Status().Update(ctx, cluster); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *ClusterReconciler) deleteTerminatedPods(ctx context.Context, cluster *clusterv1alpha1.Cluster, pods []corev1.Pod) (*ctrl.Result, error) {
	logger := log.FromContext(ctx)
	deletedPods := false

	for _, pod := range pods {
		if pod.GetDeletionTimestamp() != nil {
			continue
		}

		if pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
			continue
		}

		logger.Info("Deleting terminated pod", "podName", pod.Name, "phase", pod.Status.Phase)
		if err := r.Delete(ctx, &pod); err != nil && !k8sapierrors.IsNotFound(err) {
			return nil, err
		}
		deletedPods = true
	}

	if deletedPods {
		// We deleted objects. Give time to the informer cache to notice that.
		return &ctrl.Result{RequeueAfter: 1 * time.Second}, nil
	}

	return nil, nil
}

func (r *ClusterReconciler) processUnschedulablePods(ctx context.Context, cluster *clusterv1alpha1.Cluster, pods []corev1.Pod) (*ctrl.Result, error) {
	logger := log.FromContext(ctx)

	for _, pod := range pods {
		if pod.GetDeletionTimestamp() != nil {
			continue
		}

		if !IsPodUnschedulable(&pod) {
			continue
		}

		logger.Info("Deleting unschedulable pod", "pod", pod.Name, "podStatus", pod.Status)
		if err := r.Delete(ctx, &pod); err != nil && !k8sapierrors.IsNotFound(err) {
			return nil, err
		}

		// We deleted the pod. Give time to the informer cache to notice that.
		return &ctrl.Result{RequeueAfter: 1 * time.Second}, nil
	}

	return nil, nil
}

func (r *ClusterReconciler) scaleUpCluster(ctx context.Context, cluster *clusterv1alpha1.Cluster, pods []corev1.Pod) ([]corev1.Pod, error) {
	logger := log.FromContext(ctx)

	for i := len(pods); i < cluster.Spec.Replicas; i++ {
		serial, err := r.generateNodeSerial(ctx, cluster)
		if err != nil {
			return nil, err
		}

		pod := createPod(cluster, serial)
		if err := r.Create(ctx, pod); err != nil {
			if k8sapierrors.IsAlreadyExists(err) {
				continue
			}

			logger.Error(err, "Unable to create pod", "pod", pod)
			return nil, err
		}
		pods = append(pods, *pod)
	}

	return pods, nil
}

func (r *ClusterReconciler) scaleDownCluster(ctx context.Context, cluster *clusterv1alpha1.Cluster, pods []corev1.Pod) ([]corev1.Pod, error) {
	logger := log.FromContext(ctx)

	for i := len(pods); i > cluster.Spec.Replicas; i-- {
		pod := pods[i-1]
		pods = pods[:len(pods)-1]

		if err := r.Delete(ctx, &pod); err != nil {
			if k8sapierrors.IsNotFound(err) {
				continue
			}

			logger.Error(err, "Unable to delete pod", "pod", pod)
			return nil, err
		}
	}

	return pods, nil
}

func createPod(cluster *clusterv1alpha1.Cluster, id int) *corev1.Pod {
	name := fmt.Sprintf("%s-%v", cluster.Name, id)

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
			Labels: map[string]string{
				"managed-by": "cluster",
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: clusterv1alpha1.GroupVersion.String(),
				Kind:       clusterv1alpha1.ClusterKind,
				Name:       cluster.Name,
				UID:        cluster.UID,
				Controller: ptr.To(true),
			}},
		},
		Spec: corev1.PodSpec{
			SchedulerName: cluster.Spec.SchedulerName,
			Containers: []corev1.Container{
				{
					Name:  "main",
					Image: cluster.GetImageName(),
					Ports: []corev1.ContainerPort{{
						ContainerPort: 80,
					}},
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceMemory: resource.MustParse("256Mi"),
							corev1.ResourceCPU:    resource.MustParse("250m"),
						},
						Requests: corev1.ResourceList{
							corev1.ResourceMemory: resource.MustParse("128Mi"),
							corev1.ResourceCPU:    resource.MustParse("80m"),
						},
					},
				},
			},
			RestartPolicy: corev1.RestartPolicyNever,
		},
	}
}

func (r *ClusterReconciler) mapNodeToClusters() handler.MapFunc {
	return func(ctx context.Context, obj client.Object) []reconcile.Request {
		node := obj.(*corev1.Node)

		// exit if the node is schedulable (e.g. not cordoned)
		// could be expanded here with other conditions (e.g. pressure or issues)
		if !node.Spec.Unschedulable {
			return nil
		}

		var childPods corev1.PodList

		// get all the pods handled by the operator on that node
		err := r.List(ctx, &childPods, client.MatchingFields{".spec.nodeName": node.Name}, client.MatchingLabels{
			"managed-by": "cluster",
		})
		if err != nil {
			log.FromContext(ctx).Error(err, "while getting instances for node")
			return nil
		}
		var requests []reconcile.Request

		for idx := range childPods.Items {
			if cluster, ok := IsOwnedByCluster(&childPods.Items[idx]); ok {
				requests = append(requests,
					reconcile.Request{
						NamespacedName: types.NamespacedName{
							Name:      cluster,
							Namespace: childPods.Items[idx].Namespace,
						},
					},
				)
			}
		}
		return requests
	}
}

func (r *ClusterReconciler) generateNodeSerial(ctx context.Context, cluster *clusterv1alpha1.Cluster) (int, error) {
	cluster.Status.LatestGeneratedNode++
	if err := r.Status().Update(ctx, cluster); err != nil {
		return 0, err
	}

	return cluster.Status.LatestGeneratedNode, nil
}

func IsOwnedByCluster(obj client.Object) (string, bool) {
	owner := metav1.GetControllerOf(obj)
	if owner == nil {
		return "", false
	}

	if owner.Kind != clusterv1alpha1.ClusterKind {
		return "", false
	}

	if owner.APIVersion != clusterv1alpha1.GroupVersion.String() {
		return "", false
	}

	return owner.Name, true
}

func GetClusterPods(ctx context.Context, cl client.Client, clusterName string) ([]corev1.Pod, error) {
	var pods corev1.PodList
	if err := cl.List(ctx, &pods, &client.ListOptions{
		FieldSelector: fields.OneTermEqualSelector(podOwnerKey, clusterName),
	}); err != nil {
		return nil, err
	}

	return pods.Items, nil
}

func IsPodUnschedulable(p *corev1.Pod) bool {
	if corev1.PodPending != p.Status.Phase {
		return false
	}
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodScheduled &&
			c.Status == corev1.ConditionFalse &&
			c.Reason == corev1.PodReasonUnschedulable {
			return true
		}
	}

	return false
}

func IsPodReady(pod corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.ContainersReady && c.Status == corev1.ConditionTrue {
			return true
		}
	}

	return false
}

func CountReadyPods(podList []corev1.Pod) int {
	readyPods := 0
	for _, pod := range podList {
		if IsPodReady(pod) {
			readyPods++
		}
	}
	return readyPods
}
