package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	clusterv1alpha1 "github.com/andrewstucki/cluster-controller/api/v1alpha1"
	"github.com/cenkalti/backoff"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	errUnknownCluster     = errors.New("unknown cluster")
	errUnknownClusterNode = errors.New("unknown cluster node")
)

type TestClusterFactory[T, U any, PT ptrToObject[T], PU ptrToObject[U]] struct {
	*TestManager[T, U, PT, PU]
	Client            client.Client
	NodeCustomizer    func(node *TestClusterNode[U, PU])
	ClusterCustomizer func(cluster *TestCluster[U, PU])
}

func NewTestClusterFactory[T, U any, PT ptrToObject[T], PU ptrToObject[U]](cl client.Client, manager *TestManager[T, U, PT, PU], nodeCustomizer func(node *TestClusterNode[U, PU]), clusterCustomizer func(cluster *TestCluster[U, PU])) *TestClusterFactory[T, U, PT, PU] {
	return &TestClusterFactory[T, U, PT, PU]{
		Client:            cl,
		TestManager:       manager,
		NodeCustomizer:    nodeCustomizer,
		ClusterCustomizer: clusterCustomizer,
	}
}

func (f *TestClusterFactory[T, U, PT, PU]) CreateCluster(ctx context.Context, cluster PT, node PU) error {
	nodeTemplate := NewTestClusterNode[U, PU]()
	f.NodeCustomizer(nodeTemplate)

	testCluster := NewTestCluster(cluster, nodeTemplate)
	f.ClusterCustomizer(testCluster)

	f.TestManager.RegisterCluster(testCluster.SetClusterNodeTemplate(node))

	return f.Client.Create(ctx, cluster)
}

func (f *TestClusterFactory[T, U, PT, PU]) DeleteCluster(ctx context.Context, cluster PT) error {
	return f.Client.Create(ctx, cluster)
}

func (f *TestClusterFactory[T, U, PT, PU]) DeleteNode(ctx context.Context, node PU) error {
	return f.Client.Create(ctx, node)
}

func (f *TestClusterFactory[T, U, PT, PU]) WaitForStableNodes(ctx context.Context, timeout time.Duration, cluster PT) ([]PU, error) {
	retryFor := backoff.WithMaxRetries(backoff.NewConstantBackOff(1*time.Second), uint64(timeout/time.Second))
	var nodes []PU
	var err error
	err = backoff.Retry(func() error {
		nodes, err = getTestClusterNodes[T, U, PT, PU](ctx, f.Client, f.Client.Scheme(), cluster)
		if err != nil {
			return err
		}

		if len(nodes) != f.TestManager.GetClusterReplicas(cluster) {
			return fmt.Errorf("cluster node length doesn't match: %d != %d", len(nodes), f.TestManager.GetClusterReplicas(cluster))
		}

		for _, node := range nodes {
			if !f.TestManager.GetClusterNodeStatus(node).Healthy {
				return errors.New("cluster nodes not yet healthy")
			}
		}

		return nil
	}, backoff.WithContext(retryFor, ctx))

	if err != nil {
		return nil, err
	}

	return nodes, nil
}

type TestManager[T, U any, PT ptrToObject[T], PU ptrToObject[U]] struct {
	UnimplementedCallbacks[T, U, PT, PU]

	onClusterStatusGet        func(cluster PT) clusterv1alpha1.ClusterStatus
	onClusterStatusUpdate     func(cluster PT, status clusterv1alpha1.ClusterStatus)
	onClusterNodeStatusGet    func(node PU) clusterv1alpha1.ClusterNodeStatus
	onClusterNodeStatusUpdate func(node PU, status clusterv1alpha1.ClusterNodeStatus)

	clusters map[types.NamespacedName]*TestCluster[U, PU]
	nodes    map[types.NamespacedName]*TestClusterNode[U, PU]

	mutex sync.RWMutex
}

func NewTestManager[T, U any, PT ptrToObject[T], PU ptrToObject[U]]() *TestManager[T, U, PT, PU] {
	return &TestManager[T, U, PT, PU]{
		clusters: make(map[types.NamespacedName]*TestCluster[U, PU]),
		nodes:    make(map[types.NamespacedName]*TestClusterNode[U, PU]),
	}
}

func (t *TestManager[T, U, PT, PU]) WithClusterStatusUpdate(fn func(cluster PT, status clusterv1alpha1.ClusterStatus)) *TestManager[T, U, PT, PU] {
	t.onClusterStatusUpdate = fn
	return t
}

func (t *TestManager[T, U, PT, PU]) WithClusterNodeStatusUpdate(fn func(node PU, status clusterv1alpha1.ClusterNodeStatus)) *TestManager[T, U, PT, PU] {
	t.onClusterNodeStatusUpdate = fn
	return t
}

func (t *TestManager[T, U, PT, PU]) WithClusterStatusGet(fn func(cluster PT) clusterv1alpha1.ClusterStatus) *TestManager[T, U, PT, PU] {
	t.onClusterStatusGet = fn
	return t
}

func (t *TestManager[T, U, PT, PU]) WithClusterNodeStatusGet(fn func(node PU) clusterv1alpha1.ClusterNodeStatus) *TestManager[T, U, PT, PU] {
	t.onClusterNodeStatusGet = fn
	return t
}

func (t *TestManager[T, U, PT, PU]) RegisterCluster(cluster *TestCluster[U, PU]) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.clusters[cluster.id] = cluster
}

func (t *TestManager[T, U, PT, PU]) Cluster(id types.NamespacedName) (*TestCluster[U, PU], error) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	if cluster, ok := t.clusters[id]; ok {
		return cluster, nil
	}
	return nil, fmt.Errorf("%w: %q", errUnknownCluster, id)
}

func (t *TestManager[T, U, PT, PU]) ClusterNode(id types.NamespacedName) (*TestClusterNode[U, PU], error) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	if node, ok := t.nodes[id]; ok {
		return node, nil
	}
	return nil, fmt.Errorf("%w: %q", errUnknownClusterNode, id)
}

func (t *TestManager[T, U, PT, PU]) BeforeClusterDelete(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	cluster, err := t.getCluster(objects.Cluster)
	if err != nil {
		return err
	}
	cluster.deleting.Store(true)

	return nil
}

func (t *TestManager[T, U, PT, PU]) AfterClusterDelete(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	cluster, err := t.getCluster(objects.Cluster)
	if err != nil {
		return err
	}
	cluster.deleted.Store(true)

	return nil
}

func (t *TestManager[T, U, PT, PU]) BeforeClusterNodeCreate(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	cluster, err := t.getCluster(objects.Cluster)
	if err != nil {
		return err
	}

	node := cluster.TestNodeFor(objects.Node)
	t.nodes[node.id] = node

	return nil
}

func (t *TestManager[T, U, PT, PU]) AfterClusterNodeUpdate(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	node, err := t.getClusterNode(objects.Node)
	if err != nil {
		return err
	}

	node.updates.Add(1)

	return nil
}

func (t *TestManager[T, U, PT, PU]) BeforeClusterNodeDecommission(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	node, err := t.getClusterNode(objects.Node)
	if err != nil {
		return err
	}

	node.decommissioning.Store(true)

	return nil
}

func (t *TestManager[T, U, PT, PU]) AfterClusterNodeDecommission(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	node, err := t.getClusterNode(objects.Node)
	if err != nil {
		return err
	}

	node.decommissioned.Store(true)

	return nil
}

func (t *TestManager[T, U, PT, PU]) BeforeClusterNodeDelete(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	node, err := t.getClusterNode(objects.Node)
	if err != nil {
		return err
	}

	node.deleting.Store(true)

	return nil
}

func (t *TestManager[T, U, PT, PU]) AfterClusterNodeDelete(ctx context.Context, objects *ClusterObjects[T, U, PT, PU]) error {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	node, err := t.getClusterNode(objects.Node)
	if err != nil {
		return err
	}

	node.deleted.Store(true)

	return nil
}

func (t *TestManager[T, U, PT, PU]) HashCluster(cluster PT) (string, error) {
	c, err := t.Cluster(client.ObjectKeyFromObject(cluster))
	if err != nil {
		panic(err)
	}
	return c.GetHashClusterResponse()
}

func (t *TestManager[T, U, PT, PU]) GetClusterStatus(cluster PT) clusterv1alpha1.ClusterStatus {
	c, err := t.Cluster(client.ObjectKeyFromObject(cluster))
	if err != nil {
		panic(err)
	}
	if t.onClusterStatusGet != nil {
		return t.onClusterStatusGet(cluster)
	}
	return c.GetClusterStatus()
}

func (t *TestManager[T, U, PT, PU]) SetClusterStatus(cluster PT, status clusterv1alpha1.ClusterStatus) {
	c, err := t.Cluster(client.ObjectKeyFromObject(cluster))
	if err != nil {
		panic(err)
	}
	if t.onClusterStatusUpdate != nil {
		t.onClusterStatusUpdate(cluster, status)
	}
	c.SetClusterStatus(status)
}

func (t *TestManager[T, U, PT, PU]) GetClusterReplicas(cluster PT) int {
	c, err := t.Cluster(client.ObjectKeyFromObject(cluster))
	if err != nil {
		panic(err)
	}
	return c.GetReplicas()
}

func (t *TestManager[T, U, PT, PU]) GetClusterMinimumHealthyReplicas(cluster PT) int {
	c, err := t.Cluster(client.ObjectKeyFromObject(cluster))
	if err != nil {
		panic(err)
	}
	return c.GetMinimumHealthyReplicas()
}

func (t *TestManager[T, U, PT, PU]) GetClusterNodeTemplate(cluster PT) PU {
	c, err := t.Cluster(client.ObjectKeyFromObject(cluster))
	if err != nil {
		panic(err)
	}
	return c.GetClusterNodeTemplate()
}

func (t *TestManager[T, U, PT, PU]) HashClusterNode(node PU) (string, error) {
	c, err := t.ClusterNode(client.ObjectKeyFromObject(node))
	if err != nil {
		panic(err)
	}
	return c.GetHashClusterNodeResponse()
}

func (t *TestManager[T, U, PT, PU]) GetClusterNodeStatus(node PU) clusterv1alpha1.ClusterNodeStatus {
	c, err := t.ClusterNode(client.ObjectKeyFromObject(node))
	if err != nil {
		panic(err)
	}
	if t.onClusterNodeStatusGet != nil {
		return t.onClusterNodeStatusGet(node)
	}
	return c.GetClusterNodeStatus()
}

func (t *TestManager[T, U, PT, PU]) SetClusterNodeStatus(node PU, status clusterv1alpha1.ClusterNodeStatus) {
	c, err := t.ClusterNode(client.ObjectKeyFromObject(node))
	if err != nil {
		panic(err)
	}
	if t.onClusterNodeStatusUpdate != nil {
		t.onClusterNodeStatusUpdate(node, status)
	}
	c.SetClusterNodeStatus(status)
}

func (t *TestManager[T, U, PT, PU]) GetClusterNodePodSpec(node PU) *corev1.PodTemplateSpec {
	c, err := t.ClusterNode(client.ObjectKeyFromObject(node))
	if err != nil {
		panic(err)
	}
	return c.GetClusterNodePodSpec()
}

func (t *TestManager[T, U, PT, PU]) GetClusterNodeVolumes(node PU) []*corev1.PersistentVolume {
	c, err := t.ClusterNode(client.ObjectKeyFromObject(node))
	if err != nil {
		panic(err)
	}
	return c.GetClusterNodeVolumes()
}

func (t *TestManager[T, U, PT, PU]) GetClusterNodeVolumeClaims(node PU) []*corev1.PersistentVolumeClaim {
	c, err := t.ClusterNode(client.ObjectKeyFromObject(node))
	if err != nil {
		panic(err)
	}
	return c.GetClusterNodeVolumeClaims()
}

func (t *TestManager[T, U, PT, PU]) getCluster(cluster PT) (*TestCluster[U, PU], error) {
	key := client.ObjectKeyFromObject(cluster)
	if cluster, ok := t.clusters[key]; ok {
		return cluster, nil
	}
	return nil, fmt.Errorf("%w: %q", errUnknownCluster, key)
}

func (t *TestManager[T, U, PT, PU]) getClusterNode(node PU) (*TestClusterNode[U, PU], error) {
	key := client.ObjectKeyFromObject(node)
	if node, ok := t.nodes[key]; ok {
		return node, nil
	}
	return nil, fmt.Errorf("%w: %q", errUnknownClusterNode, key)
}

type TestClusterNode[U any, PU ptrToObject[U]] struct {
	id types.NamespacedName

	clusterNodeStatus clusterv1alpha1.ClusterNodeStatus
	nodeHash          string
	nodeHashError     error
	podSpec           *corev1.PodTemplateSpec
	volumes           []*corev1.PersistentVolume
	claims            []*corev1.PersistentVolumeClaim

	updates         atomic.Int64
	decommissioning atomic.Bool
	decommissioned  atomic.Bool
	deleting        atomic.Bool
	deleted         atomic.Bool

	mutex sync.RWMutex
}

func NewTestClusterNode[U any, PU ptrToObject[U]]() *TestClusterNode[U, PU] {
	return &TestClusterNode[U, PU]{}
}

func (t *TestClusterNode[U, PU]) Clone(id types.NamespacedName) *TestClusterNode[U, PU] {
	return &TestClusterNode[U, PU]{
		id:                id,
		clusterNodeStatus: *t.clusterNodeStatus.DeepCopy(),
		nodeHash:          t.nodeHash,
		nodeHashError:     t.nodeHashError,
		podSpec:           t.podSpec.DeepCopy(),
		volumes:           slices.Clone(t.volumes),
		claims:            slices.Clone(t.claims),
	}
}

func (t *TestClusterNode[U, PU]) SetHashClusterNodeResponse(hash string, err error) *TestClusterNode[U, PU] {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.nodeHash = hash
	t.nodeHashError = err
	return t
}

func (t *TestClusterNode[U, PU]) GetHashClusterNodeResponse() (string, error) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	return t.nodeHash, t.nodeHashError
}

func (t *TestClusterNode[U, PU]) SetClusterNodeStatus(status clusterv1alpha1.ClusterNodeStatus) *TestClusterNode[U, PU] {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.clusterNodeStatus = status
	return t
}

func (t *TestClusterNode[U, PU]) GetClusterNodeStatus() clusterv1alpha1.ClusterNodeStatus {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	return *t.clusterNodeStatus.DeepCopy()
}

func (t *TestClusterNode[U, PU]) SetClusterNodePodSpec(spec *corev1.PodTemplateSpec) *TestClusterNode[U, PU] {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.podSpec = spec
	return t
}

func (t *TestClusterNode[U, PU]) GetClusterNodePodSpec() *corev1.PodTemplateSpec {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	return t.podSpec.DeepCopy()
}

func (t *TestClusterNode[U, PU]) SetClusterNodeVolumes(volumes []*corev1.PersistentVolume) *TestClusterNode[U, PU] {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.volumes = volumes
	return t
}

func (t *TestClusterNode[U, PU]) GetClusterNodeVolumes() []*corev1.PersistentVolume {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	return slices.Clone(t.volumes)
}

func (t *TestClusterNode[U, PU]) SetClusterNodeVolumeClaims(claims []*corev1.PersistentVolumeClaim) *TestClusterNode[U, PU] {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.claims = claims
	return t
}

func (t *TestClusterNode[U, PU]) GetClusterNodeVolumeClaims() []*corev1.PersistentVolumeClaim {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	return slices.Clone(t.claims)
}

func (t *TestClusterNode[U, PU]) IsDecommissioning() bool {
	return !t.IsDeleting() && !t.IsDeleted() && !t.IsDecommissioned() && t.decommissioning.Load()
}

func (t *TestClusterNode[U, PU]) IsDecommissioned() bool {
	return !t.IsDeleting() && !t.IsDeleted() && t.decommissioned.Load()
}

func (t *TestClusterNode[U, PU]) IsDeleting() bool {
	return !t.IsDeleted() && t.deleting.Load()
}

func (t *TestClusterNode[U, PU]) IsDeleted() bool {
	return t.deleted.Load()
}

func (t *TestClusterNode[U, PU]) Updates() int64 {
	return t.updates.Load()
}

type TestCluster[U any, PU ptrToObject[U]] struct {
	id                      types.NamespacedName
	testClusterNodeTemplate *TestClusterNode[U, PU]

	clusterStatus    clusterv1alpha1.ClusterStatus
	replicas         int
	minReplicas      int
	clusterHash      string
	clusterHashError error
	clusterNode      PU

	deleting atomic.Bool
	deleted  atomic.Bool

	mutex sync.RWMutex
}

func NewTestCluster[U any, PU ptrToObject[U]](o client.Object, testClusterNodeTemplate *TestClusterNode[U, PU]) *TestCluster[U, PU] {
	return &TestCluster[U, PU]{id: client.ObjectKeyFromObject(o), testClusterNodeTemplate: testClusterNodeTemplate}
}

func (t *TestCluster[U, PU]) SetHashClusterResponse(hash string, err error) *TestCluster[U, PU] {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.clusterHash = hash
	t.clusterHashError = err
	return t
}

func (t *TestCluster[U, PU]) GetHashClusterResponse() (string, error) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	return t.clusterHash, t.clusterHashError
}

func (t *TestCluster[U, PU]) SetClusterStatus(status clusterv1alpha1.ClusterStatus) *TestCluster[U, PU] {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.clusterStatus = status
	return t
}

func (t *TestCluster[U, PU]) GetClusterStatus() clusterv1alpha1.ClusterStatus {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	return *t.clusterStatus.DeepCopy()
}

func (t *TestCluster[U, PU]) SetReplicas(replicas int) *TestCluster[U, PU] {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.replicas = replicas
	return t
}

func (t *TestCluster[U, PU]) GetReplicas() int {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	return t.replicas
}

func (t *TestCluster[U, PU]) SetMinimumHealthyReplicas(replicas int) *TestCluster[U, PU] {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.minReplicas = replicas
	return t
}

func (t *TestCluster[U, PU]) GetMinimumHealthyReplicas() int {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	return t.minReplicas
}

func (t *TestCluster[U, PU]) SetClusterNodeTemplate(node PU) *TestCluster[U, PU] {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.clusterNode = node
	return t
}

func (t *TestCluster[U, PU]) GetClusterNodeTemplate() PU {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	return t.clusterNode.DeepCopyObject().(PU)
}

func (t *TestCluster[U, PU]) TestNodeFor(o client.Object) *TestClusterNode[U, PU] {
	return t.testClusterNodeTemplate.Clone(client.ObjectKeyFromObject(o))
}

func (t *TestCluster[U, PU]) IsDeleting() bool {
	return !t.IsDeleted() && t.deleting.Load()
}

func (t *TestCluster[U, PU]) IsDeleted() bool {
	return t.deleted.Load()
}

func getTestClusterNodes[T, U any, PT ptrToObject[T], PU ptrToObject[U]](ctx context.Context, cl client.Client, scheme *runtime.Scheme, cluster PT) ([]PU, error) {
	kinds, _, err := scheme.ObjectKinds(newKubeObject[U, PU]())
	if err != nil {
		return nil, fmt.Errorf("fetching object kind: %w", err)
	}
	if len(kinds) == 0 {
		return nil, fmt.Errorf("unable to determine object kind")
	}
	kind := kinds[0]
	kind.Kind += "List"

	o, err := scheme.New(kind)
	if err != nil {
		return nil, fmt.Errorf("initializing list: %w", err)
	}
	list, ok := o.(client.ObjectList)
	if !ok {
		return nil, fmt.Errorf("invalid object list type: %T", o)
	}

	if err := cl.List(ctx, list, client.InNamespace(cluster.GetNamespace()), client.MatchingLabels{defaultClusterLabel: cluster.GetName()}); err != nil {
		return nil, fmt.Errorf("listing cluster nodes: %w", err)
	}

	converted := []PU{}

	items := reflect.ValueOf(list).Elem().FieldByName("Items")
	if items.IsZero() {
		return nil, fmt.Errorf("unable to get cluster node items")
	}
	for i := 0; i < items.Len(); i++ {
		item := items.Index(i).Interface()
		node, ok := item.(U)
		if !ok {
			return nil, fmt.Errorf("unable to convert cluster node item of type %T", item)
		}
		converted = append(converted, ptr.To(node))
	}

	return sortCreation(converted), nil
}
