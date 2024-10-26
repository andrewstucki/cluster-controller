package controller

import (
	"context"
	"errors"
	"time"

	clusterv1alpha1 "github.com/andrewstucki/cluster-controller/api/v1alpha1"
	"github.com/cenkalti/backoff"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	testGroup   = "controller.test.domain"
	testVersion = "v1"
)

var (
	testGroupVersion       = schema.GroupVersion{Group: testGroup, Version: testVersion}
	defaultTestPodTemplate = &corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod",
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "container",
				Image: "bhargavshah86/kube-test:v0.1",
			}},
			RestartPolicy: corev1.RestartPolicyNever,
		},
	}

	defaultTestVolumeClaims = []*corev1.PersistentVolumeClaim{}
	defaultTestVolumes      = []*corev1.PersistentVolume{}
)

type InternalTestCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Status            clusterv1alpha1.ClusterStatus `json:"status,omitempty"`
}

type InternalTestClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []InternalTestCluster `json:"items"`
}

type InternalTestClusterNode struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Status            clusterv1alpha1.ClusterNodeStatus `json:"status,omitempty"`
}

type InternalTestClusterNodeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []InternalTestClusterNode `json:"items"`
}

func (in *InternalTestCluster) DeepCopyInto(out *InternalTestCluster) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Status.DeepCopyInto(&out.Status)
}

func (in *InternalTestCluster) DeepCopy() *InternalTestCluster {
	if in == nil {
		return nil
	}
	out := new(InternalTestCluster)
	in.DeepCopyInto(out)
	return out
}

func (in *InternalTestCluster) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (in *InternalTestClusterNode) DeepCopyInto(out *InternalTestClusterNode) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Status.DeepCopyInto(&out.Status)
}

func (in *InternalTestClusterNode) DeepCopy() *InternalTestClusterNode {
	if in == nil {
		return nil
	}
	out := new(InternalTestClusterNode)
	in.DeepCopyInto(out)
	return out
}

func (in *InternalTestClusterNode) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (in *InternalTestClusterList) DeepCopyInto(out *InternalTestClusterList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]InternalTestCluster, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

func (in *InternalTestClusterList) DeepCopy() *InternalTestClusterList {
	if in == nil {
		return nil
	}
	out := new(InternalTestClusterList)
	in.DeepCopyInto(out)
	return out
}

func (in *InternalTestClusterList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (in *InternalTestClusterNodeList) DeepCopyInto(out *InternalTestClusterNodeList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]InternalTestClusterNode, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

func (in *InternalTestClusterNodeList) DeepCopy() *InternalTestClusterNodeList {
	if in == nil {
		return nil
	}
	out := new(InternalTestClusterNodeList)
	in.DeepCopyInto(out)
	return out
}

func (in *InternalTestClusterNodeList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func testClusterCRD() *apiextensionsv1.CustomResourceDefinition {
	return &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: "clusters." + testGroup,
		},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: testGroup,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
				{
					Name:    testVersion,
					Served:  true,
					Storage: true,
					Schema: &apiextensionsv1.CustomResourceValidation{
						OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
							Type: "object",
							Properties: map[string]apiextensionsv1.JSONSchemaProps{
								"status": {
									Type:                   "object",
									XPreserveUnknownFields: ptr.To(true),
								},
							},
						},
					},
					Subresources: &apiextensionsv1.CustomResourceSubresources{
						Status: &apiextensionsv1.CustomResourceSubresourceStatus{},
					},
				},
			},
			Scope: apiextensionsv1.NamespaceScoped,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural:   "clusters",
				Singular: "cluster",
				ListKind: "InternalTestClusterList",
				Kind:     "InternalTestCluster",
			},
		},
	}
}

func testClusterNodeCRD() *apiextensionsv1.CustomResourceDefinition {
	return &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: "nodes." + testGroup,
		},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: testGroup,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
				{
					Name:    testVersion,
					Served:  true,
					Storage: true,
					Schema: &apiextensionsv1.CustomResourceValidation{
						OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
							Type: "object",
							Properties: map[string]apiextensionsv1.JSONSchemaProps{
								"status": {
									Type:                   "object",
									XPreserveUnknownFields: ptr.To(true),
								},
							},
						},
					},
					Subresources: &apiextensionsv1.CustomResourceSubresources{
						Status: &apiextensionsv1.CustomResourceSubresourceStatus{},
					},
				},
			},
			Scope: apiextensionsv1.NamespaceScoped,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural:   "nodes",
				Singular: "node",
				ListKind: "InternalTestClusterNodeList",
				Kind:     "InternalTestClusterNode",
			},
		},
	}
}

func createCRD(ctx context.Context, cl client.Client, crd *apiextensionsv1.CustomResourceDefinition) error {
	if err := cl.Create(ctx, crd); err != nil {
		return err
	}

	retryFor := backoff.WithMaxRetries(backoff.NewConstantBackOff(1*time.Second), 60)
	return backoff.Retry(func() error {
		if err := cl.Get(ctx, client.ObjectKeyFromObject(crd), crd); err != nil {
			return err
		}
		for _, condition := range crd.Status.Conditions {
			if condition.Type == apiextensionsv1.Established && condition.Status == apiextensionsv1.ConditionTrue {
				return nil
			}
		}
		return errors.New("CRD not established")
	}, backoff.WithContext(retryFor, ctx))
}

func createTestCRDs(ctx context.Context, cl client.Client) error {
	for _, crd := range []*apiextensionsv1.CustomResourceDefinition{
		testClusterCRD(),
		testClusterNodeCRD(),
	} {
		if err := createCRD(ctx, cl, crd); err != nil {
			return err
		}
	}
	return nil
}

func registerCRDs(s *runtime.Scheme) {
	s.AddKnownTypes(testGroupVersion, &InternalTestCluster{}, &InternalTestClusterList{})
	s.AddKnownTypes(testGroupVersion, &InternalTestClusterNode{}, &InternalTestClusterNodeList{})
	metav1.AddToGroupVersion(s, testGroupVersion)
}

func clusterNodeCustomizer(fns ...func(node *TestClusterNode[InternalTestClusterNode, *InternalTestClusterNode])) func(node *TestClusterNode[InternalTestClusterNode, *InternalTestClusterNode]) {
	return func(node *TestClusterNode[InternalTestClusterNode, *InternalTestClusterNode]) {
		node.SetHashClusterNodeResponse("static", nil)
		node.SetClusterNodePodSpec(defaultTestPodTemplate)
		node.SetClusterNodeVolumeClaims(defaultTestVolumeClaims)
		node.SetClusterNodeVolumes(defaultTestVolumes)

		for _, fn := range fns {
			fn(node)
		}
	}
}

func clusterCustomizer(replicas, minReplicas int, fns ...func(cluster *TestCluster[InternalTestClusterNode, *InternalTestClusterNode])) func(cluster *TestCluster[InternalTestClusterNode, *InternalTestClusterNode]) {
	return func(cluster *TestCluster[InternalTestClusterNode, *InternalTestClusterNode]) {
		cluster.SetReplicas(replicas)
		cluster.SetMinimumHealthyReplicas(minReplicas)
		cluster.SetHashClusterResponse("static", nil)

		for _, fn := range fns {
			fn(cluster)
		}
	}
}