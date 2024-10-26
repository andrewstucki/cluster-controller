// +versionName=v1alpha1
// +groupName=cluster.lambda.coffee
package main

import (
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
