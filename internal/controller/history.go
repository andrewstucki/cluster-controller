package controller

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash"
	"hash/fnv"
	"sort"
	"strconv"

	clusterv1alpha1 "github.com/andrewstucki/cluster-controller/api/v1alpha1"
	"github.com/davecgh/go-spew/spew"
	appsv1 "k8s.io/api/apps/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/rand"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const revisionLabel = "controller-revision"

func sortControllerRevisions(revisions []*appsv1.ControllerRevision) {
	sort.SliceStable(revisions, func(i, j int) bool {
		if revisions[i].Revision == revisions[j].Revision {
			if revisions[j].CreationTimestamp.Equal(&revisions[i].CreationTimestamp) {
				return revisions[i].Name < revisions[j].Name
			}
			return revisions[j].CreationTimestamp.After(revisions[i].CreationTimestamp.Time)
		}
		return revisions[i].Revision < revisions[j].Revision
	})
}

func equalRevision(lhs *appsv1.ControllerRevision, rhs *appsv1.ControllerRevision) bool {
	var lhsHash, rhsHash *uint32
	if lhs == nil || rhs == nil {
		return lhs == rhs
	}
	if hs, found := lhs.Labels[revisionLabel]; found {
		hash, err := strconv.ParseInt(hs, 10, 32)
		if err == nil {
			lhsHash = new(uint32)
			*lhsHash = uint32(hash)
		}
	}
	if hs, found := rhs.Labels[revisionLabel]; found {
		hash, err := strconv.ParseInt(hs, 10, 32)
		if err == nil {
			rhsHash = new(uint32)
			*rhsHash = uint32(hash)
		}
	}
	if lhsHash != nil && rhsHash != nil && *lhsHash != *rhsHash {
		return false
	}
	return bytes.Equal(lhs.Data.Raw, rhs.Data.Raw) && apiequality.Semantic.DeepEqual(lhs.Data.Object, rhs.Data.Object)
}

func findEqualRevisions(revisions []*appsv1.ControllerRevision, needle *appsv1.ControllerRevision) []*appsv1.ControllerRevision {
	var eq []*appsv1.ControllerRevision
	for i := range revisions {
		if equalRevision(revisions[i], needle) {
			eq = append(eq, revisions[i])
		}
	}
	return eq
}

func newControllerRevision(parent metav1.Object,
	parentKind schema.GroupVersionKind,
	templateLabels map[string]string,
	data runtime.RawExtension,
	revision int64,
	collisionCount *int32) (*appsv1.ControllerRevision, error) {
	labelMap := make(map[string]string)
	for k, v := range templateLabels {
		labelMap[k] = v
	}
	cr := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Labels:          labelMap,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(parent, parentKind)},
		},
		Data:     data,
		Revision: revision,
	}
	hash := hashControllerRevision(cr, collisionCount)
	cr.Name = controllerRevisionName(parent.GetName(), hash)
	cr.Labels[revisionLabel] = hash
	return cr, nil
}

func controllerRevisionName(prefix string, hash string) string {
	if len(prefix) > 223 {
		prefix = prefix[:223]
	}

	return fmt.Sprintf("%s-%s", prefix, hash)
}

func deepHashObject(hasher hash.Hash, objectToWrite interface{}) {
	hasher.Reset()
	printer := spew.ConfigState{
		Indent:         " ",
		SortKeys:       true,
		DisableMethods: true,
		SpewKeys:       true,
	}
	printer.Fprintf(hasher, "%#v", objectToWrite)
}

func hashControllerRevision(revision *appsv1.ControllerRevision, probe *int32) string {
	hf := fnv.New32()
	if len(revision.Data.Raw) > 0 {
		hf.Write(revision.Data.Raw)
	}
	if revision.Data.Object != nil {
		deepHashObject(hf, revision.Data.Object)
	}
	if probe != nil {
		hf.Write([]byte(strconv.FormatInt(int64(*probe), 10)))
	}
	return rand.SafeEncodeString(fmt.Sprint(hf.Sum32()))
}

func newRevision(cluster client.Object, spec *clusterv1alpha1.ReplicatedPodSpec, revision int64, collisionCount *int32) (*appsv1.ControllerRevision, error) {
	data, err := json.Marshal(spec)
	if err != nil {
		return nil, err
	}

	return newControllerRevision(cluster,
		cluster.GetObjectKind().GroupVersionKind(),
		nil,
		runtime.RawExtension{Raw: data},
		revision,
		collisionCount,
	)
}

func nextRevision(revisions []*appsv1.ControllerRevision) int64 {
	count := len(revisions)
	if count <= 0 {
		return 1
	}
	return revisions[count-1].Revision + 1
}

func revisionToSpec(revision *appsv1.ControllerRevision) (*clusterv1alpha1.ReplicatedPodSpec, error) {
	var spec clusterv1alpha1.ReplicatedPodSpec
	if err := json.Unmarshal(revision.Data.Raw, &spec); err != nil {
		return nil, err
	}
	return &spec, nil
}
