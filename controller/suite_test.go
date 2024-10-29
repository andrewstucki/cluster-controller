package controller

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var (
	cfg         *rest.Config
	k8sClient   client.Client
	factory     *TestClusterFactory[InternalTestCluster, InternalTestClusterNode, *InternalTestCluster, *InternalTestClusterNode]
	testEnv     *envtest.Environment
	suiteCtx    context.Context
	suiteCancel context.CancelFunc
)

func TestControllers(t *testing.T) {
	RegisterFailHandler(Fail)

	RunSpecs(t, "Controller Suite")
}

var _ = BeforeSuite(func() {
	suiteCtx, suiteCancel = context.WithTimeout(context.Background(), 2*time.Minute)

	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))

	By("bootstrapping test environment")
	testEnv = &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
		BinaryAssetsDirectory: filepath.Join("..", "bin", "k8s",
			fmt.Sprintf("1.30.0-%s-%s", runtime.GOOS, runtime.GOARCH)),
	}

	var err error
	// cfg is defined in this file globally.
	cfg, err = testEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(cfg).NotTo(BeNil())

	registerCRDs(scheme.Scheme)

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	Expect(err).NotTo(HaveOccurred())
	Expect(k8sClient).NotTo(BeNil())

	Expect(createTestCRDs(suiteCtx, k8sClient)).NotTo(HaveOccurred())

	k8sManager, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: scheme.Scheme,
	})
	Expect(err).ToNot(HaveOccurred())

	factory = NewTestClusterFactory[InternalTestCluster, InternalTestClusterNode](k8sClient)
	err = New(k8sManager, factory).Testing().Setup(suiteCtx)
	Expect(err).ToNot(HaveOccurred())

	go func() {
		defer GinkgoRecover()
		err = k8sManager.Start(suiteCtx)
		Expect(err).ToNot(HaveOccurred(), "failed to run manager")
	}()
})

var _ = AfterSuite(func() {
	suiteCancel()

	By("tearing down the test environment")
	err := testEnv.Stop()
	Expect(err).NotTo(HaveOccurred())
})

var _ = Describe("Cluster controller", func() {
	It("Should manage nodes for the cluster", func() {
		ctx, cancel := context.WithTimeout(suiteCtx, 30*time.Second)
		defer cancel()

		cluster := &InternalTestCluster{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: metav1.NamespaceDefault,
				Name:      "cluster",
			},
		}

		err := factory.Client.Create(ctx, cluster)
		Expect(err).NotTo(HaveOccurred())

		nodes, err := factory.WaitForStableNodes(ctx, 10*time.Second, cluster)
		Expect(err).NotTo(HaveOccurred())

		Expect(nodes).To(HaveLen(3))

		factory.Client.Delete(ctx, nodes[0])

		nodes, err = factory.WaitForStableNodes(ctx, 10*time.Second, cluster)
		Expect(err).NotTo(HaveOccurred())

		// make sure the node gets recreated
		Expect(nodes).To(HaveLen(3))
	})
})
