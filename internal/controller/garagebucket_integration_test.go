/*
Copyright 2026.

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

package controller

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	garagev1alpha1 "github.com/QuantumDancer/garage-operator/api/v1alpha1"
)

// This envtest spec runs against a real API server. Its job is what the fake-client unit
// tests cannot do: prove the generated CRD actually accepts the full GarageBucket spec
// (CORS, lifecycle, quotas, website) and that the reconcile loop drives it to Ready through
// the genuine status subresource and finalizer machinery, with the Admin API faked out.
var _ = Describe("GarageBucket integration", Ordered, func() {
	const itNamespace = "garage-bucket-it"

	reconcilerFor := func(admin bucketAdmin) *GarageBucketReconciler {
		return &GarageBucketReconciler{
			Client:         k8sClient,
			Scheme:         k8sClient.Scheme(),
			NewAdminClient: func(string, string) (bucketAdmin, error) { return admin, nil },
			Recorder:       record.NewFakeRecorder(100),
		}
	}

	bucketKey := client.ObjectKey{Name: testBucketName, Namespace: itNamespace}

	reconcileUntilReady := func(r *GarageBucketReconciler) {
		// The first reconcile only installs the finalizer; subsequent passes converge. Drive
		// the loop a few times until Ready settles.
		Eventually(func(g Gomega) {
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: bucketKey})
			g.Expect(err).NotTo(HaveOccurred())

			var got garagev1alpha1.GarageBucket
			g.Expect(k8sClient.Get(ctx, bucketKey, &got)).To(Succeed())
			cond := meta.FindStatusCondition(got.Status.Conditions, conditionReady)
			g.Expect(cond).NotTo(BeNil())
			g.Expect(cond.Status).To(Equal(metav1.ConditionTrue))
		}).Should(Succeed())
	}

	BeforeAll(func() {
		Expect(k8sClient.Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: itNamespace},
		})).To(Succeed())

		// A Ready cluster (status set via the subresource) plus its admin-token Secret, so the
		// resolver returns a usable connection.
		cluster := &garagev1alpha1.GarageCluster{
			ObjectMeta: metav1.ObjectMeta{Name: testClusterName, Namespace: itNamespace},
			Spec:       garagev1alpha1.GarageClusterSpec{NodePools: []garagev1alpha1.NodePool{storagePool()}},
		}
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
		meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
			Type: conditionReady, Status: metav1.ConditionTrue, Reason: reasonClusterReady, Message: "ready",
		})
		Expect(k8sClient.Status().Update(ctx, cluster)).To(Succeed())

		Expect(k8sClient.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: testClusterName + "-admin-token", Namespace: itNamespace},
			Data:       map[string][]byte{secretKeyToken: []byte("test-token")},
		})).To(Succeed())
	})

	It("accepts the full spec (CORS, lifecycle, quotas, website) and reconciles to Ready", func() {
		bucket := &garagev1alpha1.GarageBucket{
			ObjectMeta: metav1.ObjectMeta{Name: testBucketName, Namespace: itNamespace},
			Spec: garagev1alpha1.GarageBucketSpec{
				ClusterRef:    garagev1alpha1.ClusterReference{Name: testClusterName},
				GlobalAliases: []string{testBucketName},
				Website: &garagev1alpha1.BucketWebsite{
					Enabled: true, IndexDocument: "index.html", ErrorDocument: "error.html",
				},
				Quotas: &garagev1alpha1.BucketQuotas{
					MaxSize:    ptr.To(resource.MustParse("50Gi")),
					MaxObjects: ptr.To(int64(100000)),
				},
				CORS: []garagev1alpha1.CORSRule{{
					ID:             "allow-web",
					AllowedOrigins: []string{"*"},
					AllowedMethods: []string{"GET", "HEAD"},
					AllowedHeaders: []string{"*"},
					MaxAgeSeconds:  ptr.To(int64(3600)),
				}},
				Lifecycle: []garagev1alpha1.LifecycleRule{{
					ID:                             "expire-tmp",
					Status:                         garagev1alpha1.LifecycleRuleEnabled,
					Filter:                         &garagev1alpha1.LifecycleFilter{Prefix: testLifecyclePrefix},
					Expiration:                     &garagev1alpha1.LifecycleExpiration{Days: ptr.To(int32(30))},
					AbortIncompleteMultipartUpload: &garagev1alpha1.AbortIncompleteMultipartUpload{DaysAfterInitiation: 7},
				}},
			},
		}
		By("creating the GarageBucket — this asserts the CRD schema accepts the full spec")
		Expect(k8sClient.Create(ctx, bucket)).To(Succeed())
		DeferCleanup(func() {
			_ = k8sClient.Delete(ctx, bucket)
		})

		admin := newFakeBucketAdmin()
		reconcileUntilReady(reconcilerFor(admin))

		By("asserting the bucket was created and its settings pushed")
		Expect(admin.createCalls).To(Equal(1))
		Expect(admin.updateCalls).To(BeNumerically(">=", 1))

		var got garagev1alpha1.GarageBucket
		Expect(k8sClient.Get(ctx, bucketKey, &got)).To(Succeed())
		Expect(got.Status.BucketID).NotTo(BeEmpty())
		Expect(got.Finalizers).To(ContainElement(bucketFinalizer))
	})
})
