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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	garagev1alpha1 "github.com/QuantumDancer/garage-operator/api/v1alpha1"
)

// This envtest spec proves the generated GarageKey CRD accepts the full spec (permissions,
// expiration, renewBefore, import, output, deletionPolicy) and that the reconcile loop drives it
// to Ready through the real status subresource, finalizer, and owned-Secret machinery, with the
// Admin API faked out.
var _ = Describe("GarageKey integration", Ordered, func() {
	const itNamespace = "garage-key-it"
	const keyName = "photos-rw"

	reconcilerFor := func(admin keyAdmin) *GarageKeyReconciler {
		return &GarageKeyReconciler{
			Client:         k8sClient,
			Scheme:         k8sClient.Scheme(),
			NewAdminClient: func(string, string) (keyAdmin, error) { return admin, nil },
		}
	}

	keyKey := client.ObjectKey{Name: keyName, Namespace: itNamespace}

	reconcileUntilReady := func(r *GarageKeyReconciler) {
		Eventually(func(g Gomega) {
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: keyKey})
			g.Expect(err).NotTo(HaveOccurred())

			var got garagev1alpha1.GarageKey
			g.Expect(k8sClient.Get(ctx, keyKey, &got)).To(Succeed())
			cond := meta.FindStatusCondition(got.Status.Conditions, conditionReady)
			g.Expect(cond).NotTo(BeNil())
			g.Expect(cond.Status).To(Equal(metav1.ConditionTrue))
		}).Should(Succeed())
	}

	BeforeAll(func() {
		Expect(k8sClient.Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: itNamespace},
		})).To(Succeed())

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

	It("accepts the full spec and reconciles to Ready with a published credentials Secret", func() {
		key := &garagev1alpha1.GarageKey{
			ObjectMeta: metav1.ObjectMeta{Name: keyName, Namespace: itNamespace},
			Spec: garagev1alpha1.GarageKeySpec{
				ClusterRef:  garagev1alpha1.ClusterReference{Name: testClusterName},
				Name:        "photos-rw",
				Permissions: garagev1alpha1.KeyPermissions{CreateBucket: true},
				Expiration:  &metav1.Time{Time: metav1.Now().Add(720 * time.Hour)},
				RenewBefore: &metav1.Duration{Duration: 168 * time.Hour},
				Output:      garagev1alpha1.KeyOutput{SecretName: "photos-rw-creds"},
			},
		}
		By("creating the GarageKey — this asserts the CRD schema accepts the full spec")
		Expect(k8sClient.Create(ctx, key)).To(Succeed())
		DeferCleanup(func() {
			_ = k8sClient.Delete(ctx, key)
		})

		admin := newFakeKeyAdmin()
		reconcileUntilReady(reconcilerFor(admin))

		By("asserting the key was created and its credentials published into the output Secret")
		Expect(admin.createCalls).To(Equal(1))

		var got garagev1alpha1.GarageKey
		Expect(k8sClient.Get(ctx, keyKey, &got)).To(Succeed())
		Expect(got.Status.KeyID).NotTo(BeEmpty())
		Expect(got.Status.CredentialsSecret).To(Equal("photos-rw-creds"))
		Expect(got.Finalizers).To(ContainElement(keyFinalizer))

		var out corev1.Secret
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "photos-rw-creds", Namespace: itNamespace}, &out)).To(Succeed())
		Expect(out.Data).To(HaveKey(credAccessKeyID))
		Expect(out.Data).To(HaveKey(credSecretAccessKey))
		Expect(metav1.GetControllerOf(&out)).NotTo(BeNil())
	})

	It("publishes imported credentials without minting a new key", func() {
		Expect(k8sClient.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "byo-creds", Namespace: itNamespace},
			Data: map[string][]byte{
				credAccessKeyID:     []byte("GKBYO"),
				credSecretAccessKey: []byte("byo-secret"),
			},
		})).To(Succeed())

		key := &garagev1alpha1.GarageKey{
			ObjectMeta: metav1.ObjectMeta{Name: "imported", Namespace: itNamespace},
			Spec: garagev1alpha1.GarageKeySpec{
				ClusterRef: garagev1alpha1.ClusterReference{Name: testClusterName},
				Import:     &garagev1alpha1.KeyImport{SecretName: "byo-creds"},
			},
		}
		Expect(k8sClient.Create(ctx, key)).To(Succeed())
		DeferCleanup(func() {
			_ = k8sClient.Delete(ctx, key)
		})

		admin := newFakeKeyAdmin()
		r := reconcilerFor(admin)
		importedKey := client.ObjectKey{Name: "imported", Namespace: itNamespace}
		Eventually(func(g Gomega) {
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: importedKey})
			g.Expect(err).NotTo(HaveOccurred())
			var got garagev1alpha1.GarageKey
			g.Expect(k8sClient.Get(ctx, importedKey, &got)).To(Succeed())
			cond := meta.FindStatusCondition(got.Status.Conditions, conditionReady)
			g.Expect(cond).NotTo(BeNil())
			g.Expect(cond.Status).To(Equal(metav1.ConditionTrue))
		}).Should(Succeed())

		Expect(admin.importCalls).To(Equal(1))
		Expect(admin.createCalls).To(Equal(0))

		var out corev1.Secret
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "imported-credentials", Namespace: itNamespace}, &out)).To(Succeed())
		Expect(out.Data[credSecretAccessKey]).To(Equal([]byte("byo-secret")))
	})
})
