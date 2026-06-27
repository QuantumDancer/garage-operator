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

package v1alpha1

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	garagev1alpha1 "github.com/QuantumDancer/garage-operator/api/v1alpha1"
)

var _ = Describe("GarageKey Webhook", func() {
	var validator GarageKeyCustomValidator

	BeforeEach(func() {
		validator = GarageKeyCustomValidator{Reader: k8sClient}
	})

	Context("When validating a GarageKey against its cluster's referencePolicy", func() {
		It("admits a key whose namespace the policy allows", func() {
			key := keyTargeting("media")
			Expect(validator.ValidateCreate(ctx, key)).Error().NotTo(HaveOccurred())
		})

		It("denies a key whose namespace the policy forbids", func() {
			key := keyTargeting("rogue")
			Expect(validator.ValidateCreate(ctx, key)).Error().To(HaveOccurred())
		})

		It("admits a key in the cluster's own namespace", func() {
			key := keyTargeting(policyClusterNS)
			Expect(validator.ValidateCreate(ctx, key)).Error().NotTo(HaveOccurred())
		})

		It("denies an update that repoints clusterRef into a forbidden reference", func() {
			oldKey := keyTargeting("rogue")
			oldKey.Spec.ClusterRef.Name = "some-other-cluster"
			newKey := keyTargeting("rogue")
			Expect(validator.ValidateUpdate(ctx, oldKey, newKey)).Error().To(HaveOccurred())
		})

		It("allows an update that leaves clusterRef unchanged, even from a forbidden namespace", func() {
			// Mirrors the operator removing its finalizer: a metadata-only update must never be
			// blocked, or a tightened policy would wedge the key in Terminating.
			key := keyTargeting("rogue")
			Expect(validator.ValidateUpdate(ctx, key, key)).Error().NotTo(HaveOccurred())
		})

		It("never blocks deletion, even from a forbidden namespace", func() {
			key := keyTargeting("rogue")
			Expect(validator.ValidateDelete(ctx, key)).Error().NotTo(HaveOccurred())
		})
	})
})

func keyTargeting(namespace string) *garagev1alpha1.GarageKey {
	return &garagev1alpha1.GarageKey{
		ObjectMeta: metav1.ObjectMeta{Name: "photos-rw", Namespace: namespace},
		Spec: garagev1alpha1.GarageKeySpec{
			ClusterRef: garagev1alpha1.ClusterReference{Name: policyClusterName, Namespace: policyClusterNS},
		},
	}
}
