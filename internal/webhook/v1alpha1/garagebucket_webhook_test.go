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

var _ = Describe("GarageBucket Webhook", func() {
	var validator GarageBucketCustomValidator

	BeforeEach(func() {
		validator = GarageBucketCustomValidator{Reader: k8sClient}
	})

	Context("When validating a GarageBucket against its cluster's referencePolicy", func() {
		It("admits a bucket whose namespace the policy allows", func() {
			bucket := bucketTargeting("media")
			Expect(validator.ValidateCreate(ctx, bucket)).Error().NotTo(HaveOccurred())
		})

		It("denies a bucket whose namespace the policy forbids", func() {
			bucket := bucketTargeting("rogue")
			Expect(validator.ValidateCreate(ctx, bucket)).Error().To(HaveOccurred())
		})

		It("admits a bucket in the cluster's own namespace", func() {
			bucket := bucketTargeting(policyClusterNS)
			Expect(validator.ValidateCreate(ctx, bucket)).Error().NotTo(HaveOccurred())
		})

		It("denies an update that repoints clusterRef into a forbidden reference", func() {
			oldBucket := bucketTargeting("rogue")
			oldBucket.Spec.ClusterRef.Name = "some-other-cluster" // a different reference
			newBucket := bucketTargeting("rogue")                 // now points at the policy cluster
			Expect(validator.ValidateUpdate(ctx, oldBucket, newBucket)).Error().To(HaveOccurred())
		})

		It("allows an update that leaves clusterRef unchanged, even from a forbidden namespace", func() {
			// Mirrors the operator removing its finalizer: a metadata-only update must never be
			// blocked, or a tightened policy would wedge the bucket in Terminating.
			bucket := bucketTargeting("rogue")
			Expect(validator.ValidateUpdate(ctx, bucket, bucket)).Error().NotTo(HaveOccurred())
		})

		It("never blocks deletion, even from a forbidden namespace", func() {
			bucket := bucketTargeting("rogue")
			Expect(validator.ValidateDelete(ctx, bucket)).Error().NotTo(HaveOccurred())
		})
	})
})

func bucketTargeting(namespace string) *garagev1alpha1.GarageBucket {
	return &garagev1alpha1.GarageBucket{
		ObjectMeta: metav1.ObjectMeta{Name: "photos", Namespace: namespace},
		Spec: garagev1alpha1.GarageBucketSpec{
			ClusterRef: garagev1alpha1.ClusterReference{Name: policyClusterName, Namespace: policyClusterNS},
		},
	}
}
