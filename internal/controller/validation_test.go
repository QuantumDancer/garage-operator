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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	garagev1alpha1 "github.com/QuantumDancer/garage-operator/api/v1alpha1"
)

// These specs run against the envtest API server, which evaluates the CEL
// (x-kubernetes-validations) rules baked into the CRDs exactly as a real cluster would. Their
// job is to prove each Phase 6 validation rule is in force: the API server itself accepts a
// valid object and rejects an invalid one, with no admission webhook in the loop.
var _ = Describe("CRD validation rules", Ordered, func() {
	const valNS = "garage-validation-it"

	// uniqueName keeps successive Create calls from colliding within the shared namespace.
	var counter int
	uniqueName := func() string {
		counter++
		return "v" + time.Now().Format("150405") + "-" + string(rune('a'+counter%26))
	}

	validationCluster := func() *garagev1alpha1.GarageCluster {
		return &garagev1alpha1.GarageCluster{
			ObjectMeta: metav1.ObjectMeta{Name: uniqueName(), Namespace: valNS},
			Spec: garagev1alpha1.GarageClusterSpec{
				NodePools: []garagev1alpha1.NodePool{storagePool()},
			},
		}
	}

	validationBucket := func() *garagev1alpha1.GarageBucket {
		return &garagev1alpha1.GarageBucket{
			ObjectMeta: metav1.ObjectMeta{Name: uniqueName(), Namespace: valNS},
			Spec: garagev1alpha1.GarageBucketSpec{
				ClusterRef: garagev1alpha1.ClusterReference{Name: "some-cluster"},
			},
		}
	}

	validationKey := func() *garagev1alpha1.GarageKey {
		return &garagev1alpha1.GarageKey{
			ObjectMeta: metav1.ObjectMeta{Name: uniqueName(), Namespace: valNS},
			Spec: garagev1alpha1.GarageKeySpec{
				ClusterRef: garagev1alpha1.ClusterReference{Name: "some-cluster"},
			},
		}
	}

	BeforeAll(func() {
		Expect(k8sClient.Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: valNS},
		})).To(Succeed())
	})

	Describe("GarageCluster", func() {
		It("rejects a change to the immutable replicationFactor", func() {
			c := validationCluster()
			c.Spec.ReplicationFactor = 3
			Expect(k8sClient.Create(ctx, c)).To(Succeed())

			c.Spec.ReplicationFactor = 2
			err := k8sClient.Update(ctx, c)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("replicationFactor is immutable"))
		})

		It("rejects a change to the immutable dbEngine", func() {
			c := validationCluster()
			c.Spec.DBEngine = garagev1alpha1.GarageDBEngine("lmdb")
			Expect(k8sClient.Create(ctx, c)).To(Succeed())

			c.Spec.DBEngine = garagev1alpha1.GarageDBEngine("sqlite")
			err := k8sClient.Update(ctx, c)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("dbEngine is immutable"))
		})

		It("rejects adminToken mode Provided without secretRef", func() {
			c := validationCluster()
			c.Spec.AdminToken = garagev1alpha1.SecretBootstrap{
				Mode: garagev1alpha1.SecretBootstrapProvided,
			}
			err := k8sClient.Create(ctx, c)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("secretRef is required when mode is Provided"))
		})

		It("rejects rpcSecret mode Provided without secretRef", func() {
			c := validationCluster()
			c.Spec.RpcSecret = garagev1alpha1.SecretBootstrap{
				Mode: garagev1alpha1.SecretBootstrapProvided,
			}
			err := k8sClient.Create(ctx, c)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("secretRef is required when mode is Provided"))
		})

		It("accepts mode Provided with a secretRef", func() {
			c := validationCluster()
			c.Spec.AdminToken = garagev1alpha1.SecretBootstrap{
				Mode:      garagev1alpha1.SecretBootstrapProvided,
				SecretRef: &garagev1alpha1.SecretKeySelector{Name: "tok", Key: "token"},
			}
			Expect(k8sClient.Create(ctx, c)).To(Succeed())
		})

		It("rejects zoneRedundancy AtLeast without atLeast", func() {
			c := validationCluster()
			c.Spec.ZoneRedundancy = &garagev1alpha1.ZoneRedundancy{
				Mode: garagev1alpha1.ZoneRedundancyAtLeast,
			}
			err := k8sClient.Create(ctx, c)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("atLeast must be set"))
		})

		It("accepts zoneRedundancy AtLeast with atLeast", func() {
			c := validationCluster()
			c.Spec.ZoneRedundancy = &garagev1alpha1.ZoneRedundancy{
				Mode:    garagev1alpha1.ZoneRedundancyAtLeast,
				AtLeast: 2,
			}
			Expect(k8sClient.Create(ctx, c)).To(Succeed())
		})

		It("accepts zoneRedundancy Maximum without atLeast", func() {
			c := validationCluster()
			c.Spec.ZoneRedundancy = &garagev1alpha1.ZoneRedundancy{
				Mode: garagev1alpha1.ZoneRedundancyMaximum,
			}
			Expect(k8sClient.Create(ctx, c)).To(Succeed())
		})

		It("accepts metrics with a valid Prometheus duration interval", func() {
			c := validationCluster()
			c.Spec.Metrics = garagev1alpha1.MetricsConfig{
				Enabled:  true,
				Interval: "30s",
				Labels:   map[string]string{"release": testPrometheusLabel},
			}
			Expect(k8sClient.Create(ctx, c)).To(Succeed())
		})

		It("rejects metrics with a malformed interval", func() {
			c := validationCluster()
			c.Spec.Metrics = garagev1alpha1.MetricsConfig{Enabled: true, Interval: "30 seconds"}
			err := k8sClient.Create(ctx, c)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("spec.metrics.interval"))
		})
	})

	Describe("GarageBucket", func() {
		It("rejects duplicate globalAliases", func() {
			b := validationBucket()
			b.Spec.GlobalAliases = []string{testBucketName, testBucketName}
			err := k8sClient.Create(ctx, b)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Duplicate value"))
		})

		It("accepts distinct globalAliases", func() {
			b := validationBucket()
			b.Spec.GlobalAliases = []string{testBucketName, "images"}
			Expect(k8sClient.Create(ctx, b)).To(Succeed())
		})

		It("rejects a lifecycle expiration with both days and date", func() {
			b := validationBucket()
			b.Spec.Lifecycle = []garagev1alpha1.LifecycleRule{{
				Status: garagev1alpha1.LifecycleRuleEnabled,
				Expiration: &garagev1alpha1.LifecycleExpiration{
					Days: ptr.To(int32(30)),
					Date: "2026-01-01",
				},
			}}
			err := k8sClient.Create(ctx, b)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("exactly one of days or date"))
		})

		It("rejects a lifecycle expiration with neither days nor date", func() {
			b := validationBucket()
			b.Spec.Lifecycle = []garagev1alpha1.LifecycleRule{{
				Status:     garagev1alpha1.LifecycleRuleEnabled,
				Expiration: &garagev1alpha1.LifecycleExpiration{},
			}}
			err := k8sClient.Create(ctx, b)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("exactly one of days or date"))
		})

		It("rejects a lifecycle expiration whose date is an explicit empty string", func() {
			// The typed client's omitempty would strip Date:"" before it reached the API
			// server, so build the object unstructured to send date present-but-empty — the
			// exact shape a raw `kubectl apply` would produce and the case the rule guards.
			u := &unstructured.Unstructured{}
			u.SetGroupVersionKind(garagev1alpha1.GroupVersion.WithKind("GarageBucket"))
			u.SetName(uniqueName())
			u.SetNamespace(valNS)
			Expect(unstructured.SetNestedField(u.Object, "some-cluster", "spec", "clusterRef", "name")).To(Succeed())
			Expect(unstructured.SetNestedSlice(u.Object, []any{
				map[string]any{
					"status":     "Enabled",
					"expiration": map[string]any{"date": ""},
				},
			}, "spec", "lifecycle")).To(Succeed())

			err := k8sClient.Create(ctx, u)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("exactly one of days or date"))
		})

		It("rejects a lifecycle rule with no action", func() {
			b := validationBucket()
			b.Spec.Lifecycle = []garagev1alpha1.LifecycleRule{{
				Status: garagev1alpha1.LifecycleRuleEnabled,
				Filter: &garagev1alpha1.LifecycleFilter{Prefix: testLifecyclePrefix},
			}}
			err := k8sClient.Create(ctx, b)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("must set expiration or abortIncompleteMultipartUpload"))
		})

		It("rejects a filter whose greaterThan is not below lessThan", func() {
			b := validationBucket()
			b.Spec.Lifecycle = []garagev1alpha1.LifecycleRule{{
				Status: garagev1alpha1.LifecycleRuleEnabled,
				Filter: &garagev1alpha1.LifecycleFilter{
					ObjectSizeGreaterThan: ptr.To(int64(1024)),
					ObjectSizeLessThan:    ptr.To(int64(1024)),
				},
				Expiration: &garagev1alpha1.LifecycleExpiration{Days: ptr.To(int32(30))},
			}}
			err := k8sClient.Create(ctx, b)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("objectSizeGreaterThan must be less than objectSizeLessThan"))
		})

		It("accepts a fully valid lifecycle rule", func() {
			b := validationBucket()
			b.Spec.Lifecycle = []garagev1alpha1.LifecycleRule{{
				Status: garagev1alpha1.LifecycleRuleEnabled,
				Filter: &garagev1alpha1.LifecycleFilter{
					Prefix:                testLifecyclePrefix,
					ObjectSizeGreaterThan: ptr.To(int64(1024)),
					ObjectSizeLessThan:    ptr.To(int64(1048576)),
				},
				Expiration: &garagev1alpha1.LifecycleExpiration{Days: ptr.To(int32(30))},
			}}
			Expect(k8sClient.Create(ctx, b)).To(Succeed())
		})
	})

	Describe("GarageKey", func() {
		It("rejects renewBefore without expiration", func() {
			k := validationKey()
			k.Spec.RenewBefore = &metav1.Duration{Duration: 168 * time.Hour}
			err := k8sClient.Create(ctx, k)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("renewBefore requires expiration"))
		})

		It("accepts renewBefore with expiration", func() {
			k := validationKey()
			k.Spec.Expiration = &metav1.Time{Time: time.Now().Add(720 * time.Hour)}
			k.Spec.RenewBefore = &metav1.Duration{Duration: 168 * time.Hour}
			Expect(k8sClient.Create(ctx, k)).To(Succeed())
		})

		It("accepts a key with neither expiration nor renewBefore", func() {
			Expect(k8sClient.Create(ctx, validationKey())).To(Succeed())
		})
	})

	AfterAll(func() {
		// Best-effort cleanup; the suite tears down envtest regardless.
		_ = k8sClient.DeleteAllOf(ctx, &garagev1alpha1.GarageCluster{}, client.InNamespace(valNS))
		_ = k8sClient.DeleteAllOf(ctx, &garagev1alpha1.GarageBucket{}, client.InNamespace(valNS))
		_ = k8sClient.DeleteAllOf(ctx, &garagev1alpha1.GarageKey{}, client.InNamespace(valNS))
	})
})
