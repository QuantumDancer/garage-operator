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
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	garagev1alpha1 "github.com/QuantumDancer/garage-operator/api/v1alpha1"
)

// newTestCluster returns a fully-populated single-pool cluster as the API server would
// present it (defaults applied), so the pure builders see no zero values.
func newTestCluster() *garagev1alpha1.GarageCluster {
	return &garagev1alpha1.GarageCluster{
		ObjectMeta: metav1.ObjectMeta{Name: testClusterName, Namespace: testClusterNS},
		Spec: garagev1alpha1.GarageClusterSpec{
			Image:             garagev1alpha1.GarageImage{Repository: "dxflrs/amd64_garage", Tag: "v2.0.0"},
			DBEngine:          "lmdb",
			BlockSize:         1048576,
			ReplicationFactor: 1,
			ConsistencyMode:   "consistent",
			CompressionLevel:  1,
			NodePools: []garagev1alpha1.NodePool{{
				Name:     "default",
				Replicas: 1,
				Storage: garagev1alpha1.NodePoolStorage{
					Data: garagev1alpha1.StorageSpec{Size: resource.MustParse("1Ti"), StorageClass: ptrString("bulk")},
					Meta: garagev1alpha1.StorageSpec{Size: resource.MustParse("5Gi"), StorageClass: ptrString("fast")},
				},
			}},
			S3: garagev1alpha1.S3Config{
				Api: garagev1alpha1.S3ApiConfig{Region: "garage", RootDomain: ".s3.garage.tld"},
				Web: garagev1alpha1.S3WebConfig{RootDomain: ".web.garage.tld"},
			},
		},
	}
}

func ptrString(s string) *string { return &s }

var _ = Describe("GarageCluster resource builders", func() {
	var cluster *garagev1alpha1.GarageCluster

	BeforeEach(func() {
		cluster = newTestCluster()
	})

	Describe("renderGarageToml", func() {
		It("renders core config without embedding secrets or k8s discovery", func() {
			toml := renderGarageToml(cluster)

			Expect(toml).To(ContainSubstring(`db_engine = "lmdb"`))
			Expect(toml).To(ContainSubstring("replication_factor = 1"))
			Expect(toml).To(ContainSubstring(`consistency_mode = "consistent"`))
			Expect(toml).To(ContainSubstring("block_size = 1048576"))
			Expect(toml).To(ContainSubstring(`s3_region = "garage"`))
			Expect(toml).To(ContainSubstring(`api_bind_addr = "[::]:3900"`))
			Expect(toml).To(ContainSubstring(`api_bind_addr = "[::]:3903"`))

			// Secrets are injected via env, never written into the ConfigMap.
			Expect(toml).NotTo(ContainSubstring("rpc_secret"))
			Expect(toml).NotTo(ContainSubstring("admin_token"))
			// The operator drives the mesh; Garage's own discovery is disabled.
			Expect(toml).NotTo(ContainSubstring("kubernetes_discovery"))
		})

		It("omits the [s3_web] section when no web root domain is set (Garage requires root_domain)", func() {
			cluster.Spec.S3.Web.RootDomain = ""
			toml := renderGarageToml(cluster)
			Expect(toml).NotTo(ContainSubstring("[s3_web]"))

			cluster.Spec.S3.Web.RootDomain = ".web.example.tld"
			toml = renderGarageToml(cluster)
			Expect(toml).To(ContainSubstring("[s3_web]"))
			Expect(toml).To(ContainSubstring(`root_domain = ".web.example.tld"`))
		})

		It("defaults the s3 region when unset", func() {
			cluster.Spec.S3.Api.Region = ""
			Expect(renderGarageToml(cluster)).To(ContainSubstring(`s3_region = "garage"`))
		})

		It("omits the snapshot interval when unset and includes it when set", func() {
			Expect(renderGarageToml(cluster)).NotTo(ContainSubstring("metadata_auto_snapshot_interval"))

			cluster.Spec.MetadataAutoSnapshotInterval = "6h"
			Expect(renderGarageToml(cluster)).To(ContainSubstring(`metadata_auto_snapshot_interval = "6h"`))
		})
	})

	Describe("desiredHeadlessService", func() {
		It("is headless, publishes not-ready addresses, and exposes admin + rpc", func() {
			svc := desiredHeadlessService(cluster)

			Expect(svc.Name).To(Equal("homelab-headless"))
			Expect(svc.Spec.ClusterIP).To(Equal(corev1.ClusterIPNone))
			Expect(svc.Spec.PublishNotReadyAddresses).To(BeTrue())
			Expect(svc.Spec.Selector).To(HaveKeyWithValue(labelCluster, testClusterName))
			Expect(svc.Spec.Selector).NotTo(HaveKey(labelPool))

			ports := map[string]int32{}
			for _, p := range svc.Spec.Ports {
				ports[p.Name] = p.Port
			}
			Expect(ports).To(HaveKeyWithValue("admin", int32(portAdmin)))
			Expect(ports).To(HaveKeyWithValue("rpc", int32(portRPC)))
		})
	})

	Describe("desiredStatefulSet", func() {
		It("wires the image, secret env, volumes, and a pool-scoped selector", func() {
			ss := desiredStatefulSet(cluster, &cluster.Spec.NodePools[0])

			Expect(ss.Name).To(Equal("homelab-default"))
			Expect(ss.Spec.ServiceName).To(Equal("homelab-headless"))
			Expect(*ss.Spec.Replicas).To(Equal(int32(1)))
			Expect(ss.Spec.Selector.MatchLabels).To(HaveKeyWithValue(labelPool, "default"))

			c := ss.Spec.Template.Spec.Containers[0]
			Expect(c.Image).To(Equal("dxflrs/amd64_garage:v2.0.0"))

			env := map[string]string{}
			for _, e := range c.Env {
				if e.ValueFrom != nil && e.ValueFrom.SecretKeyRef != nil {
					env[e.Name] = e.ValueFrom.SecretKeyRef.Name
				}
			}
			Expect(env).To(HaveKeyWithValue("GARAGE_RPC_SECRET", "homelab-rpc-token"))
			Expect(env).To(HaveKeyWithValue("GARAGE_ADMIN_TOKEN", "homelab-admin-token"))

			// Readiness must be a TCP probe (not /health) to avoid the layout deadlock.
			Expect(c.ReadinessProbe.TCPSocket).NotTo(BeNil())
			Expect(c.ReadinessProbe.HTTPGet).To(BeNil())
		})

		It("falls back to the default image when spec.image is omitted", func() {
			cluster.Spec.Image = garagev1alpha1.GarageImage{}
			ss := desiredStatefulSet(cluster, &cluster.Spec.NodePools[0])
			Expect(ss.Spec.Template.Spec.Containers[0].Image).To(Equal("dxflrs/amd64_garage:v2.0.0"))
		})

		It("derives PVC templates from the pool storage spec", func() {
			ss := desiredStatefulSet(cluster, &cluster.Spec.NodePools[0])

			claims := map[string]corev1.PersistentVolumeClaim{}
			for _, pvc := range ss.Spec.VolumeClaimTemplates {
				claims[pvc.Name] = pvc
			}
			data := claims["data"]
			Expect(data.Spec.Resources.Requests.Storage().String()).To(Equal("1Ti"))
			Expect(*data.Spec.StorageClassName).To(Equal("bulk"))
			meta := claims["meta"]
			Expect(*meta.Spec.StorageClassName).To(Equal("fast"))
		})

		It("satisfies the restricted Pod Security Standard", func() {
			ss := desiredStatefulSet(cluster, &cluster.Spec.NodePools[0])
			pod := ss.Spec.Template.Spec
			c := pod.Containers[0]

			Expect(*pod.SecurityContext.RunAsNonRoot).To(BeTrue())
			Expect(*c.SecurityContext.AllowPrivilegeEscalation).To(BeFalse())
			Expect(c.SecurityContext.Capabilities.Drop).To(ContainElement(corev1.Capability("ALL")))
			Expect(c.SecurityContext.SeccompProfile.Type).To(Equal(corev1.SeccompProfileTypeRuntimeDefault))
		})
	})

	Describe("config hash annotation", func() {
		It("stamps the pod template with a config-hash annotation", func() {
			ss := desiredStatefulSet(cluster, &cluster.Spec.NodePools[0])
			Expect(ss.Spec.Template.Annotations).To(HaveKey(annotationConfigHash))
			Expect(ss.Spec.Template.Annotations[annotationConfigHash]).NotTo(BeEmpty())
		})

		It("changes the hash when rendered config changes, forcing a rolling restart", func() {
			before := desiredStatefulSet(cluster, &cluster.Spec.NodePools[0]).Spec.Template.Annotations[annotationConfigHash]

			cluster.Spec.BlockSize = 2 * 1048576
			after := desiredStatefulSet(cluster, &cluster.Spec.NodePools[0]).Spec.Template.Annotations[annotationConfigHash]

			Expect(after).NotTo(Equal(before))
		})

		It("is stable when the config is unchanged, so converged clusters do not roll", func() {
			a := desiredStatefulSet(cluster, &cluster.Spec.NodePools[0]).Spec.Template.Annotations[annotationConfigHash]
			b := desiredStatefulSet(cluster, &cluster.Spec.NodePools[0]).Spec.Template.Annotations[annotationConfigHash]
			Expect(a).To(Equal(b))
		})
	})

	Describe("resolveBootstrapSecret", func() {
		It("uses the generated Secret when mode is Generate", func() {
			ref := resolveRpcSecret(cluster)
			Expect(ref.name).To(Equal("homelab-rpc-token"))
			Expect(ref.key).To(Equal(secretKeyToken))
		})

		It("uses the user secretRef when mode is Provided", func() {
			cluster.Spec.AdminToken = garagev1alpha1.SecretBootstrap{
				Mode:      garagev1alpha1.SecretBootstrapProvided,
				SecretRef: &garagev1alpha1.SecretKeySelector{Name: "my-token", Key: "admin"},
			}
			ref := resolveAdminTokenSecret(cluster)
			Expect(ref.name).To(Equal("my-token"))
			Expect(ref.key).To(Equal("admin"))
		})
	})

	Describe("renderGarageToml stability", func() {
		It("is deterministic across calls", func() {
			Expect(renderGarageToml(cluster)).To(Equal(renderGarageToml(cluster)))
			Expect(strings.Count(renderGarageToml(cluster), "[admin]")).To(Equal(1))
		})
	})
})
