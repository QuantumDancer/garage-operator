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
	"fmt"
	"maps"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	garagev1alpha1 "github.com/QuantumDancer/garage-operator/api/v1alpha1"
)

// Fixed Garage network ports. Hard-coded for now (see cluster.yaml); a future spec field
// could expose them. The admin port is reachable only over per-pod headless DNS.
const (
	portS3    = 3900
	portRPC   = 3901
	portWeb   = 3902
	portAdmin = 3903
)

// Container filesystem paths for the Garage volumes and config.
const (
	metaMountPath   = "/mnt/meta"
	dataMountPath   = "/mnt/data"
	configMountPath = "/etc/garage.toml"
	configFileKey   = "garage.toml"
)

// Label keys identifying the cluster and pool a resource belongs to. The pool label is
// what lets the operator (and pod anti-affinity) distinguish StatefulSets within one cluster.
const (
	labelCluster = "garage.rottler.io/cluster"
	labelPool    = "garage.rottler.io/pool"

	labelAppName     = "app.kubernetes.io/name"
	labelAppInstance = "app.kubernetes.io/instance"
	labelManagedBy   = "app.kubernetes.io/managed-by"

	appName = "garage"
)

// Named container/Service ports.
const (
	portNameS3    = "s3-api"
	portNameRPC   = "rpc"
	portNameWeb   = "web"
	portNameAdmin = "admin"
)

// secretKeyToken is the key under which generated admin-token / RPC secrets are stored.
const secretKeyToken = "token"

// Default Garage image, applied by the operator when spec.image is omitted. CRD field
// defaults do not descend into an omitted parent object, so the controller defaults here to
// honour "defaulted by the operator if omitted".
const (
	defaultImageRepository = "dxflrs/amd64_garage"
	defaultImageTag        = "v2.0.0"
)

// defaultS3Region is used when spec.s3.api.region is omitted. Garage's [s3_api] section
// requires a region.
const defaultS3Region = "garage"

// garageImage returns the fully-qualified image reference, falling back to the defaults when
// the repository or tag are unset so a malformed "repo:" / ":tag" reference can never reach a Pod.
func garageImage(c *garagev1alpha1.GarageCluster) string {
	repo := c.Spec.Image.Repository
	if repo == "" {
		repo = defaultImageRepository
	}
	tag := c.Spec.Image.Tag
	if tag == "" {
		tag = defaultImageTag
	}
	return repo + ":" + tag
}

// secretRef is a resolved (name, key) pointer into a Secret.
type secretRef struct {
	name string
	key  string
}

func headlessServiceName(c *garagev1alpha1.GarageCluster) string { return c.Name + "-headless" }
func s3ServiceName(c *garagev1alpha1.GarageCluster) string       { return c.Name + "-s3" }
func webServiceName(c *garagev1alpha1.GarageCluster) string      { return c.Name + "-web" }
func configMapName(c *garagev1alpha1.GarageCluster) string       { return c.Name + "-config" }

func statefulSetName(c *garagev1alpha1.GarageCluster, pool *garagev1alpha1.NodePool) string {
	return c.Name + "-" + pool.Name
}

// resolveAdminTokenSecret returns the Secret reference the controller mounts for the admin
// token: the user-provided secretRef when mode is Provided, otherwise the operator-managed
// Secret (named "<cluster>-admin-token", key "token").
func resolveAdminTokenSecret(c *garagev1alpha1.GarageCluster) secretRef {
	return resolveBootstrapSecret(c.Spec.AdminToken, c.Name+"-admin-token")
}

// resolveRpcSecret mirrors resolveAdminTokenSecret for the inter-node RPC secret
// ("<cluster>-rpc-token", key "token").
func resolveRpcSecret(c *garagev1alpha1.GarageCluster) secretRef {
	return resolveBootstrapSecret(c.Spec.RpcSecret, c.Name+"-rpc-token")
}

func resolveBootstrapSecret(b garagev1alpha1.SecretBootstrap, generatedName string) secretRef {
	if b.Mode == garagev1alpha1.SecretBootstrapProvided && b.SecretRef != nil {
		return secretRef{name: b.SecretRef.Name, key: b.SecretRef.Key}
	}
	return secretRef{name: generatedName, key: secretKeyToken}
}

// clusterLabels are applied to every child resource.
func clusterLabels(c *garagev1alpha1.GarageCluster) map[string]string {
	return map[string]string{
		labelAppName:     appName,
		labelAppInstance: c.Name,
		labelManagedBy:   "garage-operator",
		labelCluster:     c.Name,
	}
}

// clusterSelectorLabels select all pods in the cluster regardless of pool — used by the
// headless Service (the RPC mesh and admin DNS span every pool).
func clusterSelectorLabels(c *garagev1alpha1.GarageCluster) map[string]string {
	return map[string]string{
		labelAppInstance: c.Name,
		labelCluster:     c.Name,
	}
}

// poolSelectorLabels select the pods of a single pool — used as a StatefulSet selector.
func poolSelectorLabels(c *garagev1alpha1.GarageCluster, poolName string) map[string]string {
	return map[string]string{
		labelAppInstance: c.Name,
		labelCluster:     c.Name,
		labelPool:        poolName,
	}
}

func mergeLabels(base, extra map[string]string) map[string]string {
	out := make(map[string]string, len(base)+len(extra))
	maps.Copy(out, base)
	maps.Copy(out, extra)
	return out
}

// renderGarageToml renders the non-secret garage.toml. Secrets (rpc_secret, admin_token)
// are intentionally absent: they are injected at runtime via the GARAGE_RPC_SECRET and
// GARAGE_ADMIN_TOKEN environment variables, keeping them out of the ConfigMap. Garage's
// own Kubernetes discovery is also omitted — the operator drives the peer mesh and layout
// through the Admin API, so the [kubernetes_discovery] block (and its cluster-wide RBAC)
// is not needed.
func renderGarageToml(c *garagev1alpha1.GarageCluster) string {
	var b strings.Builder

	fmt.Fprintf(&b, "metadata_dir = %q\n", metaMountPath)
	fmt.Fprintf(&b, "data_dir = %q\n", dataMountPath)
	fmt.Fprintf(&b, "db_engine = %q\n\n", c.Spec.DBEngine)

	fmt.Fprintf(&b, "block_size = %d\n", c.Spec.BlockSize)
	fmt.Fprintf(&b, "replication_factor = %d\n", c.Spec.ReplicationFactor)
	fmt.Fprintf(&b, "consistency_mode = %q\n", c.Spec.ConsistencyMode)
	fmt.Fprintf(&b, "compression_level = %d\n", c.Spec.CompressionLevel)
	if c.Spec.MetadataAutoSnapshotInterval != "" {
		fmt.Fprintf(&b, "metadata_auto_snapshot_interval = %q\n", c.Spec.MetadataAutoSnapshotInterval)
	}

	fmt.Fprintf(&b, "\nrpc_bind_addr = \"[::]:%d\"\n", portRPC)

	region := c.Spec.S3.Api.Region
	if region == "" {
		region = defaultS3Region
	}
	fmt.Fprintf(&b, "\n[s3_api]\n")
	fmt.Fprintf(&b, "s3_region = %q\n", region)
	fmt.Fprintf(&b, "api_bind_addr = \"[::]:%d\"\n", portS3)
	if c.Spec.S3.Api.RootDomain != "" {
		fmt.Fprintf(&b, "root_domain = %q\n", c.Spec.S3.Api.RootDomain)
	}

	// Garage's [s3_web] section mandates root_domain, so emit the whole section only when
	// website hosting is actually configured; otherwise Garage refuses to start.
	if c.Spec.S3.Web.RootDomain != "" {
		fmt.Fprintf(&b, "\n[s3_web]\n")
		fmt.Fprintf(&b, "bind_addr = \"[::]:%d\"\n", portWeb)
		fmt.Fprintf(&b, "root_domain = %q\n", c.Spec.S3.Web.RootDomain)
	}

	fmt.Fprintf(&b, "\n[admin]\n")
	fmt.Fprintf(&b, "api_bind_addr = \"[::]:%d\"\n", portAdmin)

	return b.String()
}

func desiredConfigMap(c *garagev1alpha1.GarageCluster) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName(c),
			Namespace: c.Namespace,
			Labels:    clusterLabels(c),
		},
		Data: map[string]string{configFileKey: renderGarageToml(c)},
	}
}

// desiredGeneratedSecret builds the operator-managed Secret for a generated token value.
// It is created only when absent and never overwritten, so the value persists.
func desiredGeneratedSecret(c *garagev1alpha1.GarageCluster, name, value string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: c.Namespace,
			Labels:    clusterLabels(c),
		},
		Type:       corev1.SecretTypeOpaque,
		StringData: map[string]string{secretKeyToken: value},
	}
}

// desiredHeadlessService is the headless Service that gives every pod stable DNS for both
// the RPC mesh and the operator's admin-API access. publishNotReadyAddresses keeps pod DNS
// resolvable during startup and rolling restarts so the mesh can re-form.
func desiredHeadlessService(c *garagev1alpha1.GarageCluster) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      headlessServiceName(c),
			Namespace: c.Namespace,
			Labels:    clusterLabels(c),
		},
		Spec: corev1.ServiceSpec{
			ClusterIP:                corev1.ClusterIPNone,
			PublishNotReadyAddresses: true,
			Selector:                 clusterSelectorLabels(c),
			Ports: []corev1.ServicePort{
				{Name: portNameRPC, Port: portRPC, TargetPort: intstr.FromInt(portRPC)},
				{Name: portNameAdmin, Port: portAdmin, TargetPort: intstr.FromInt(portAdmin)},
				{Name: portNameS3, Port: portS3, TargetPort: intstr.FromInt(portS3)},
				{Name: portNameWeb, Port: portWeb, TargetPort: intstr.FromInt(portWeb)},
			},
		},
	}
}

func desiredClientService(c *garagev1alpha1.GarageCluster, name, portName string, port int32, cfg garagev1alpha1.ServiceConfig) *corev1.Service {
	svcType := cfg.Type
	if svcType == "" {
		svcType = corev1.ServiceTypeClusterIP
	}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: c.Namespace,
			Labels:    clusterLabels(c),
		},
		Spec: corev1.ServiceSpec{
			Type:     svcType,
			Selector: clusterSelectorLabels(c),
			Ports: []corev1.ServicePort{
				{Name: portName, Port: port, TargetPort: intstr.FromInt(int(port))},
			},
		},
	}
}

func desiredS3Service(c *garagev1alpha1.GarageCluster) *corev1.Service {
	return desiredClientService(c, s3ServiceName(c), "s3-api", portS3, c.Spec.Services.S3Api)
}

func desiredWebService(c *garagev1alpha1.GarageCluster) *corev1.Service {
	return desiredClientService(c, webServiceName(c), "web", portWeb, c.Spec.Services.Web)
}

// desiredStatefulSet builds the StatefulSet for a single node pool.
func desiredStatefulSet(c *garagev1alpha1.GarageCluster, pool *garagev1alpha1.NodePool) *appsv1.StatefulSet {
	selector := poolSelectorLabels(c, pool.Name)
	podLabels := mergeLabels(clusterLabels(c), map[string]string{labelPool: pool.Name})
	admin := resolveAdminTokenSecret(c)
	rpc := resolveRpcSecret(c)
	image := garageImage(c)

	container := corev1.Container{
		Name:            "garage",
		Image:           image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Ports: []corev1.ContainerPort{
			{Name: portNameS3, ContainerPort: portS3},
			{Name: portNameRPC, ContainerPort: portRPC},
			{Name: portNameWeb, ContainerPort: portWeb},
			{Name: portNameAdmin, ContainerPort: portAdmin},
		},
		Env: []corev1.EnvVar{
			{Name: "GARAGE_RPC_SECRET", ValueFrom: secretEnvSource(rpc)},
			{Name: "GARAGE_ADMIN_TOKEN", ValueFrom: secretEnvSource(admin)},
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "meta", MountPath: metaMountPath},
			{Name: "data", MountPath: dataMountPath},
			{Name: "config", MountPath: configMountPath, SubPath: configFileKey, ReadOnly: true},
		},
		// Readiness/liveness use a TCP probe on the admin port rather than the admin
		// /health endpoint on purpose: a fresh node has no layout role yet, so /health
		// reports unhealthy until the operator applies the layout — but the operator only
		// applies the layout once pods are Ready. A TCP probe breaks that chicken-and-egg
		// by reporting Ready as soon as the admin API is listening.
		ReadinessProbe: tcpProbe(portAdmin, 5, 10),
		LivenessProbe:  tcpProbe(portAdmin, 15, 20),
		Resources:      pool.Resources,
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: ptr.To(false),
			RunAsNonRoot:             ptr.To(true),
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
			SeccompProfile:           &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
		},
	}

	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      statefulSetName(c, pool),
			Namespace: c.Namespace,
			Labels:    podLabels,
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName:         headlessServiceName(c),
			Replicas:            ptr.To(pool.Replicas),
			PodManagementPolicy: appsv1.ParallelPodManagement,
			Selector:            &metav1.LabelSelector{MatchLabels: selector},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: podLabels},
				Spec: corev1.PodSpec{
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot:   ptr.To(true),
						RunAsUser:      ptr.To(int64(1000)),
						RunAsGroup:     ptr.To(int64(1000)),
						FSGroup:        ptr.To(int64(1000)),
						SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
					},
					Containers:   []corev1.Container{container},
					NodeSelector: pool.NodeSelector,
					Tolerations:  pool.Tolerations,
					Affinity:     pool.Affinity,
					Volumes: []corev1.Volume{
						{
							Name: "config",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{Name: configMapName(c)},
								},
							},
						},
					},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				volumeClaim("meta", pool.Storage.Meta),
				volumeClaim("data", pool.Storage.Data),
			},
		},
	}
}

func secretEnvSource(ref secretRef) *corev1.EnvVarSource {
	return &corev1.EnvVarSource{
		SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: ref.name},
			Key:                  ref.key,
		},
	}
}

func tcpProbe(port int, initialDelay, period int32) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt(port)},
		},
		InitialDelaySeconds: initialDelay,
		PeriodSeconds:       period,
	}
}

func volumeClaim(name string, spec garagev1alpha1.StorageSpec) corev1.PersistentVolumeClaim {
	pvc := corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: spec.Size},
			},
		},
	}
	if spec.StorageClass != nil {
		pvc.Spec.StorageClassName = spec.StorageClass
	}
	return pvc
}
