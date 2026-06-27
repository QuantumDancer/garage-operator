//go:build e2e
// +build e2e

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

package e2e

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/QuantumDancer/garage-operator/test/utils"
)

// namespace where the project is deployed in
const namespace = "garage-operator-system"

// serviceAccountName created for the project
const serviceAccountName = "garage-operator-controller-manager"

// metricsServiceName is the name of the metrics service of the project
const metricsServiceName = "garage-operator-controller-manager-metrics-service"

// metricsRoleBindingName is the name of the RBAC that will be created to allow get the metrics data
const metricsRoleBindingName = "garage-operator-metrics-binding"

// garageImage is the Garage container image the single-node e2e cluster runs. It is preloaded
// into Kind so the test does not depend on a live pull at pod-start time.
const garageImage = "dxflrs/amd64_garage:v2.0.0"

// ContinueOnFailure keeps an unrelated spec failure (e.g. the metrics curl pod flaking on
// cluster-DNS warm-up) from skipping the rest of this Ordered container — the GarageCluster
// spec must run and report on its own merits regardless of the metrics spec's outcome.
var _ = Describe("Manager", Ordered, ContinueOnFailure, func() {
	var controllerPodName string

	// Before running the tests, set up the environment by creating the namespace,
	// enforce the restricted security policy to the namespace, installing CRDs,
	// and deploying the controller.
	BeforeAll(func() {
		By("creating manager namespace")
		cmd := exec.Command("kubectl", "create", "ns", namespace)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to create namespace")

		By("labeling the namespace to enforce the restricted security policy")
		cmd = exec.Command("kubectl", "label", "--overwrite", "ns", namespace,
			"pod-security.kubernetes.io/enforce=restricted")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to label namespace with restricted policy")

		By("installing CRDs")
		cmd = exec.Command("make", "install")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to install CRDs")

		By("deploying the controller-manager")
		cmd = exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", managerImage))
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to deploy the controller-manager")
	})

	// After all tests have been executed, clean up by undeploying the controller, uninstalling CRDs,
	// and deleting the namespace.
	AfterAll(func() {
		By("cleaning up the curl pod for metrics")
		cmd := exec.Command("kubectl", "delete", "pod", "curl-metrics", "-n", namespace)
		_, _ = utils.Run(cmd)

		By("cleaning up the cluster-scoped metrics ClusterRoleBinding")
		cmd = exec.Command("kubectl", "delete", "clusterrolebinding", metricsRoleBindingName, "--ignore-not-found")
		_, _ = utils.Run(cmd)

		By("undeploying the controller-manager")
		cmd = exec.Command("make", "undeploy")
		_, _ = utils.Run(cmd)

		By("uninstalling CRDs")
		cmd = exec.Command("make", "uninstall")
		_, _ = utils.Run(cmd)

		By("removing manager namespace")
		cmd = exec.Command("kubectl", "delete", "ns", namespace)
		_, _ = utils.Run(cmd)
	})

	// After each test, check for failures and collect logs, events,
	// and pod descriptions for debugging.
	AfterEach(func() {
		specReport := CurrentSpecReport()
		if specReport.Failed() {
			By("Fetching controller manager pod logs")
			cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
			controllerLogs, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Controller logs:\n %s", controllerLogs)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Controller logs: %s", err)
			}

			By("Fetching Kubernetes events")
			cmd = exec.Command("kubectl", "get", "events", "-n", namespace, "--sort-by=.lastTimestamp")
			eventsOutput, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Kubernetes events:\n%s", eventsOutput)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Kubernetes events: %s", err)
			}

			By("Fetching curl-metrics logs")
			cmd = exec.Command("kubectl", "logs", "curl-metrics", "-n", namespace)
			metricsOutput, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Metrics logs:\n %s", metricsOutput)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get curl-metrics logs: %s", err)
			}

			By("Fetching controller manager pod description")
			cmd = exec.Command("kubectl", "describe", "pod", controllerPodName, "-n", namespace)
			podDescription, err := utils.Run(cmd)
			if err == nil {
				fmt.Println("Pod description:\n", podDescription)
			} else {
				fmt.Println("Failed to describe controller pod")
			}
		}
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)

	Context("Manager", func() {
		// Each spec carries a Ginkgo Label so CI can shard the suite across parallel
		// jobs (one Kind cluster per job) via `-ginkgo.label-filter`, and so a local
		// run can focus only the specs covering the code under change. The label↔shard
		// mapping lives in .github/workflows/test-e2e.yml. The manager and metrics
		// specs share the "manager" label deliberately: the metrics spec reuses the
		// controllerPodName the manager spec discovers, so they must run together.
		It("should run successfully", Label("manager"), func() {
			By("validating that the controller-manager pod is running as expected")
			verifyControllerUp := func(g Gomega) {
				By("getting the name of the controller-manager pod")
				cmd := exec.Command("kubectl", "get",
					"pods", "-l", "control-plane=controller-manager",
					"-o", "go-template={{ range .items }}"+
						"{{ if not .metadata.deletionTimestamp }}"+
						"{{ .metadata.name }}"+
						"{{ \"\\n\" }}{{ end }}{{ end }}",
					"-n", namespace,
				)

				podOutput, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve controller-manager pod information")
				podNames := utils.GetNonEmptyLines(podOutput)
				g.Expect(podNames).To(HaveLen(1), "expected 1 controller pod running")
				controllerPodName = podNames[0]
				g.Expect(controllerPodName).To(ContainSubstring("controller-manager"))

				By("validating the pod's status")
				cmd = exec.Command("kubectl", "get",
					"pods", controllerPodName, "-o", "jsonpath={.status.phase}",
					"-n", namespace,
				)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Running"), "Incorrect controller-manager pod status")
			}
			Eventually(verifyControllerUp).Should(Succeed())
		})

		// FlakeAttempts(2) is insurance against residual warm-up jitter on a fresh Kind cluster
		// (the metrics probe itself hits the Service ClusterIP, so it does not depend on in-pod
		// DNS). The spec is idempotent across retries: the ClusterRoleBinding and curl pod are
		// both create-or-replace.
		It("should ensure the metrics endpoint is serving metrics", Label("manager"), FlakeAttempts(2), func() {
			By("creating a ClusterRoleBinding for the service account to allow access to metrics")
			cmd := exec.Command("kubectl", "create", "clusterrolebinding", metricsRoleBindingName,
				"--clusterrole=garage-operator-metrics-reader",
				fmt.Sprintf("--serviceaccount=%s:%s", namespace, serviceAccountName),
			)
			// Idempotent: the binding is cluster-scoped (not torn down with the namespace), so a
			// FlakeAttempts retry — or a rerun against a cluster a prior failed run left behind —
			// must tolerate it already existing rather than failing before the real checks.
			if _, err := utils.Run(cmd); err != nil {
				Expect(err.Error()).To(ContainSubstring("already exists"), "Failed to create ClusterRoleBinding")
			}

			By("validating that the metrics service is available")
			cmd = exec.Command("kubectl", "get", "service", metricsServiceName, "-n", namespace)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Metrics service should exist")

			By("getting the service account token")
			token, err := serviceAccountToken()
			Expect(err).NotTo(HaveOccurred())
			Expect(token).NotTo(BeEmpty())

			By("ensuring the controller pod is ready")
			verifyControllerPodReady := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pod", controllerPodName, "-n", namespace,
					"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("True"), "Controller pod not ready")
			}
			Eventually(verifyControllerPodReady, 3*time.Minute, time.Second).Should(Succeed())

			By("verifying that the controller manager is serving the metrics server")
			verifyMetricsServerStarted := func(g Gomega) {
				cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("Serving metrics server"),
					"Metrics server not yet started")
			}
			Eventually(verifyMetricsServerStarted, 3*time.Minute, time.Second).Should(Succeed())

			// +kubebuilder:scaffold:e2e-metrics-webhooks-readiness

			By("waiting for the metrics service to have ready endpoints")
			verifyMetricsEndpoints := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "endpoints", metricsServiceName, "-n", namespace,
					"-o", "jsonpath={.subsets[*].addresses[*].ip}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).NotTo(BeEmpty(), "metrics service has no ready endpoints yet")
			}
			Eventually(verifyMetricsEndpoints, 2*time.Minute, 5*time.Second).Should(Succeed())

			// Probe the metrics Service by its ClusterIP, not its DNS name. curlimages/curl is
			// Alpine/musl, whose resolver fails to resolve *.svc.cluster.local in some Kind
			// environments even after CoreDNS is healthy (the operator's own glibc pods resolve
			// fine in the same cluster). The ClusterIP is routed by kube-proxy with no in-pod DNS,
			// and TLS verification is already skipped (-k), so the cert not covering the IP is fine.
			By("resolving the metrics service ClusterIP")
			metricsClusterIP, err := utils.Run(exec.Command("kubectl", "get", "service", metricsServiceName,
				"-n", namespace, "-o", "jsonpath={.spec.clusterIP}"))
			Expect(err).NotTo(HaveOccurred())
			Expect(metricsClusterIP).NotTo(BeEmpty(), "metrics service has no ClusterIP")

			// A FlakeAttempts retry re-runs the spec from the top; clear any curl pod a prior
			// attempt left behind so the run below is not rejected as AlreadyExists.
			By("creating the curl-metrics pod to access the metrics endpoint")
			_, _ = utils.Run(exec.Command("kubectl", "delete", "pod", "curl-metrics", "-n", namespace, "--ignore-not-found"))
			cmd = exec.Command("kubectl", "run", "curl-metrics", "--restart=Never",
				"--namespace", namespace,
				"--image=curlimages/curl:latest",
				"--overrides",
				fmt.Sprintf(`{
					"spec": {
						"containers": [{
							"name": "curl",
							"image": "curlimages/curl:latest",
							"command": ["/bin/sh", "-c"],
							"args": [
								"for i in $(seq 1 60); do curl -v -k -H 'Authorization: Bearer %s' https://%s:8443/metrics && exit 0 || sleep 3; done; exit 1"
							],
							"securityContext": {
								"readOnlyRootFilesystem": true,
								"allowPrivilegeEscalation": false,
								"capabilities": {
									"drop": ["ALL"]
								},
								"runAsNonRoot": true,
								"runAsUser": 1000,
								"seccompProfile": {
									"type": "RuntimeDefault"
								}
							}
						}],
						"serviceAccountName": "%s"
					}
				}`, token, metricsClusterIP, serviceAccountName))
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create curl-metrics pod")

			By("waiting for the curl-metrics pod to complete.")
			verifyCurlUp := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pods", "curl-metrics",
					"-o", "jsonpath={.status.phase}",
					"-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Succeeded"), "curl pod in wrong status")
			}
			Eventually(verifyCurlUp, 5*time.Minute).Should(Succeed())

			By("getting the metrics by checking curl-metrics logs")
			verifyMetricsAvailable := func(g Gomega) {
				metricsOutput, err := getMetricsOutput()
				g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve logs from curl pod")
				g.Expect(metricsOutput).NotTo(BeEmpty())
				g.Expect(metricsOutput).To(ContainSubstring("< HTTP/1.1 200 OK"))
			}
			Eventually(verifyMetricsAvailable, 2*time.Minute).Should(Succeed())
		})

		It("should bring up a 3-node GarageCluster with the layout applied", Label("cluster"), func() {
			const clusterNamespace = "garage-e2e"
			const clusterName = "e2e"

			By("preloading the Garage image into the Kind cluster")
			_, err := utils.Run(exec.Command("docker", "pull", garageImage))
			Expect(err).NotTo(HaveOccurred(), "Failed to pull the Garage image")
			Expect(utils.LoadImageToKindClusterWithName(garageImage)).To(Succeed(), "Failed to load Garage image into Kind")

			By("creating the cluster namespace")
			_, _ = utils.Run(exec.Command("kubectl", "create", "ns", clusterNamespace))

			By("applying a 3-node, replication-factor-3 GarageCluster")
			manifest := fmt.Sprintf(`apiVersion: garage.rottler.io/v1alpha1
kind: GarageCluster
metadata:
  name: %s
  namespace: %s
spec:
  replicationFactor: 3
  nodePools:
    - name: default
      replicas: 3
      storage:
        data: { size: 1Gi }
        meta: { size: 1Gi }
`, clusterName, clusterNamespace)
			manifestFile := filepath.Join("/tmp", "garagecluster-e2e.yaml")
			Expect(os.WriteFile(manifestFile, []byte(manifest), os.FileMode(0o644))).To(Succeed())
			_, err = utils.Run(exec.Command("kubectl", "apply", "-f", manifestFile))
			Expect(err).NotTo(HaveOccurred(), "Failed to apply GarageCluster")

			By("waiting for the GarageCluster to become Ready with a fully-connected 3-node layout")
			getStatus := func(jsonPath string) (string, error) {
				return utils.Run(exec.Command("kubectl", "get", "garagecluster", clusterName,
					"-n", clusterNamespace, "-o", "jsonpath="+jsonPath))
			}
			verifyClusterReady := func(g Gomega) {
				ready, err := getStatus("{.status.conditions[?(@.type=='Ready')].status}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(ready).To(Equal("True"), "GarageCluster is not Ready")

				version, err := getStatus("{.status.layout.version}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(version).NotTo(BeEmpty(), "layout version is not reported")
				g.Expect(strconv.Atoi(version)).To(BeNumerically(">=", 1), "layout version should be applied")

				health, err := getStatus("{.status.health.status}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(health).To(Equal("healthy"), "cluster health is not healthy")

				By("asserting all three nodes peered and joined the layout")
				connected, err := getStatus("{.status.health.connectedNodes}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(connected).To(Equal("3"), "all three nodes should be connected")

				known, err := getStatus("{.status.health.knownNodes}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(known).To(Equal("3"), "all three nodes should be known")

				layoutNodes, err := getStatus("{.status.layout.nodes[*].nodeId}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(utils.GetNonEmptyLines(strings.ReplaceAll(layoutNodes, " ", "\n"))).
					To(HaveLen(3), "all three nodes should be in the layout")
			}
			Eventually(verifyClusterReady, 5*time.Minute, 5*time.Second).Should(Succeed())

			By("cleaning up the GarageCluster")
			_, _ = utils.Run(exec.Command("kubectl", "delete", "-f", manifestFile))
			_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", clusterNamespace))
		})

		It("should gate a node drain behind approval, then reclaim the node's PVCs", Label("drain"), func() {
			const clusterNamespace = "garage-drain-e2e"
			const clusterName = "drain"
			const ssName = "drain-default"

			By("preloading the Garage image into the Kind cluster")
			_, err := utils.Run(exec.Command("docker", "pull", garageImage))
			Expect(err).NotTo(HaveOccurred(), "Failed to pull the Garage image")
			Expect(utils.LoadImageToKindClusterWithName(garageImage)).To(Succeed(), "Failed to load Garage image into Kind")

			By("creating the drain-test namespace")
			_, _ = utils.Run(exec.Command("kubectl", "create", "ns", clusterNamespace))

			// replicationFactor 2 with 3 nodes: scaling to 2 nodes still satisfies the factor,
			// so the cluster stays healthy after the drain (unlike an rf=3 cluster, which needs
			// at least 3 nodes).
			By("applying a 3-node, replication-factor-2 GarageCluster")
			manifest := fmt.Sprintf(`apiVersion: garage.rottler.io/v1alpha1
kind: GarageCluster
metadata:
  name: %s
  namespace: %s
spec:
  replicationFactor: 2
  nodePools:
    - name: default
      replicas: 3
      storage:
        data: { size: 1Gi }
        meta: { size: 1Gi }
`, clusterName, clusterNamespace)
			manifestFile := filepath.Join("/tmp", "garagecluster-drain-e2e.yaml")
			Expect(os.WriteFile(manifestFile, []byte(manifest), os.FileMode(0o644))).To(Succeed())
			_, err = utils.Run(exec.Command("kubectl", "apply", "-f", manifestFile))
			Expect(err).NotTo(HaveOccurred(), "Failed to apply GarageCluster")

			getStatus := func(jsonPath string) (string, error) {
				return utils.Run(exec.Command("kubectl", "get", "garagecluster", clusterName,
					"-n", clusterNamespace, "-o", "jsonpath="+jsonPath))
			}

			By("waiting for the 3-node cluster to become Ready")
			verifyReady := func(g Gomega) {
				ready, err := getStatus("{.status.conditions[?(@.type=='Ready')].status}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(ready).To(Equal("True"), "GarageCluster is not Ready")
				nodes, err := getStatus("{.status.layout.nodes[*].nodeId}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(utils.GetNonEmptyLines(strings.ReplaceAll(nodes, " ", "\n"))).
					To(HaveLen(3), "all three nodes should be in the layout")
			}
			Eventually(verifyReady, 5*time.Minute, 5*time.Second).Should(Succeed())

			By("recording the applied layout version to derive the approval target")
			currentVersionStr, err := getStatus("{.status.layout.version}")
			Expect(err).NotTo(HaveOccurred())
			currentVersion, err := strconv.Atoi(currentVersionStr)
			Expect(err).NotTo(HaveOccurred())
			targetVersion := currentVersion + 1

			By("requesting a scale-down from 3 to 2 replicas")
			_, err = utils.Run(exec.Command("kubectl", "patch", "garagecluster", clusterName,
				"-n", clusterNamespace, "--type=json",
				"-p", `[{"op":"replace","path":"/spec/nodePools/0/replicas","value":2}]`))
			Expect(err).NotTo(HaveOccurred(), "Failed to patch replicas")

			By("holding the drain behind the approval annotation: still 3 nodes, change pending")
			verifyPending := func(g Gomega) {
				pending, err := getStatus("{.status.conditions[?(@.type=='LayoutChangePending')].status}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(pending).To(Equal("True"), "expected a pending destructive layout change")

				replicas, err := utils.Run(exec.Command("kubectl", "get", "statefulset", ssName,
					"-n", clusterNamespace, "-o", "jsonpath={.spec.replicas}"))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(replicas).To(Equal("3"), "StatefulSet must not scale down before approval")

				nodes, err := getStatus("{.status.layout.nodes[*].nodeId}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(utils.GetNonEmptyLines(strings.ReplaceAll(nodes, " ", "\n"))).
					To(HaveLen(3), "the node must not be drained before approval")
			}
			Eventually(verifyPending, 2*time.Minute, 5*time.Second).Should(Succeed())

			By("approving the drain via the layout-approval annotation")
			_, err = utils.Run(exec.Command("kubectl", "annotate", "garagecluster", clusterName,
				"-n", clusterNamespace,
				fmt.Sprintf("garage.rottler.io/approve-layout=%d", targetVersion), "--overwrite"))
			Expect(err).NotTo(HaveOccurred(), "Failed to annotate approval")

			By("draining the node: layout shrinks to 2, the StatefulSet scales down, health stays healthy")
			verifyDrained := func(g Gomega) {
				nodes, err := getStatus("{.status.layout.nodes[*].nodeId}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(utils.GetNonEmptyLines(strings.ReplaceAll(nodes, " ", "\n"))).
					To(HaveLen(2), "the drained node should leave the layout")

				replicas, err := utils.Run(exec.Command("kubectl", "get", "statefulset", ssName,
					"-n", clusterNamespace, "-o", "jsonpath={.spec.replicas}"))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(replicas).To(Equal("2"), "StatefulSet should be scaled down")

				health, err := getStatus("{.status.health.status}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(health).To(Equal("healthy"), "cluster should stay healthy after the drain")
			}
			Eventually(verifyDrained, 5*time.Minute, 5*time.Second).Should(Succeed())

			By("reclaiming the drained node's PVCs (ordinal 2)")
			verifyClaimsGone := func(g Gomega) {
				for _, vol := range []string{"meta", "data"} {
					out, _ := utils.Run(exec.Command("kubectl", "get", "pvc",
						fmt.Sprintf("%s-%s-2", vol, ssName), "-n", clusterNamespace,
						"--ignore-not-found", "-o", "jsonpath={.metadata.name}"))
					g.Expect(out).To(BeEmpty(), "drained node's %s PVC should be deleted", vol)
				}
			}
			Eventually(verifyClaimsGone, 2*time.Minute, 5*time.Second).Should(Succeed())

			By("cleaning up the GarageCluster")
			_, _ = utils.Run(exec.Command("kubectl", "delete", "-f", manifestFile))
			_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", clusterNamespace))
		})

		It("should lay out a multi-zone cluster (static + zoneFrom) and apply zoneRedundancy", Label("multizone"), func() {
			const clusterNamespace = "garage-zone-e2e"
			const clusterName = "zone"
			// A custom Node label pool "b" derives its zone from via zoneFrom. The e2e Kind
			// cluster is single-node, so both pools land on the same Node; the static zone on
			// pool "a" and the derived zone on pool "b" still give the layout two distinct
			// zones, which is what cross-zone replication needs.
			const zoneNodeLabel = "garage.rottler.io/e2e-zone"

			By("preloading the Garage image into the Kind cluster")
			_, err := utils.Run(exec.Command("docker", "pull", garageImage))
			Expect(err).NotTo(HaveOccurred(), "Failed to pull the Garage image")
			Expect(utils.LoadImageToKindClusterWithName(garageImage)).To(Succeed(), "Failed to load Garage image into Kind")

			By("labelling the Node so the zoneFrom pool can derive its zone")
			_, err = utils.Run(exec.Command("kubectl", "label", "nodes", "--all",
				zoneNodeLabel+"=zone-b", "--overwrite"))
			Expect(err).NotTo(HaveOccurred(), "Failed to label nodes for zoneFrom")

			By("creating the cluster namespace")
			_, _ = utils.Run(exec.Command("kubectl", "create", "ns", clusterNamespace))

			// Two single-node pools at replicationFactor 2: pool "a" pins its zone statically,
			// pool "b" derives it from the Node label. zoneRedundancy atLeast 2 then requires
			// every partition to span both zones — the cross-zone replication this exercises.
			By("applying a two-pool, two-zone GarageCluster with zoneRedundancy atLeast 2")
			manifest := fmt.Sprintf(`apiVersion: garage.rottler.io/v1alpha1
kind: GarageCluster
metadata:
  name: %s
  namespace: %s
spec:
  replicationFactor: 2
  zoneRedundancy:
    mode: AtLeast
    atLeast: 2
  nodePools:
    - name: a
      replicas: 1
      zone: zone-a
      storage:
        data: { size: 1Gi }
        meta: { size: 1Gi }
    - name: b
      replicas: 1
      zoneFrom: %s
      storage:
        data: { size: 1Gi }
        meta: { size: 1Gi }
`, clusterName, clusterNamespace, zoneNodeLabel)
			manifestFile := filepath.Join("/tmp", "garagecluster-zone-e2e.yaml")
			Expect(os.WriteFile(manifestFile, []byte(manifest), os.FileMode(0o644))).To(Succeed())
			_, err = utils.Run(exec.Command("kubectl", "apply", "-f", manifestFile))
			Expect(err).NotTo(HaveOccurred(), "Failed to apply GarageCluster")

			getStatus := func(jsonPath string) (string, error) {
				return utils.Run(exec.Command("kubectl", "get", "garagecluster", clusterName,
					"-n", clusterNamespace, "-o", "jsonpath="+jsonPath))
			}

			By("waiting for Ready with both nodes in the layout, one per zone, and healthy")
			verifyZoned := func(g Gomega) {
				ready, err := getStatus("{.status.conditions[?(@.type=='Ready')].status}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(ready).To(Equal("True"), "GarageCluster is not Ready")

				health, err := getStatus("{.status.health.status}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(health).To(Equal("healthy"), "cluster health is not healthy")

				nodes, err := getStatus("{.status.layout.nodes[*].nodeId}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(utils.GetNonEmptyLines(strings.ReplaceAll(nodes, " ", "\n"))).
					To(HaveLen(2), "both nodes should be in the layout")

				zones, err := getStatus("{.status.layout.nodes[*].zone}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(zones).To(ContainSubstring("zone-a"), "pool a should be laid out in its static zone")
				g.Expect(zones).To(ContainSubstring("zone-b"), "pool b should derive its zone from the Node label")
			}
			Eventually(verifyZoned, 5*time.Minute, 5*time.Second).Should(Succeed())

			By("confirming Garage accepted the atLeast-2 zone redundancy and keeps LayoutApplied")
			// A too-high atLeast would leave LayoutApplied=False/ZoneRedundancyInvalid; with two
			// zones present the change applies cleanly, so LayoutApplied stays True.
			Consistently(func(g Gomega) {
				applied, err := getStatus("{.status.conditions[?(@.type=='LayoutApplied')].status}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(applied).To(Equal("True"), "zone redundancy should apply cleanly with two zones")
			}, 15*time.Second, 5*time.Second).Should(Succeed())

			By("cleaning up the GarageCluster and the Node label")
			_, _ = utils.Run(exec.Command("kubectl", "delete", "-f", manifestFile))
			_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", clusterNamespace))
			_, _ = utils.Run(exec.Command("kubectl", "label", "nodes", "--all", zoneNodeLabel+"-"))
		})

		It("should grow a node's storage in place when its data size is increased", Label("grow"), func() {
			const clusterNamespace = "garage-grow-e2e"
			const clusterName = "grow"
			const ssName = "grow-default"
			const storageClassName = "garage-expandable"

			By("preloading the Garage image into the Kind cluster")
			_, err := utils.Run(exec.Command("docker", "pull", garageImage))
			Expect(err).NotTo(HaveOccurred(), "Failed to pull the Garage image")
			Expect(utils.LoadImageToKindClusterWithName(garageImage)).To(Succeed(), "Failed to load Garage image into Kind")

			By("creating the grow-test namespace")
			_, _ = utils.Run(exec.Command("kubectl", "create", "ns", clusterNamespace))

			// kind's default StorageClass does not allow volume expansion, so define one backed by
			// the same local-path provisioner that does. local-path accepts the resize request at
			// the API level (it does not enforce volume size), which is all the operator's
			// in-place-growth contract needs: the physical filesystem resize is the CSI's job.
			By("creating an expansion-capable StorageClass")
			scManifest := fmt.Sprintf(`apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: %s
provisioner: rancher.io/local-path
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
`, storageClassName)
			scFile := filepath.Join("/tmp", "garage-expandable-sc.yaml")
			Expect(os.WriteFile(scFile, []byte(scManifest), os.FileMode(0o644))).To(Succeed())
			_, err = utils.Run(exec.Command("kubectl", "apply", "-f", scFile))
			Expect(err).NotTo(HaveOccurred(), "Failed to apply StorageClass")

			By("applying a single-node GarageCluster on the expandable StorageClass")
			manifest := fmt.Sprintf(`apiVersion: garage.rottler.io/v1alpha1
kind: GarageCluster
metadata:
  name: %s
  namespace: %s
spec:
  replicationFactor: 1
  nodePools:
    - name: default
      replicas: 1
      storage:
        data: { size: 1Gi, storageClass: %s }
        meta: { size: 1Gi, storageClass: %s }
`, clusterName, clusterNamespace, storageClassName, storageClassName)
			manifestFile := filepath.Join("/tmp", "garagecluster-grow-e2e.yaml")
			Expect(os.WriteFile(manifestFile, []byte(manifest), os.FileMode(0o644))).To(Succeed())
			_, err = utils.Run(exec.Command("kubectl", "apply", "-f", manifestFile))
			Expect(err).NotTo(HaveOccurred(), "Failed to apply GarageCluster")

			getStatus := func(jsonPath string) (string, error) {
				return utils.Run(exec.Command("kubectl", "get", "garagecluster", clusterName,
					"-n", clusterNamespace, "-o", "jsonpath="+jsonPath))
			}

			By("waiting for the single-node cluster to become Ready at 1Gi")
			Eventually(func(g Gomega) {
				ready, err := getStatus("{.status.conditions[?(@.type=='Ready')].status}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(ready).To(Equal("True"), "GarageCluster is not Ready")
				capacity, err := getStatus("{.status.layout.nodes[0].capacity}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(capacity).To(Equal("1Gi"), "initial layout capacity should be 1Gi")
			}, 5*time.Minute, 5*time.Second).Should(Succeed())

			By("recording the original pod name to prove it is not restarted by the resize")
			podName, err := utils.Run(exec.Command("kubectl", "get", "pod", ssName+"-0",
				"-n", clusterNamespace, "-o", "jsonpath={.metadata.uid}"))
			Expect(err).NotTo(HaveOccurred())
			Expect(podName).NotTo(BeEmpty())

			By("increasing the data volume size to 2Gi")
			_, err = utils.Run(exec.Command("kubectl", "patch", "garagecluster", clusterName,
				"-n", clusterNamespace, "--type=json",
				"-p", `[{"op":"replace","path":"/spec/nodePools/0/storage/data/size","value":"2Gi"}]`))
			Expect(err).NotTo(HaveOccurred(), "Failed to patch data size")

			By("expanding the data PVC and recreating the StatefulSet with the larger template")
			Eventually(func(g Gomega) {
				pvcSize, err := utils.Run(exec.Command("kubectl", "get", "pvc",
					"data-"+ssName+"-0", "-n", clusterNamespace,
					"-o", "jsonpath={.spec.resources.requests.storage}"))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(pvcSize).To(Equal("2Gi"), "data PVC request should be grown to 2Gi")

				vctSize, err := utils.Run(exec.Command("kubectl", "get", "statefulset", ssName,
					"-n", clusterNamespace,
					"-o", "jsonpath={.spec.volumeClaimTemplates[?(@.metadata.name=='data')].spec.resources.requests.storage}"))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(vctSize).To(Equal("2Gi"), "StatefulSet data template should be recreated at 2Gi")
			}, 3*time.Minute, 5*time.Second).Should(Succeed())

			By("re-applying the derived capacity to the layout while staying Ready and healthy")
			Eventually(func(g Gomega) {
				ready, err := getStatus("{.status.conditions[?(@.type=='Ready')].status}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(ready).To(Equal("True"), "cluster should remain Ready after the resize")

				capacity, err := getStatus("{.status.layout.nodes[0].capacity}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(capacity).To(Equal("2Gi"), "layout capacity should reflect the grown data size")

				health, err := getStatus("{.status.health.status}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(health).To(Equal("healthy"), "cluster should stay healthy")
			}, 3*time.Minute, 5*time.Second).Should(Succeed())

			By("confirming the Garage pod was not restarted by the in-place resize")
			uidAfter, err := utils.Run(exec.Command("kubectl", "get", "pod", ssName+"-0",
				"-n", clusterNamespace, "-o", "jsonpath={.metadata.uid}"))
			Expect(err).NotTo(HaveOccurred())
			Expect(uidAfter).To(Equal(podName), "the pod should be re-adopted, not recreated")

			By("cleaning up the GarageCluster and StorageClass")
			_, _ = utils.Run(exec.Command("kubectl", "delete", "-f", manifestFile))
			_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", clusterNamespace))
			_, _ = utils.Run(exec.Command("kubectl", "delete", "-f", scFile))
		})

		It("should migrate a node's storage when its data size is shrunk", Label("shrink"), func() {
			const clusterNamespace = "garage-shrink-e2e"
			const clusterName = "shrink"
			const ssName = "shrink-default"

			By("preloading the Garage image into the Kind cluster")
			_, err := utils.Run(exec.Command("docker", "pull", garageImage))
			Expect(err).NotTo(HaveOccurred(), "Failed to pull the Garage image")
			Expect(utils.LoadImageToKindClusterWithName(garageImage)).To(Succeed(), "Failed to load Garage image into Kind")

			By("creating the shrink-test namespace")
			_, _ = utils.Run(exec.Command("kubectl", "create", "ns", clusterNamespace))

			// replicationFactor 2 with 3 nodes: draining one node for recreation still leaves two,
			// which can hold both replicas, so the cluster re-replicates fully (the migration's
			// safety gate) and stays healthy throughout. A shrink cannot be served in place — a PVC
			// never shrinks — so the operator drains, recreates, and refills each node in turn.
			By("applying a 3-node, replication-factor-2 GarageCluster with 2Gi data volumes")
			manifest := fmt.Sprintf(`apiVersion: garage.rottler.io/v1alpha1
kind: GarageCluster
metadata:
  name: %s
  namespace: %s
spec:
  replicationFactor: 2
  nodePools:
    - name: default
      replicas: 3
      storage:
        data: { size: 2Gi }
        meta: { size: 1Gi }
`, clusterName, clusterNamespace)
			manifestFile := filepath.Join("/tmp", "garagecluster-shrink-e2e.yaml")
			Expect(os.WriteFile(manifestFile, []byte(manifest), os.FileMode(0o644))).To(Succeed())
			_, err = utils.Run(exec.Command("kubectl", "apply", "-f", manifestFile))
			Expect(err).NotTo(HaveOccurred(), "Failed to apply GarageCluster")

			getStatus := func(jsonPath string) (string, error) {
				return utils.Run(exec.Command("kubectl", "get", "garagecluster", clusterName,
					"-n", clusterNamespace, "-o", "jsonpath="+jsonPath))
			}

			By("waiting for the 3-node cluster to become Ready at 2Gi")
			Eventually(func(g Gomega) {
				ready, err := getStatus("{.status.conditions[?(@.type=='Ready')].status}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(ready).To(Equal("True"), "GarageCluster is not Ready")
				nodes, err := getStatus("{.status.layout.nodes[*].nodeId}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(utils.GetNonEmptyLines(strings.ReplaceAll(nodes, " ", "\n"))).
					To(HaveLen(3), "all three nodes should be in the layout")
			}, 5*time.Minute, 5*time.Second).Should(Succeed())

			By("shrinking the data volume from 2Gi to 1Gi")
			_, err = utils.Run(exec.Command("kubectl", "patch", "garagecluster", clusterName,
				"-n", clusterNamespace, "--type=json",
				"-p", `[{"op":"replace","path":"/spec/nodePools/0/storage/data/size","value":"1Gi"}]`))
			Expect(err).NotTo(HaveOccurred(), "Failed to patch data size")

			By("observing the migration take over (Ready reports StorageMigrating)")
			Eventually(func(g Gomega) {
				reason, err := getStatus("{.status.conditions[?(@.type=='Ready')].reason}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(reason).To(Equal("StorageMigrating"), "the migration should take over the layout")
			}, 3*time.Minute, 5*time.Second).Should(Succeed())

			By("migrating every node in turn: all three data PVCs end up recreated at 1Gi")
			Eventually(func(g Gomega) {
				for ordinal := 0; ordinal < 3; ordinal++ {
					pvcSize, err := utils.Run(exec.Command("kubectl", "get", "pvc",
						fmt.Sprintf("data-%s-%d", ssName, ordinal), "-n", clusterNamespace,
						"-o", "jsonpath={.spec.resources.requests.storage}"))
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(pvcSize).To(Equal("1Gi"), "node %d data PVC should be recreated at 1Gi", ordinal)
				}
			}, 12*time.Minute, 10*time.Second).Should(Succeed())

			By("converging: migration cleared, layout capacity 1Gi, cluster healthy with 3 nodes")
			Eventually(func(g Gomega) {
				phase, err := getStatus("{.status.storageMigration.phase}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(phase).To(BeEmpty(), "the migration should be complete")

				ready, err := getStatus("{.status.conditions[?(@.type=='Ready')].reason}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(ready).To(Equal("ClusterReady"), "the cluster should return to a steady Ready")

				capacity, err := getStatus("{.status.layout.nodes[0].capacity}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(capacity).To(Equal("1Gi"), "layout capacity should reflect the shrunk data size")

				health, err := getStatus("{.status.health.status}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(health).To(Equal("healthy"), "cluster should be healthy after the migration")

				nodes, err := getStatus("{.status.layout.nodes[*].nodeId}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(utils.GetNonEmptyLines(strings.ReplaceAll(nodes, " ", "\n"))).
					To(HaveLen(3), "all three nodes should be back in the layout")
			}, 5*time.Minute, 10*time.Second).Should(Succeed())

			By("cleaning up the GarageCluster")
			_, _ = utils.Run(exec.Command("kubectl", "delete", "-f", manifestFile))
			_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", clusterNamespace))
		})

		It("should migrate a node's storage when its data StorageClass is changed", Label("class"), func() {
			const clusterNamespace = "garage-class-e2e"
			const clusterName = "class"
			const ssName = "class-default"
			const classA = "garage-class-a"
			const classB = "garage-class-b"

			By("preloading the Garage image into the Kind cluster")
			_, err := utils.Run(exec.Command("docker", "pull", garageImage))
			Expect(err).NotTo(HaveOccurred(), "Failed to pull the Garage image")
			Expect(utils.LoadImageToKindClusterWithName(garageImage)).To(Succeed(), "Failed to load Garage image into Kind")

			By("creating the class-change-test namespace")
			_, _ = utils.Run(exec.Command("kubectl", "create", "ns", clusterNamespace))

			// Two StorageClasses backed by the same local-path provisioner. A StorageClass change
			// can never be served in place — a PVC's storageClassName is immutable — so the
			// operator drains, recreates the volume on the new class, and refills each node in
			// turn, exactly like a shrink. The classes need not differ in capability; only the
			// name on the PVC changes.
			By("creating two local-path-backed StorageClasses")
			scManifest := fmt.Sprintf(`apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: %s
provisioner: rancher.io/local-path
volumeBindingMode: WaitForFirstConsumer
---
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: %s
provisioner: rancher.io/local-path
volumeBindingMode: WaitForFirstConsumer
`, classA, classB)
			scFile := filepath.Join("/tmp", "garage-class-scs.yaml")
			Expect(os.WriteFile(scFile, []byte(scManifest), os.FileMode(0o644))).To(Succeed())
			_, err = utils.Run(exec.Command("kubectl", "apply", "-f", scFile))
			Expect(err).NotTo(HaveOccurred(), "Failed to apply StorageClasses")

			// replicationFactor 2 with 3 nodes: draining one node for recreation still leaves two,
			// so the cluster re-replicates fully (the migration's safety gate) and stays healthy.
			By("applying a 3-node, replication-factor-2 GarageCluster on StorageClass A")
			manifest := fmt.Sprintf(`apiVersion: garage.rottler.io/v1alpha1
kind: GarageCluster
metadata:
  name: %s
  namespace: %s
spec:
  replicationFactor: 2
  nodePools:
    - name: default
      replicas: 3
      storage:
        data: { size: 1Gi, storageClass: %s }
        meta: { size: 1Gi, storageClass: %s }
`, clusterName, clusterNamespace, classA, classA)
			manifestFile := filepath.Join("/tmp", "garagecluster-class-e2e.yaml")
			Expect(os.WriteFile(manifestFile, []byte(manifest), os.FileMode(0o644))).To(Succeed())
			_, err = utils.Run(exec.Command("kubectl", "apply", "-f", manifestFile))
			Expect(err).NotTo(HaveOccurred(), "Failed to apply GarageCluster")

			getStatus := func(jsonPath string) (string, error) {
				return utils.Run(exec.Command("kubectl", "get", "garagecluster", clusterName,
					"-n", clusterNamespace, "-o", "jsonpath="+jsonPath))
			}

			By("waiting for the 3-node cluster to become Ready on StorageClass A")
			Eventually(func(g Gomega) {
				ready, err := getStatus("{.status.conditions[?(@.type=='Ready')].status}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(ready).To(Equal("True"), "GarageCluster is not Ready")
				nodes, err := getStatus("{.status.layout.nodes[*].nodeId}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(utils.GetNonEmptyLines(strings.ReplaceAll(nodes, " ", "\n"))).
					To(HaveLen(3), "all three nodes should be in the layout")
			}, 5*time.Minute, 5*time.Second).Should(Succeed())

			By("changing the data StorageClass from A to B")
			_, err = utils.Run(exec.Command("kubectl", "patch", "garagecluster", clusterName,
				"-n", clusterNamespace, "--type=json",
				"-p", fmt.Sprintf(`[{"op":"replace","path":"/spec/nodePools/0/storage/data/storageClass","value":"%s"}]`, classB)))
			Expect(err).NotTo(HaveOccurred(), "Failed to patch data storageClass")

			By("observing the migration take over (Ready reports StorageMigrating)")
			Eventually(func(g Gomega) {
				reason, err := getStatus("{.status.conditions[?(@.type=='Ready')].reason}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(reason).To(Equal("StorageMigrating"), "the migration should take over the layout")
			}, 3*time.Minute, 5*time.Second).Should(Succeed())

			By("migrating every node in turn: all three data PVCs end up on StorageClass B")
			Eventually(func(g Gomega) {
				for ordinal := 0; ordinal < 3; ordinal++ {
					class, err := utils.Run(exec.Command("kubectl", "get", "pvc",
						fmt.Sprintf("data-%s-%d", ssName, ordinal), "-n", clusterNamespace,
						"-o", "jsonpath={.spec.storageClassName}"))
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(class).To(Equal(classB), "node %d data PVC should be recreated on class B", ordinal)
				}
			}, 12*time.Minute, 10*time.Second).Should(Succeed())

			By("converging: migration cleared, meta untouched on A, cluster healthy with 3 nodes")
			Eventually(func(g Gomega) {
				phase, err := getStatus("{.status.storageMigration.phase}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(phase).To(BeEmpty(), "the migration should be complete")

				ready, err := getStatus("{.status.conditions[?(@.type=='Ready')].reason}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(ready).To(Equal("ClusterReady"), "the cluster should return to a steady Ready")

				// Only the data class was changed; the meta volume stays on A throughout.
				metaClass, err := utils.Run(exec.Command("kubectl", "get", "pvc",
					"meta-"+ssName+"-0", "-n", clusterNamespace, "-o", "jsonpath={.spec.storageClassName}"))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(metaClass).To(Equal(classA), "the meta volume's class should be unchanged")

				health, err := getStatus("{.status.health.status}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(health).To(Equal("healthy"), "cluster should be healthy after the migration")

				nodes, err := getStatus("{.status.layout.nodes[*].nodeId}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(utils.GetNonEmptyLines(strings.ReplaceAll(nodes, " ", "\n"))).
					To(HaveLen(3), "all three nodes should be back in the layout")
			}, 5*time.Minute, 10*time.Second).Should(Succeed())

			By("cleaning up the GarageCluster and StorageClasses")
			_, _ = utils.Run(exec.Command("kubectl", "delete", "-f", manifestFile))
			_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", clusterNamespace))
			_, _ = utils.Run(exec.Command("kubectl", "delete", "-f", scFile))
		})

		It("should take a metadata snapshot and launch a repair when annotated", Label("maintenance"), func() {
			const clusterNamespace = "garage-maint-e2e"
			const clusterName = "maint"

			By("preloading the Garage image into the Kind cluster")
			_, err := utils.Run(exec.Command("docker", "pull", garageImage))
			Expect(err).NotTo(HaveOccurred(), "Failed to pull the Garage image")
			Expect(utils.LoadImageToKindClusterWithName(garageImage)).To(Succeed(), "Failed to load Garage image into Kind")

			By("creating the maintenance-test namespace")
			_, _ = utils.Run(exec.Command("kubectl", "create", "ns", clusterNamespace))

			By("applying a single-node GarageCluster")
			manifest := fmt.Sprintf(`apiVersion: garage.rottler.io/v1alpha1
kind: GarageCluster
metadata:
  name: %s
  namespace: %s
spec:
  replicationFactor: 1
  nodePools:
    - name: default
      replicas: 1
      storage:
        data: { size: 1Gi }
        meta: { size: 1Gi }
`, clusterName, clusterNamespace)
			manifestFile := filepath.Join("/tmp", "garagecluster-maint-e2e.yaml")
			Expect(os.WriteFile(manifestFile, []byte(manifest), os.FileMode(0o644))).To(Succeed())
			_, err = utils.Run(exec.Command("kubectl", "apply", "-f", manifestFile))
			Expect(err).NotTo(HaveOccurred(), "Failed to apply GarageCluster")

			getStatus := func(jsonPath string) (string, error) {
				return utils.Run(exec.Command("kubectl", "get", "garagecluster", clusterName,
					"-n", clusterNamespace, "-o", "jsonpath="+jsonPath))
			}

			By("waiting for the cluster to become Ready")
			Eventually(func(g Gomega) {
				ready, err := getStatus("{.status.conditions[?(@.type=='Ready')].status}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(ready).To(Equal("True"), "GarageCluster is not Ready")
			}, 5*time.Minute, 5*time.Second).Should(Succeed())

			By("annotating the cluster to trigger a metadata snapshot and a blocks repair")
			_, err = utils.Run(exec.Command("kubectl", "annotate", "garagecluster", clusterName,
				"-n", clusterNamespace, "--overwrite",
				"garage.rottler.io/snapshot=snap-1", "garage.rottler.io/repair=blocks"))
			Expect(err).NotTo(HaveOccurred(), "Failed to annotate GarageCluster")

			By("asserting the snapshot succeeded and the repair launched against real Garage")
			Eventually(func(g Gomega) {
				snapTrigger, err := getStatus("{.status.maintenance.snapshot.observedTrigger}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(snapTrigger).To(Equal("snap-1"), "snapshot trigger not yet acted on")

				snapResult, err := getStatus("{.status.maintenance.snapshot.result}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(snapResult).To(Equal("Succeeded"), "real Garage rejected the metadata snapshot")

				repairType, err := getStatus("{.status.maintenance.repair.type}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(repairType).To(Equal("blocks"), "repair type not recorded")

				repairState, err := getStatus("{.status.maintenance.repair.state}")
				g.Expect(err).NotTo(HaveOccurred())
				// The repair is fire-and-forget; on a fresh cluster it may already be Done, but it
				// must never be Failed (real Garage accepted the launch).
				g.Expect(repairState).To(BeElementOf("Launched", "Running", "Done"),
					"repair was not launched successfully (state=%s)", repairState)
			}, 3*time.Minute, 5*time.Second).Should(Succeed())

			By("cleaning up the GarageCluster")
			_, _ = utils.Run(exec.Command("kubectl", "delete", "-f", manifestFile))
			_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", clusterNamespace))
		})

		It("should provision a GarageBucket against a single-node cluster", Label("bucket"), func() {
			const bucketNamespace = "garage-bucket-e2e"
			const clusterName = "bkt"
			const bucketName = "photos"

			By("preloading the Garage image into the Kind cluster")
			_, err := utils.Run(exec.Command("docker", "pull", garageImage))
			Expect(err).NotTo(HaveOccurred(), "Failed to pull the Garage image")
			Expect(utils.LoadImageToKindClusterWithName(garageImage)).To(Succeed(), "Failed to load Garage image into Kind")

			By("creating the bucket-test namespace")
			_, _ = utils.Run(exec.Command("kubectl", "create", "ns", bucketNamespace))

			By("applying a single-node GarageCluster to host the bucket")
			clusterManifest := fmt.Sprintf(`apiVersion: garage.rottler.io/v1alpha1
kind: GarageCluster
metadata:
  name: %s
  namespace: %s
spec:
  replicationFactor: 1
  nodePools:
    - name: default
      replicas: 1
      storage:
        data: { size: 1Gi }
        meta: { size: 1Gi }
`, clusterName, bucketNamespace)
			clusterFile := filepath.Join("/tmp", "garagecluster-bkt-e2e.yaml")
			Expect(os.WriteFile(clusterFile, []byte(clusterManifest), os.FileMode(0o644))).To(Succeed())
			_, err = utils.Run(exec.Command("kubectl", "apply", "-f", clusterFile))
			Expect(err).NotTo(HaveOccurred(), "Failed to apply GarageCluster")

			By("waiting for the hosting cluster to become Ready")
			Eventually(func(g Gomega) {
				ready, err := utils.Run(exec.Command("kubectl", "get", "garagecluster", clusterName,
					"-n", bucketNamespace, "-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}"))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(ready).To(Equal("True"), "hosting cluster is not Ready")
			}, 5*time.Minute, 5*time.Second).Should(Succeed())

			By("applying a GarageBucket with website, quotas, CORS and lifecycle rules")
			// The rich spec is deliberate: it exercises the CR -> Admin API translation
			// (CORS/lifecycle S3-shaped types, quota byte conversion) against real Garage, which
			// the unit/envtest tiers cannot validate.
			bucketManifest := fmt.Sprintf(`apiVersion: garage.rottler.io/v1alpha1
kind: GarageBucket
metadata:
  name: %s
  namespace: %s
spec:
  clusterRef:
    name: %s
  globalAliases: [%s]
  website:
    enabled: true
    indexDocument: index.html
    errorDocument: error.html
  quotas:
    maxSize: 1Gi
    maxObjects: 1000
  cors:
    - id: allow-web
      allowedOrigins: ["*"]
      allowedMethods: [GET, HEAD]
      allowedHeaders: ["*"]
      maxAgeSeconds: 3600
  lifecycle:
    - id: expire-tmp
      status: Enabled
      filter:
        prefix: tmp/
      expiration:
        days: 30
      abortIncompleteMultipartUpload:
        daysAfterInitiation: 7
  deletionPolicy: Retain
`, bucketName, bucketNamespace, clusterName, bucketName)
			bucketFile := filepath.Join("/tmp", "garagebucket-e2e.yaml")
			Expect(os.WriteFile(bucketFile, []byte(bucketManifest), os.FileMode(0o644))).To(Succeed())
			_, err = utils.Run(exec.Command("kubectl", "apply", "-f", bucketFile))
			Expect(err).NotTo(HaveOccurred(), "Failed to apply GarageBucket")

			By("waiting for the GarageBucket to become Ready with a bucketId")
			Eventually(func(g Gomega) {
				ready, err := utils.Run(exec.Command("kubectl", "get", "garagebucket", bucketName,
					"-n", bucketNamespace, "-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}"))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(ready).To(Equal("True"), "GarageBucket is not Ready (real Garage rejected the bucket config)")

				bucketID, err := utils.Run(exec.Command("kubectl", "get", "garagebucket", bucketName,
					"-n", bucketNamespace, "-o", "jsonpath={.status.bucketId}"))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(bucketID).NotTo(BeEmpty(), "bucketId should be recorded once the bucket exists in Garage")
			}, 3*time.Minute, 5*time.Second).Should(Succeed())

			By("deleting the GarageBucket and confirming the finalizer is released (Retain)")
			_, err = utils.Run(exec.Command("kubectl", "delete", "garagebucket", bucketName,
				"-n", bucketNamespace, "--timeout=60s"))
			Expect(err).NotTo(HaveOccurred(), "GarageBucket deletion did not complete (finalizer stuck)")

			By("cleaning up the hosting cluster")
			_, _ = utils.Run(exec.Command("kubectl", "delete", "-f", clusterFile))
			_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", bucketNamespace))
		})

		It("should provision a GarageKey and grant it on a bucket with a local alias", Label("key"), func() {
			const ns = "garage-key-e2e"
			const clusterName = "keys"
			const keyName = "appkey"
			const bucketName = "appdata"

			By("preloading the Garage image into the Kind cluster")
			_, err := utils.Run(exec.Command("docker", "pull", garageImage))
			Expect(err).NotTo(HaveOccurred(), "Failed to pull the Garage image")
			Expect(utils.LoadImageToKindClusterWithName(garageImage)).To(Succeed(), "Failed to load Garage image into Kind")

			By("creating the key-test namespace")
			_, _ = utils.Run(exec.Command("kubectl", "create", "ns", ns))

			By("applying a single-node GarageCluster to host the key and bucket")
			clusterManifest := fmt.Sprintf(`apiVersion: garage.rottler.io/v1alpha1
kind: GarageCluster
metadata:
  name: %s
  namespace: %s
spec:
  replicationFactor: 1
  nodePools:
    - name: default
      replicas: 1
      storage:
        data: { size: 1Gi }
        meta: { size: 1Gi }
`, clusterName, ns)
			clusterFile := filepath.Join("/tmp", "garagecluster-key-e2e.yaml")
			Expect(os.WriteFile(clusterFile, []byte(clusterManifest), os.FileMode(0o644))).To(Succeed())
			_, err = utils.Run(exec.Command("kubectl", "apply", "-f", clusterFile))
			Expect(err).NotTo(HaveOccurred(), "Failed to apply GarageCluster")

			By("waiting for the hosting cluster to become Ready")
			Eventually(func(g Gomega) {
				ready, err := utils.Run(exec.Command("kubectl", "get", "garagecluster", clusterName,
					"-n", ns, "-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}"))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(ready).To(Equal("True"), "hosting cluster is not Ready")
			}, 5*time.Minute, 5*time.Second).Should(Succeed())

			By("applying a GarageKey (create mode)")
			keyManifest := fmt.Sprintf(`apiVersion: garage.rottler.io/v1alpha1
kind: GarageKey
metadata:
  name: %s
  namespace: %s
spec:
  clusterRef:
    name: %s
  permissions:
    createBucket: false
  deletionPolicy: Delete
`, keyName, ns, clusterName)
			keyFile := filepath.Join("/tmp", "garagekey-e2e.yaml")
			Expect(os.WriteFile(keyFile, []byte(keyManifest), os.FileMode(0o644))).To(Succeed())
			_, err = utils.Run(exec.Command("kubectl", "apply", "-f", keyFile))
			Expect(err).NotTo(HaveOccurred(), "Failed to apply GarageKey")

			By("waiting for the GarageKey to become Ready with credentials published to a Secret")
			Eventually(func(g Gomega) {
				ready, err := utils.Run(exec.Command("kubectl", "get", "garagekey", keyName,
					"-n", ns, "-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}"))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(ready).To(Equal("True"), "GarageKey is not Ready (real Garage rejected the key)")

				keyID, err := utils.Run(exec.Command("kubectl", "get", "garagekey", keyName,
					"-n", ns, "-o", "jsonpath={.status.keyId}"))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(keyID).NotTo(BeEmpty(), "keyId should be recorded once the key exists in Garage")

				// The credentials Secret defaults to <cr-name>-credentials and must carry both halves.
				accessKey, err := utils.Run(exec.Command("kubectl", "get", "secret", keyName+"-credentials",
					"-n", ns, "-o", "jsonpath={.data.accessKeyId}"))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(accessKey).NotTo(BeEmpty(), "accessKeyId not published")
				secretKey, err := utils.Run(exec.Command("kubectl", "get", "secret", keyName+"-credentials",
					"-n", ns, "-o", "jsonpath={.data.secretAccessKey}"))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(secretKey).NotTo(BeEmpty(), "secretAccessKey not published")
			}, 3*time.Minute, 5*time.Second).Should(Succeed())

			By("applying a GarageBucket that grants the key and gives it a local alias")
			// Reaching Ready proves the grant (AllowBucketKey) and local alias (AddBucketAlias,
			// local variant) were accepted by real Garage — the Phase 2 carryover the unit/envtest
			// tiers cannot exercise.
			bucketManifest := fmt.Sprintf(`apiVersion: garage.rottler.io/v1alpha1
kind: GarageBucket
metadata:
  name: %s
  namespace: %s
spec:
  clusterRef:
    name: %s
  globalAliases: [%s]
  grants:
    - keyRef:
        name: %s
      read: true
      write: true
  localAliases:
    - keyRef:
        name: %s
      alias: mydata
  deletionPolicy: Retain
`, bucketName, ns, clusterName, bucketName, keyName, keyName)
			bucketFile := filepath.Join("/tmp", "garagebucket-key-e2e.yaml")
			Expect(os.WriteFile(bucketFile, []byte(bucketManifest), os.FileMode(0o644))).To(Succeed())
			_, err = utils.Run(exec.Command("kubectl", "apply", "-f", bucketFile))
			Expect(err).NotTo(HaveOccurred(), "Failed to apply GarageBucket")

			By("waiting for the GarageBucket to become Ready (grant + local alias applied)")
			Eventually(func(g Gomega) {
				ready, err := utils.Run(exec.Command("kubectl", "get", "garagebucket", bucketName,
					"-n", ns, "-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}"))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(ready).To(Equal("True"), "GarageBucket is not Ready (grant/local alias rejected)")
			}, 3*time.Minute, 5*time.Second).Should(Succeed())

			By("deleting the GarageKey and confirming the finalizer is released")
			_, err = utils.Run(exec.Command("kubectl", "delete", "garagekey", keyName,
				"-n", ns, "--timeout=60s"))
			Expect(err).NotTo(HaveOccurred(), "GarageKey deletion did not complete (finalizer stuck)")

			By("cleaning up")
			_, _ = utils.Run(exec.Command("kubectl", "delete", "garagebucket", bucketName, "-n", ns, "--timeout=60s"))
			_, _ = utils.Run(exec.Command("kubectl", "delete", "-f", clusterFile))
			_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", ns))
		})

		// The deployed CRDs carry CEL (x-kubernetes-validations) rules instead of an admission
		// webhook. A server-side dry-run exercises those rules in the real API server without
		// persisting anything or needing a running Garage cluster, proving the packaged CRDs
		// reject invalid specs end-to-end. (Field-by-field coverage lives in the envtest suite.)
		It("should reject invalid CRs via the CRD validation rules", Label("validation"), func() {
			applyDryRun := func(kind, manifest string) error {
				manifestFile := filepath.Join("/tmp", "garage-validation-"+kind+".yaml")
				Expect(os.WriteFile(manifestFile, []byte(manifest), os.FileMode(0o644))).To(Succeed())
				_, err := utils.Run(exec.Command("kubectl", "apply", "--dry-run=server", "-f", manifestFile))
				return err
			}

			By("rejecting a GarageCluster whose adminToken is Provided without a secretRef")
			err := applyDryRun("cluster", fmt.Sprintf(`apiVersion: garage.rottler.io/v1alpha1
kind: GarageCluster
metadata:
  name: invalid-cluster
  namespace: %s
spec:
  nodePools:
    - name: default
      replicas: 1
      storage:
        data: { size: 1Gi }
        meta: { size: 1Gi }
  adminToken:
    mode: Provided
`, namespace))
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("secretRef is required when mode is Provided"))

			By("rejecting a GarageBucket with duplicate globalAliases")
			err = applyDryRun("bucket", fmt.Sprintf(`apiVersion: garage.rottler.io/v1alpha1
kind: GarageBucket
metadata:
  name: invalid-bucket
  namespace: %s
spec:
  clusterRef:
    name: some-cluster
  globalAliases: [dup, dup]
`, namespace))
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Duplicate value"))

			By("rejecting a GarageKey with renewBefore but no expiration")
			err = applyDryRun("key", fmt.Sprintf(`apiVersion: garage.rottler.io/v1alpha1
kind: GarageKey
metadata:
  name: invalid-key
  namespace: %s
spec:
  clusterRef:
    name: some-cluster
  renewBefore: 168h
`, namespace))
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("renewBefore requires expiration"))

			By("accepting a valid GarageBucket")
			err = applyDryRun("valid-bucket", fmt.Sprintf(`apiVersion: garage.rottler.io/v1alpha1
kind: GarageBucket
metadata:
  name: valid-bucket
  namespace: %s
spec:
  clusterRef:
    name: some-cluster
  globalAliases: [photos, images]
`, namespace))
			Expect(err).NotTo(HaveOccurred(), "a valid GarageBucket should pass validation")
		})

		// +kubebuilder:scaffold:e2e-webhooks-checks

		// TODO: Customize the e2e test suite with scenarios specific to your project.
		// Consider applying sample/CR(s) and check their status and/or verifying
		// the reconciliation by using the metrics, i.e.:
		// metricsOutput, err := getMetricsOutput()
		// Expect(err).NotTo(HaveOccurred(), "Failed to retrieve logs from curl pod")
		// Expect(metricsOutput).To(ContainSubstring(
		//    fmt.Sprintf(`controller_runtime_reconcile_total{controller="%s",result="success"} 1`,
		//    strings.ToLower(<Kind>),
		// ))
	})
})

// serviceAccountToken returns a token for the specified service account in the given namespace.
// It uses the Kubernetes TokenRequest API to generate a token by directly sending a request
// and parsing the resulting token from the API response.
func serviceAccountToken() (string, error) {
	const tokenRequestRawString = `{
		"apiVersion": "authentication.k8s.io/v1",
		"kind": "TokenRequest"
	}`

	By("creating temporary file to store the token request")
	secretName := fmt.Sprintf("%s-token-request", serviceAccountName)
	tokenRequestFile := filepath.Join("/tmp", secretName)
	err := os.WriteFile(tokenRequestFile, []byte(tokenRequestRawString), os.FileMode(0o644))
	if err != nil {
		return "", err
	}

	var out string
	verifyTokenCreation := func(g Gomega) {
		By("executing kubectl command to create the token")
		cmd := exec.Command("kubectl", "create", "--raw", fmt.Sprintf(
			"/api/v1/namespaces/%s/serviceaccounts/%s/token",
			namespace,
			serviceAccountName,
		), "-f", tokenRequestFile)

		output, err := cmd.CombinedOutput()
		g.Expect(err).NotTo(HaveOccurred())

		By("parsing the JSON output to extract the token")
		var token tokenRequest
		err = json.Unmarshal(output, &token)
		g.Expect(err).NotTo(HaveOccurred())

		out = token.Status.Token
	}
	Eventually(verifyTokenCreation).Should(Succeed())

	return out, err
}

// getMetricsOutput retrieves and returns the logs from the curl pod used to access the metrics endpoint.
func getMetricsOutput() (string, error) {
	By("getting the curl-metrics logs")
	cmd := exec.Command("kubectl", "logs", "curl-metrics", "-n", namespace)
	return utils.Run(cmd)
}

// tokenRequest is a simplified representation of the Kubernetes TokenRequest API response,
// containing only the token field that we need to extract.
type tokenRequest struct {
	Status struct {
		Token string `json:"token"`
	} `json:"status"`
}
