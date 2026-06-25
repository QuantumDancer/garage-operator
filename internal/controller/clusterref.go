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
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"

	garagev1alpha1 "github.com/QuantumDancer/garage-operator/api/v1alpha1"
)

// resolveState classifies the outcome of resolving a clusterRef, so callers can react
// differently to "cluster gone" vs "cluster not ready yet" vs "good to go".
type resolveState int

const (
	// resolveReady means the cluster exists, is Ready, and its admin endpoint is returned.
	resolveReady resolveState = iota
	// resolveClusterMissing means the referenced GarageCluster does not exist.
	resolveClusterMissing
	// resolveClusterNotReady means the cluster exists but has not reported Ready yet.
	resolveClusterNotReady
)

// clusterConnection is everything a bucket/key controller needs to reach a cluster's Admin API.
type clusterConnection struct {
	baseURL string
	token   string
}

// resolveClusterConnection loads the GarageCluster named by ref (defaulting the namespace to
// defaultNamespace) and, when it is Ready, returns the admin endpoint and token. A returned
// error is reserved for genuine failures (e.g. the admin-token Secret is unreadable); the
// expected "not found"/"not ready" outcomes are reported via resolveState with a nil error so
// callers requeue rather than back off.
func resolveClusterConnection(
	ctx context.Context,
	c client.Client,
	ref garagev1alpha1.ClusterReference,
	defaultNamespace string,
) (clusterConnection, resolveState, error) {
	namespace := ref.Namespace
	if namespace == "" {
		namespace = defaultNamespace
	}

	var cluster garagev1alpha1.GarageCluster
	if err := c.Get(ctx, client.ObjectKey{Name: ref.Name, Namespace: namespace}, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return clusterConnection{}, resolveClusterMissing, nil
		}
		return clusterConnection{}, resolveClusterMissing, err
	}

	if !meta.IsStatusConditionTrue(cluster.Status.Conditions, conditionReady) {
		return clusterConnection{}, resolveClusterNotReady, nil
	}

	tokenRef := resolveAdminTokenSecret(&cluster)
	var secret corev1.Secret
	if err := c.Get(ctx, client.ObjectKey{Name: tokenRef.name, Namespace: cluster.Namespace}, &secret); err != nil {
		return clusterConnection{}, resolveClusterNotReady, err
	}
	token, ok := secret.Data[tokenRef.key]
	if !ok {
		return clusterConnection{}, resolveClusterNotReady,
			fmt.Errorf("admin token Secret %q is missing key %q", tokenRef.name, tokenRef.key)
	}

	return clusterConnection{baseURL: clusterAdminBaseURL(&cluster), token: string(token)}, resolveReady, nil
}

// clusterAdminBaseURL is the Admin API URL of the first pool's node 0, addressed over per-pod
// headless DNS. The admin API must be reached at a specific pod, never a load-balanced
// Service: some admin operations are node-local, and the layout is cluster-wide so any single
// reachable node serves bucket/key calls.
func clusterAdminBaseURL(c *garagev1alpha1.GarageCluster) string {
	pool := &c.Spec.NodePools[0]
	pod := fmt.Sprintf("%s-0", statefulSetName(c, pool))
	return fmt.Sprintf("http://%s.%s.%s.svc:%d", pod, headlessServiceName(c), c.Namespace, portAdmin)
}
