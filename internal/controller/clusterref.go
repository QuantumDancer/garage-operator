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
	// resolveClusterUnknown is the zero value and is returned on every error path. The
	// state is meaningless whenever the accompanying error is non-nil, so callers must
	// check the error first; making the zero value a neutral sentinel ensures a caller
	// that (incorrectly) switches on the state first never mistakes an apiserver blip for
	// a deleted cluster and drops a finalizer.
	resolveClusterUnknown resolveState = iota
	// resolveReady means the cluster exists, is Ready, and its admin endpoint is returned.
	resolveReady
	// resolveClusterMissing means the referenced GarageCluster does not exist.
	resolveClusterMissing
	// resolveClusterNotReady means the cluster exists but has not reported Ready yet.
	resolveClusterNotReady
)

// clusterConnection is everything a bucket/key controller needs to reach a cluster's Admin API.
// baseURLs is ordered most-preferred first; callers try each in turn via firstReachableAdmin.
type clusterConnection struct {
	baseURLs []string
	token    string
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
		return clusterConnection{}, resolveClusterUnknown, err
	}

	if !meta.IsStatusConditionTrue(cluster.Status.Conditions, conditionReady) {
		return clusterConnection{}, resolveClusterNotReady, nil
	}

	tokenRef := resolveAdminTokenSecret(&cluster)
	var secret corev1.Secret
	if err := c.Get(ctx, client.ObjectKey{Name: tokenRef.name, Namespace: cluster.Namespace}, &secret); err != nil {
		return clusterConnection{}, resolveClusterUnknown, err
	}
	token, ok := secret.Data[tokenRef.key]
	if !ok {
		return clusterConnection{}, resolveClusterUnknown,
			fmt.Errorf("admin token Secret %q is missing key %q", tokenRef.name, tokenRef.key)
	}

	return clusterConnection{baseURLs: clusterAdminBaseURLs(&cluster), token: string(token)}, resolveReady, nil
}

// podDNSName is a pod's stable per-pod headless DNS name, "<pod>.<headless>.<ns>.svc".
func podDNSName(c *garagev1alpha1.GarageCluster, pod string) string {
	return fmt.Sprintf("%s.%s.%s.svc", pod, headlessServiceName(c), c.Namespace)
}

// adminURLForPod is the Admin API URL of one pod, addressed over per-pod headless DNS.
func adminURLForPod(c *garagev1alpha1.GarageCluster, pod string) string {
	return fmt.Sprintf("http://%s:%d", podDNSName(c, pod), portAdmin)
}

// clusterAdminBaseURLs returns the candidate Admin API endpoints for a cluster's bucket/key
// operations, most-preferred first. The admin API must be reached at a specific pod (never a
// load-balanced Service): some operations are node-local, but the layout is cluster-wide so any
// single reachable node serves bucket/key calls. Prefer the laid-out nodes from status so a
// single down pod no longer wedges every operation; before the first layout is recorded, fall
// back to pool-0/pod-0.
func clusterAdminBaseURLs(c *garagev1alpha1.GarageCluster) []string {
	if c.Status.Layout != nil && len(c.Status.Layout.Nodes) > 0 {
		urls := make([]string, 0, len(c.Status.Layout.Nodes))
		for i := range c.Status.Layout.Nodes {
			urls = append(urls, adminURLForPod(c, c.Status.Layout.Nodes[i].Pod))
		}
		return urls
	}
	pod := podName(statefulSetName(c, &c.Spec.NodePools[0]), 0)
	return []string{adminURLForPod(c, pod)}
}

// reachableProbe is satisfied by any admin client that can confirm its endpoint is answering.
type reachableProbe interface {
	NodeID(ctx context.Context) (string, error)
}

// firstReachableAdmin builds an admin client for each candidate endpoint in turn and returns the
// first whose node answers a NodeID probe — the same liveness check discoverNodes uses. Bucket
// and key admin operations are cluster-wide, so any reachable node serves them; this removes the
// single-pod SPOF of always targeting pool-0/pod-0. It errors only when NO candidate is
// reachable, which callers treat as "requeue", never as "cluster gone" (so no finalizer drops).
func firstReachableAdmin[T reachableProbe](
	ctx context.Context,
	factory func(baseURL, token string) (T, error),
	baseURLs []string,
	token string,
) (T, error) {
	var zero T
	var lastErr error
	for _, baseURL := range baseURLs {
		admin, err := factory(baseURL, token)
		if err != nil {
			lastErr = err
			continue
		}
		if _, err := admin.NodeID(ctx); err != nil {
			lastErr = err
			continue
		}
		return admin, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no candidate admin endpoints")
	}
	return zero, fmt.Errorf("no reachable Garage admin endpoint: %w", lastErr)
}
