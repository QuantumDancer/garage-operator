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

// Package refpolicy evaluates a GarageCluster's spec.referencePolicy: it decides whether a
// GarageBucket / GarageKey in a given namespace may target that cluster via clusterRef. The
// same decision backs two enforcement points — the validating admission webhook (fast
// rejection at apply time) and the controllers (the hard backstop, since admission only fires
// on the bucket/key, never when the cluster's policy is tightened later).
package refpolicy

import (
	"context"
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	garagev1alpha1 "github.com/QuantumDancer/garage-operator/api/v1alpha1"
)

// Check loads the GarageCluster named by ref (defaulting the cluster namespace to refNamespace,
// matching the controllers' clusterRef resolution) and reports whether a reference originating
// from refNamespace is permitted by its referencePolicy.
//
// A cluster that does not exist yet is treated as allowed: existence and readiness are the
// controller's concern, and hard-rejecting a missing reference at admission would fight GitOps
// apply-ordering. The boolean is the decision; the string is a human-readable reason when the
// reference is denied (empty otherwise). A non-nil error signals a read failure the caller must
// handle (the webhook fails open; the controller requeues).
func Check(
	ctx context.Context,
	reader client.Reader,
	ref garagev1alpha1.ClusterReference,
	refNamespace string,
) (allowed bool, reason string, err error) {
	clusterNamespace := ref.Namespace
	if clusterNamespace == "" {
		clusterNamespace = refNamespace
	}

	var cluster garagev1alpha1.GarageCluster
	if getErr := reader.Get(ctx, client.ObjectKey{Name: ref.Name, Namespace: clusterNamespace}, &cluster); getErr != nil {
		if apierrors.IsNotFound(getErr) {
			return true, "", nil
		}
		return false, "", getErr
	}

	return Allowed(ctx, reader, &cluster, refNamespace)
}

// Allowed evaluates the cluster's referencePolicy against a reference from refNamespace. A
// reference is permitted when refNamespace is the cluster's own namespace, OR appears in
// allowedNamespaces, OR the referencing namespace's labels match namespaceSelector. A nil
// policy permits everything (the default, unrestricted behavior). The namespace's labels are
// read through reader only when a namespaceSelector is set.
func Allowed(
	ctx context.Context,
	reader client.Reader,
	cluster *garagev1alpha1.GarageCluster,
	refNamespace string,
) (allowed bool, reason string, err error) {
	policy := cluster.Spec.ReferencePolicy
	if policy == nil {
		return true, "", nil
	}
	if refNamespace == cluster.Namespace {
		return true, "", nil
	}
	if slices.Contains(policy.AllowedNamespaces, refNamespace) {
		return true, "", nil
	}

	if policy.NamespaceSelector != nil {
		selector, selErr := metav1.LabelSelectorAsSelector(policy.NamespaceSelector)
		if selErr != nil {
			return false, "", selErr
		}
		// An empty selector ({}) converts to labels.Everything(), which matches every
		// namespace. In a deny-by-default policy that silently flips a restrictive rule into
		// allow-all, so we fail closed and treat an empty selector as granting nothing.
		if !selector.Empty() {
			var namespace corev1.Namespace
			if getErr := reader.Get(ctx, client.ObjectKey{Name: refNamespace}, &namespace); getErr != nil {
				return false, "", getErr
			}
			if selector.Matches(labels.Set(namespace.Labels)) {
				return true, "", nil
			}
		}
	}

	return false, deniedReason(cluster, refNamespace), nil
}

func deniedReason(cluster *garagev1alpha1.GarageCluster, refNamespace string) string {
	return fmt.Sprintf(
		"namespace %q is not permitted to reference GarageCluster %q/%q by its referencePolicy",
		refNamespace, cluster.Namespace, cluster.Name,
	)
}
