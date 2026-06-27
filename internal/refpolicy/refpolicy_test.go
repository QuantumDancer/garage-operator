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

package refpolicy

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	garagev1alpha1 "github.com/QuantumDancer/garage-operator/api/v1alpha1"
)

const (
	clusterName = "homelab"
	clusterNS   = "storage"
	nsMedia     = "media"
	labelTenant = "tenant"
	labelTrue   = "true"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := garagev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add garage scheme: %v", err)
	}
	return scheme
}

func clusterWithPolicy(policy *garagev1alpha1.ReferencePolicy) *garagev1alpha1.GarageCluster {
	return &garagev1alpha1.GarageCluster{
		ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: clusterNS},
		Spec:       garagev1alpha1.GarageClusterSpec{ReferencePolicy: policy},
	}
}

func newReader(t *testing.T, objs ...client.Object) client.Reader {
	t.Helper()
	return fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(objs...).Build()
}

func namespaceWithLabels(name string, labels map[string]string) *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}}
}

// TestCheck exercises the full decision matrix through Check, the entry point both the webhook
// and the controller backstop call.
func TestCheck(t *testing.T) {
	tests := []struct {
		name        string
		policy      *garagev1alpha1.ReferencePolicy
		refNS       string
		extraNS     *corev1.Namespace // a Namespace object for selector cases
		wantAllowed bool
	}{
		{
			name:        "nil policy allows any namespace",
			policy:      nil,
			refNS:       nsMedia,
			wantAllowed: true,
		},
		{
			name:        "own namespace is always allowed even under an empty policy",
			policy:      &garagev1alpha1.ReferencePolicy{},
			refNS:       clusterNS,
			wantAllowed: true,
		},
		{
			name:        "empty policy denies a foreign namespace",
			policy:      &garagev1alpha1.ReferencePolicy{},
			refNS:       nsMedia,
			wantAllowed: false,
		},
		{
			name:        "allowedNamespaces match is allowed",
			policy:      &garagev1alpha1.ReferencePolicy{AllowedNamespaces: []string{nsMedia, "backups"}},
			refNS:       nsMedia,
			wantAllowed: true,
		},
		{
			name:        "allowedNamespaces miss is denied",
			policy:      &garagev1alpha1.ReferencePolicy{AllowedNamespaces: []string{nsMedia}},
			refNS:       "rogue",
			wantAllowed: false,
		},
		{
			name: "namespaceSelector match is allowed",
			policy: &garagev1alpha1.ReferencePolicy{
				NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{labelTenant: labelTrue}},
			},
			refNS:       nsMedia,
			extraNS:     namespaceWithLabels(nsMedia, map[string]string{labelTenant: labelTrue}),
			wantAllowed: true,
		},
		{
			name: "namespaceSelector miss is denied",
			policy: &garagev1alpha1.ReferencePolicy{
				NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{labelTenant: labelTrue}},
			},
			refNS:       nsMedia,
			extraNS:     namespaceWithLabels(nsMedia, map[string]string{labelTenant: "false"}),
			wantAllowed: false,
		},
		{
			name: "empty namespaceSelector grants nothing (fails closed, not allow-all)",
			policy: &garagev1alpha1.ReferencePolicy{
				NamespaceSelector: &metav1.LabelSelector{},
			},
			refNS:       nsMedia,
			extraNS:     namespaceWithLabels(nsMedia, map[string]string{labelTenant: labelTrue}),
			wantAllowed: false,
		},
		{
			name: "allowedNamespaces and selector are OR-ed (name matches, selector would not)",
			policy: &garagev1alpha1.ReferencePolicy{
				AllowedNamespaces: []string{nsMedia},
				NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{labelTenant: labelTrue}},
			},
			refNS:       nsMedia,
			wantAllowed: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objs := []client.Object{clusterWithPolicy(tt.policy)}
			if tt.extraNS != nil {
				objs = append(objs, tt.extraNS)
			}
			reader := newReader(t, objs...)

			ref := garagev1alpha1.ClusterReference{Name: clusterName, Namespace: clusterNS}
			allowed, reason, err := Check(context.Background(), reader, ref, tt.refNS)
			if err != nil {
				t.Fatalf("Check: unexpected error: %v", err)
			}
			if allowed != tt.wantAllowed {
				t.Errorf("allowed = %v, want %v (reason: %q)", allowed, tt.wantAllowed, reason)
			}
			if !allowed && reason == "" {
				t.Error("a denied reference must carry a non-empty reason")
			}
			if allowed && reason != "" {
				t.Errorf("an allowed reference must not carry a reason, got %q", reason)
			}
		})
	}
}

// TestCheckMissingClusterIsAllowed verifies that a not-yet-created cluster is permitted, so the
// webhook never blocks a GitOps apply on apply-ordering; existence is the controller's concern.
func TestCheckMissingClusterIsAllowed(t *testing.T) {
	reader := newReader(t) // no cluster object
	ref := garagev1alpha1.ClusterReference{Name: "ghost", Namespace: clusterNS}

	allowed, reason, err := Check(context.Background(), reader, ref, nsMedia)
	if err != nil {
		t.Fatalf("Check: unexpected error: %v", err)
	}
	if !allowed {
		t.Errorf("a missing cluster must be allowed, got denied (reason: %q)", reason)
	}
}

// TestCheckDefaultsClusterNamespace verifies that an omitted clusterRef.namespace defaults to
// the referencing object's namespace, matching the controllers' resolution.
func TestCheckDefaultsClusterNamespace(t *testing.T) {
	// Cluster and referrer share namespace clusterNS; the ref omits the namespace.
	reader := newReader(t, clusterWithPolicy(&garagev1alpha1.ReferencePolicy{}))
	ref := garagev1alpha1.ClusterReference{Name: clusterName} // no namespace

	allowed, _, err := Check(context.Background(), reader, ref, clusterNS)
	if err != nil {
		t.Fatalf("Check: unexpected error: %v", err)
	}
	if !allowed {
		t.Error("a same-namespace reference with a defaulted clusterRef namespace must be allowed")
	}
}
