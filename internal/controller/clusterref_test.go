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
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// clusterGVR identifies the GarageCluster resource for the NotFound error constructed below;
// the fake client interceptor needs a concrete GroupResource to build a realistic API error.
var clusterGVR = schema.GroupResource{Group: "garage.quantumdancer.dev", Resource: "garageclusters"}

// TestResolveClusterConnection_TransientGetError is the regression guard for REVIEW.md #15: a
// transient (non-NotFound) failure reading the GarageCluster must surface as
// resolveClusterUnknown, never resolveClusterMissing. Before the fix, this branch returned
// resolveClusterMissing alongside the error; a caller that (incorrectly) switched on the state
// before checking the error would treat an apiserver blip as "cluster deleted" and drop a
// bucket/key finalizer, releasing the underlying Garage resource.
func TestResolveClusterConnection_TransientGetError(t *testing.T) {
	scheme := bucketTestScheme(t)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				return apierrors.NewServiceUnavailable("apiserver blip")
			},
		}).
		Build()

	_, state, err := resolveClusterConnection(context.Background(), c, clusterRef(), testClusterNS)

	if err == nil {
		t.Fatal("resolveClusterConnection() returned nil error for a transient Get failure")
	}
	if state == resolveClusterMissing {
		t.Errorf("resolveClusterConnection() returned resolveClusterMissing for a transient error; "+
			"want resolveClusterUnknown, since a caller switching on state before checking err would "+
			"mistake this apiserver blip for a deleted cluster and drop the finalizer (got state=%v)", state)
	}
	if state != resolveClusterUnknown {
		t.Errorf("resolveClusterConnection() state = %v, want resolveClusterUnknown", state)
	}
}

// TestResolveClusterConnection_NotFound guards the unchanged branch: a genuine NotFound must
// keep reporting resolveClusterMissing with a nil error, since callers rely on the nil error to
// requeue rather than back off.
func TestResolveClusterConnection_NotFound(t *testing.T) {
	scheme := bucketTestScheme(t)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				return apierrors.NewNotFound(clusterGVR, key.Name)
			},
		}).
		Build()

	_, state, err := resolveClusterConnection(context.Background(), c, clusterRef(), testClusterNS)

	if err != nil {
		t.Fatalf("resolveClusterConnection() error = %v, want nil for NotFound", err)
	}
	if state != resolveClusterMissing {
		t.Errorf("resolveClusterConnection() state = %v, want resolveClusterMissing", state)
	}
}
