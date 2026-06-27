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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	garagev1alpha1 "github.com/QuantumDancer/garage-operator/api/v1alpha1"
	"github.com/QuantumDancer/garage-operator/internal/garageadmin"
)

// fakeBucketAdmin is an in-memory stand-in for the bucket slice of the Admin API. It records
// the calls the controller makes so tests can assert on convergence behaviour.
type fakeBucketAdmin struct {
	buckets map[string]*garageadmin.GetBucketInfoResponse // by id
	aliases map[string]string                             // global alias -> bucket id
	nextID  int

	createCalls       int
	updateCalls       int
	deleteCalls       int
	addedAlias        []string
	removedAlias      []string
	allowCalls        []string
	denyCalls         []string
	addedLocalAlias   []string
	removedLocalAlias []string
}

func newFakeBucketAdmin() *fakeBucketAdmin {
	return &fakeBucketAdmin{
		buckets: map[string]*garageadmin.GetBucketInfoResponse{},
		aliases: map[string]string{},
	}
}

func (f *fakeBucketAdmin) GetBucketByID(_ context.Context, id string) (*garageadmin.GetBucketInfoResponse, bool, error) {
	b, ok := f.buckets[id]
	return b, ok, nil
}

func (f *fakeBucketAdmin) GetBucketByGlobalAlias(_ context.Context, alias string) (*garageadmin.GetBucketInfoResponse, bool, error) {
	id, ok := f.aliases[alias]
	if !ok {
		return nil, false, nil
	}
	return f.buckets[id], true, nil
}

func (f *fakeBucketAdmin) CreateBucket(_ context.Context, globalAlias string) (string, error) {
	f.createCalls++
	f.nextID++
	id := fmt.Sprintf("bucket-%d", f.nextID)
	info := &garageadmin.GetBucketInfoResponse{Id: id}
	if globalAlias != "" {
		info.GlobalAliases = []string{globalAlias}
		f.aliases[globalAlias] = id
	}
	f.buckets[id] = info
	return id, nil
}

func (f *fakeBucketAdmin) UpdateBucket(_ context.Context, _ string, _ garageadmin.UpdateBucketRequestBody) error {
	f.updateCalls++
	return nil
}

func (f *fakeBucketAdmin) AddBucketGlobalAlias(_ context.Context, bucketID, alias string) error {
	f.addedAlias = append(f.addedAlias, alias)
	f.aliases[alias] = bucketID
	if b := f.buckets[bucketID]; b != nil {
		b.GlobalAliases = append(b.GlobalAliases, alias)
	}
	return nil
}

func (f *fakeBucketAdmin) RemoveBucketGlobalAlias(_ context.Context, bucketID, alias string) error {
	f.removedAlias = append(f.removedAlias, alias)
	delete(f.aliases, alias)
	if b := f.buckets[bucketID]; b != nil {
		kept := b.GlobalAliases[:0]
		for _, a := range b.GlobalAliases {
			if a != alias {
				kept = append(kept, a)
			}
		}
		b.GlobalAliases = kept
	}
	return nil
}

func (f *fakeBucketAdmin) DeleteBucket(_ context.Context, id string) error {
	f.deleteCalls++
	delete(f.buckets, id)
	return nil
}

func (f *fakeBucketAdmin) keyPerm(bucketID, accessKeyID string) *garageadmin.ApiBucketKeyPerm {
	b := f.buckets[bucketID]
	if b == nil {
		return nil
	}
	for i := range b.Keys {
		if b.Keys[i].AccessKeyId == accessKeyID {
			return &b.Keys[i].Permissions
		}
	}
	b.Keys = append(b.Keys, garageadmin.GetBucketInfoKey{AccessKeyId: accessKeyID})
	return &b.Keys[len(b.Keys)-1].Permissions
}

func (f *fakeBucketAdmin) AllowBucketKey(_ context.Context, bucketID, accessKeyID string, perm garageadmin.ApiBucketKeyPerm) error {
	f.allowCalls = append(f.allowCalls, accessKeyID)
	cur := f.keyPerm(bucketID, accessKeyID)
	applyPerm(cur, perm, true)
	return nil
}

func (f *fakeBucketAdmin) DenyBucketKey(_ context.Context, bucketID, accessKeyID string, perm garageadmin.ApiBucketKeyPerm) error {
	f.denyCalls = append(f.denyCalls, accessKeyID)
	cur := f.keyPerm(bucketID, accessKeyID)
	applyPerm(cur, perm, false)
	return nil
}

// applyPerm sets the permissions named in chg (non-nil fields) to value on dst.
func applyPerm(dst *garageadmin.ApiBucketKeyPerm, chg garageadmin.ApiBucketKeyPerm, value bool) {
	if chg.Read != nil {
		dst.Read = &value
	}
	if chg.Write != nil {
		dst.Write = &value
	}
	if chg.Owner != nil {
		dst.Owner = &value
	}
}

func (f *fakeBucketAdmin) keyEntry(bucketID, accessKeyID string) *garageadmin.GetBucketInfoKey {
	b := f.buckets[bucketID]
	if b == nil {
		return nil
	}
	for i := range b.Keys {
		if b.Keys[i].AccessKeyId == accessKeyID {
			return &b.Keys[i]
		}
	}
	b.Keys = append(b.Keys, garageadmin.GetBucketInfoKey{AccessKeyId: accessKeyID})
	return &b.Keys[len(b.Keys)-1]
}

func (f *fakeBucketAdmin) AddBucketLocalAlias(_ context.Context, bucketID, accessKeyID, alias string) error {
	f.addedLocalAlias = append(f.addedLocalAlias, alias)
	k := f.keyEntry(bucketID, accessKeyID)
	k.BucketLocalAliases = append(k.BucketLocalAliases, alias)
	return nil
}

func (f *fakeBucketAdmin) RemoveBucketLocalAlias(_ context.Context, bucketID, accessKeyID, alias string) error {
	f.removedLocalAlias = append(f.removedLocalAlias, alias)
	k := f.keyEntry(bucketID, accessKeyID)
	kept := k.BucketLocalAliases[:0]
	for _, a := range k.BucketLocalAliases {
		if a != alias {
			kept = append(kept, a)
		}
	}
	k.BucketLocalAliases = kept
	return nil
}

func bucketTestScheme(t *testing.T) *runtime.Scheme {
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

func storagePool() garagev1alpha1.NodePool {
	return garagev1alpha1.NodePool{
		Name:     testPoolName,
		Replicas: 1,
		Storage: garagev1alpha1.NodePoolStorage{
			Data: garagev1alpha1.StorageSpec{Size: resource.MustParse("1Gi")},
			Meta: garagev1alpha1.StorageSpec{Size: resource.MustParse("1Gi")},
		},
	}
}

// readyCluster builds a GarageCluster reporting Ready, plus its admin-token Secret, so the
// resolver returns a usable connection.
func readyCluster() (*garagev1alpha1.GarageCluster, *corev1.Secret) {
	cluster := &garagev1alpha1.GarageCluster{
		ObjectMeta: metav1.ObjectMeta{Name: testClusterName, Namespace: testClusterNS},
		Spec:       garagev1alpha1.GarageClusterSpec{NodePools: []garagev1alpha1.NodePool{storagePool()}},
		Status: garagev1alpha1.GarageClusterStatus{
			Conditions: []metav1.Condition{{
				Type:               conditionReady,
				Status:             metav1.ConditionTrue,
				Reason:             reasonClusterReady,
				LastTransitionTime: metav1.Now(),
			}},
		},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: testClusterName + "-admin-token", Namespace: testClusterNS},
		Data:       map[string][]byte{secretKeyToken: []byte("test-token")},
	}
	return cluster, secret
}

func clusterRef() garagev1alpha1.ClusterReference {
	return garagev1alpha1.ClusterReference{Name: testClusterName, Namespace: testClusterNS}
}

func newBucketReconciler(t *testing.T, admin bucketAdmin, objs ...client.Object) (*GarageBucketReconciler, client.Client) {
	t.Helper()
	scheme := bucketTestScheme(t)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(&garagev1alpha1.GarageBucket{}).
		Build()
	return &GarageBucketReconciler{
		Client:         c,
		Scheme:         scheme,
		NewAdminClient: func(string, string) (bucketAdmin, error) { return admin, nil },
		Recorder:       record.NewFakeRecorder(100),
	}, c
}

func reconcileBucket(t *testing.T, r *GarageBucketReconciler) ctrl.Result {
	t.Helper()
	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: testBucketName, Namespace: testBucketNS},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	return res
}

func getBucket(t *testing.T, c client.Client) *garagev1alpha1.GarageBucket {
	t.Helper()
	var b garagev1alpha1.GarageBucket
	if err := c.Get(context.Background(), types.NamespacedName{Name: testBucketName, Namespace: testBucketNS}, &b); err != nil {
		t.Fatalf("get bucket: %v", err)
	}
	return &b
}

// bucketCR builds a GarageBucket fixture targeting the shared test cluster.
func bucketCR(finalized bool, spec garagev1alpha1.GarageBucketSpec) *garagev1alpha1.GarageBucket {
	spec.ClusterRef = clusterRef()
	objMeta := metav1.ObjectMeta{Name: testBucketName, Namespace: testBucketNS}
	if finalized {
		objMeta.Finalizers = []string{bucketFinalizer}
	}
	return &garagev1alpha1.GarageBucket{ObjectMeta: objMeta, Spec: spec}
}

func TestBucketReconcileAddsFinalizer(t *testing.T) {
	admin := newFakeBucketAdmin()
	r, c := newBucketReconciler(t, admin, bucketCR(false, garagev1alpha1.GarageBucketSpec{}))

	reconcileBucket(t, r)

	got := getBucket(t, c)
	if !controllerutil.ContainsFinalizer(got, bucketFinalizer) {
		t.Fatal("expected finalizer to be added on first reconcile")
	}
	if admin.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0 before finalizer is established", admin.createCalls)
	}
}

func TestBucketReconcilePendingWhenClusterNotReady(t *testing.T) {
	notReady := &garagev1alpha1.GarageCluster{
		ObjectMeta: metav1.ObjectMeta{Name: testClusterName, Namespace: testClusterNS},
		Spec:       garagev1alpha1.GarageClusterSpec{NodePools: []garagev1alpha1.NodePool{storagePool()}},
	}
	admin := newFakeBucketAdmin()
	r, c := newBucketReconciler(t, admin, bucketCR(true, garagev1alpha1.GarageBucketSpec{}), notReady)

	res := reconcileBucket(t, r)
	if res.RequeueAfter == 0 {
		t.Error("expected a requeue while the cluster is not Ready")
	}

	cond := meta.FindStatusCondition(getBucket(t, c).Status.Conditions, conditionReady)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != reasonClusterNotReady {
		t.Fatalf("Ready condition = %+v, want False/ClusterNotReady", cond)
	}
	if admin.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0 while pending", admin.createCalls)
	}
}

// TestBucketReconcileDeniedByReferencePolicy is the controller backstop for the cluster's
// referencePolicy: a bucket whose namespace the policy forbids must report
// ReferenceNotAllowed and never touch the Admin API, even though the cluster is Ready.
func TestBucketReconcileDeniedByReferencePolicy(t *testing.T) {
	cluster, secret := readyCluster()
	// testBucketNS ("media") is not in the allow-list and the cluster lives in "storage".
	cluster.Spec.ReferencePolicy = &garagev1alpha1.ReferencePolicy{AllowedNamespaces: []string{"other"}}
	admin := newFakeBucketAdmin()
	r, c := newBucketReconciler(t, admin, bucketCR(true, garagev1alpha1.GarageBucketSpec{}), cluster, secret)

	reconcileBucket(t, r)

	cond := meta.FindStatusCondition(getBucket(t, c).Status.Conditions, conditionReady)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != reasonReferenceNotAllowed {
		t.Fatalf("Ready condition = %+v, want False/ReferenceNotAllowed", cond)
	}
	if admin.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0 for a denied reference", admin.createCalls)
	}
}

func TestBucketReconcileCreatesBucket(t *testing.T) {
	cluster, secret := readyCluster()
	bucket := bucketCR(true, garagev1alpha1.GarageBucketSpec{GlobalAliases: []string{testBucketName}})
	admin := newFakeBucketAdmin()
	r, c := newBucketReconciler(t, admin, bucket, cluster, secret)

	reconcileBucket(t, r)

	if admin.createCalls != 1 {
		t.Fatalf("createCalls = %d, want 1", admin.createCalls)
	}
	if admin.updateCalls != 1 {
		t.Errorf("updateCalls = %d, want 1 (website/quotas/cors/lifecycle pushed)", admin.updateCalls)
	}
	got := getBucket(t, c)
	if got.Status.BucketID == "" {
		t.Error("status.bucketId not recorded after creation")
	}
	cond := meta.FindStatusCondition(got.Status.Conditions, conditionReady)
	if cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("Ready condition = %+v, want True", cond)
	}
}

func TestBucketReconcileConvergesAliases(t *testing.T) {
	cluster, secret := readyCluster()
	admin := newFakeBucketAdmin()
	// Seed an existing bucket carrying a stale alias, already bound via status.
	admin.buckets[testBucketID] = &garageadmin.GetBucketInfoResponse{Id: testBucketID, GlobalAliases: []string{"old"}}
	admin.aliases["old"] = testBucketID

	bucket := bucketCR(true, garagev1alpha1.GarageBucketSpec{GlobalAliases: []string{"new"}})
	bucket.Status = garagev1alpha1.GarageBucketStatus{BucketID: testBucketID}
	r, _ := newBucketReconciler(t, admin, bucket, cluster, secret)

	reconcileBucket(t, r)

	if len(admin.addedAlias) != 1 || admin.addedAlias[0] != "new" {
		t.Errorf("addedAlias = %v, want [new]", admin.addedAlias)
	}
	if len(admin.removedAlias) != 1 || admin.removedAlias[0] != "old" {
		t.Errorf("removedAlias = %v, want [old]", admin.removedAlias)
	}
	if admin.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0 (bucket already exists)", admin.createCalls)
	}
}

func TestBucketDeleteRetainDropsFinalizer(t *testing.T) {
	cluster, secret := readyCluster()
	bucket := bucketCR(true, garagev1alpha1.GarageBucketSpec{DeletionPolicy: garagev1alpha1.BucketDeletionRetain})
	bucket.Status = garagev1alpha1.GarageBucketStatus{BucketID: testBucketID}
	admin := newFakeBucketAdmin()
	admin.buckets[testBucketID] = &garageadmin.GetBucketInfoResponse{Id: testBucketID}
	r, c := newBucketReconciler(t, admin, bucket, cluster, secret)

	if err := c.Delete(context.Background(), bucket); err != nil {
		t.Fatalf("delete: %v", err)
	}
	reconcileBucket(t, r)

	if admin.deleteCalls != 0 {
		t.Errorf("deleteCalls = %d, want 0 under Retain", admin.deleteCalls)
	}
	var b garagev1alpha1.GarageBucket
	err := c.Get(context.Background(), types.NamespacedName{Name: testBucketName, Namespace: testBucketNS}, &b)
	if !apierrors.IsNotFound(err) {
		t.Errorf("bucket still present after finalizer drop: err=%v", err)
	}
}

func TestBucketDeletePolicyDeletesEmptyBucket(t *testing.T) {
	cluster, secret := readyCluster()
	bucket := bucketCR(true, garagev1alpha1.GarageBucketSpec{DeletionPolicy: garagev1alpha1.BucketDeletionDelete})
	bucket.Status = garagev1alpha1.GarageBucketStatus{BucketID: testBucketID}
	admin := newFakeBucketAdmin()
	admin.buckets[testBucketID] = &garageadmin.GetBucketInfoResponse{Id: testBucketID, Objects: 0}
	r, c := newBucketReconciler(t, admin, bucket, cluster, secret)

	if err := c.Delete(context.Background(), bucket); err != nil {
		t.Fatalf("delete: %v", err)
	}
	reconcileBucket(t, r)

	if admin.deleteCalls != 1 {
		t.Errorf("deleteCalls = %d, want 1 for an empty bucket under Delete", admin.deleteCalls)
	}
	var b garagev1alpha1.GarageBucket
	if err := c.Get(context.Background(), types.NamespacedName{Name: testBucketName, Namespace: testBucketNS}, &b); !apierrors.IsNotFound(err) {
		t.Errorf("bucket still present after deletion: err=%v", err)
	}
}

// readyGarageKey builds a GarageKey CR that has already published the given Garage key id, so
// the bucket controller can resolve a keyRef pointing at it.
func readyGarageKey(name, namespace, keyID string) *garagev1alpha1.GarageKey {
	return &garagev1alpha1.GarageKey{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec:       garagev1alpha1.GarageKeySpec{ClusterRef: clusterRef()},
		Status:     garagev1alpha1.GarageKeyStatus{KeyID: keyID},
	}
}

func TestBucketReconcileAppliesGrant(t *testing.T) {
	cluster, secret := readyCluster()
	key := readyGarageKey(testKeyName, testBucketNS, testGarageKeyID)
	bucket := bucketCR(true, garagev1alpha1.GarageBucketSpec{
		GlobalAliases: []string{testBucketName},
		Grants: []garagev1alpha1.BucketGrant{{
			KeyRef: garagev1alpha1.KeyReference{Name: testKeyName},
			Read:   true,
			Write:  true,
		}},
	})
	admin := newFakeBucketAdmin()
	r, c := newBucketReconciler(t, admin, bucket, key, cluster, secret)

	reconcileBucket(t, r)

	if len(admin.allowCalls) != 1 || admin.allowCalls[0] != testGarageKeyID {
		t.Fatalf("allowCalls = %v, want [%s]", admin.allowCalls, testGarageKeyID)
	}
	if cond := meta.FindStatusCondition(getBucket(t, c).Status.Conditions, conditionReady); cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("Ready condition = %+v, want True", cond)
	}
}

func TestBucketReconcilePendingWhenKeyNotReady(t *testing.T) {
	cluster, secret := readyCluster()
	// The key exists but has not published an id yet.
	key := readyGarageKey(testKeyName, testBucketNS, "")
	bucket := bucketCR(true, garagev1alpha1.GarageBucketSpec{
		GlobalAliases: []string{testBucketName},
		Grants:        []garagev1alpha1.BucketGrant{{KeyRef: garagev1alpha1.KeyReference{Name: testKeyName}, Read: true}},
	})
	admin := newFakeBucketAdmin()
	r, c := newBucketReconciler(t, admin, bucket, key, cluster, secret)

	res := reconcileBucket(t, r)
	if res.RequeueAfter == 0 {
		t.Error("expected a requeue while the referenced key is not Ready")
	}
	if len(admin.allowCalls) != 0 {
		t.Errorf("allowCalls = %v, want none while the key is pending", admin.allowCalls)
	}
	cond := meta.FindStatusCondition(getBucket(t, c).Status.Conditions, conditionReady)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "KeyNotReady" {
		t.Fatalf("Ready condition = %+v, want False/KeyNotReady", cond)
	}
}

func TestBucketReconcileAppliesLocalAlias(t *testing.T) {
	cluster, secret := readyCluster()
	key := readyGarageKey(testKeyName, testBucketNS, testGarageKeyID)
	bucket := bucketCR(true, garagev1alpha1.GarageBucketSpec{
		GlobalAliases: []string{testBucketName},
		LocalAliases:  []garagev1alpha1.LocalAlias{{KeyRef: garagev1alpha1.KeyReference{Name: testKeyName}, Alias: "pics"}},
	})
	admin := newFakeBucketAdmin()
	r, _ := newBucketReconciler(t, admin, bucket, key, cluster, secret)

	reconcileBucket(t, r)

	if len(admin.addedLocalAlias) != 1 || admin.addedLocalAlias[0] != "pics" {
		t.Fatalf("addedLocalAlias = %v, want [pics]", admin.addedLocalAlias)
	}
}

func TestBucketReconcileRevokesUnlistedGrant(t *testing.T) {
	cluster, secret := readyCluster()
	admin := newFakeBucketAdmin()
	// A bucket bound via status already carries a stale grant for a key no longer in the spec.
	admin.buckets[testBucketID] = &garageadmin.GetBucketInfoResponse{
		Id: testBucketID,
		Keys: []garageadmin.GetBucketInfoKey{{
			AccessKeyId: "GK-STALE",
			Permissions: garageadmin.ApiBucketKeyPerm{Read: ptr.To(true), Write: ptr.To(true)},
		}},
	}
	bucket := bucketCR(true, garagev1alpha1.GarageBucketSpec{})
	bucket.Status = garagev1alpha1.GarageBucketStatus{BucketID: testBucketID}
	r, _ := newBucketReconciler(t, admin, bucket, cluster, secret)

	reconcileBucket(t, r)

	if len(admin.denyCalls) != 1 || admin.denyCalls[0] != "GK-STALE" {
		t.Fatalf("denyCalls = %v, want [GK-STALE] (stale grant revoked)", admin.denyCalls)
	}
}

func TestBucketDeleteRefusesNonEmptyBucket(t *testing.T) {
	cluster, secret := readyCluster()
	bucket := bucketCR(true, garagev1alpha1.GarageBucketSpec{DeletionPolicy: garagev1alpha1.BucketDeletionDelete})
	bucket.Status = garagev1alpha1.GarageBucketStatus{BucketID: testBucketID}
	admin := newFakeBucketAdmin()
	admin.buckets[testBucketID] = &garageadmin.GetBucketInfoResponse{Id: testBucketID, Objects: 3}
	r, c := newBucketReconciler(t, admin, bucket, cluster, secret)

	if err := c.Delete(context.Background(), bucket); err != nil {
		t.Fatalf("delete: %v", err)
	}
	res := reconcileBucket(t, r)
	if res.RequeueAfter == 0 {
		t.Error("expected a requeue while deletion is blocked")
	}

	if admin.deleteCalls != 0 {
		t.Errorf("deleteCalls = %d, want 0 for a non-empty bucket", admin.deleteCalls)
	}
	got := getBucket(t, c)
	if !controllerutil.ContainsFinalizer(got, bucketFinalizer) {
		t.Error("finalizer was dropped despite a non-empty bucket")
	}
	cond := meta.FindStatusCondition(got.Status.Conditions, conditionReady)
	if cond == nil || cond.Reason != "DeletionBlocked" {
		t.Fatalf("Ready condition = %+v, want reason DeletionBlocked", cond)
	}

	// The block must also surface as an Event so it shows up in `kubectl describe` during a
	// hung delete, not just in the status condition.
	select {
	case ev := <-r.Recorder.(*record.FakeRecorder).Events:
		if !strings.Contains(ev, "DeletionBlocked") {
			t.Errorf("event = %q, want it to mention DeletionBlocked", ev)
		}
	default:
		t.Error("expected a DeletionBlocked event to be recorded")
	}
}
