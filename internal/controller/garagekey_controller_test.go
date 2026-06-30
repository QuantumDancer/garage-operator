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
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	garagev1alpha1 "github.com/QuantumDancer/garage-operator/api/v1alpha1"
	"github.com/QuantumDancer/garage-operator/internal/garageadmin"
)

// testImportSecretName is the Secret holding caller-supplied credentials, reused across the
// import-key fixtures in this file.
const testImportSecretName = "existing-creds"

// fakeKeyAdmin is an in-memory stand-in for the key slice of the Admin API. It records the calls
// the controller makes so tests can assert on convergence behaviour.
type fakeKeyAdmin struct {
	keys   map[string]*garageadmin.GetKeyInfoResponse // by accessKeyId
	nextID int

	createCalls int
	importCalls int
	updateCalls int
	deleteCalls int
}

func newFakeKeyAdmin() *fakeKeyAdmin {
	return &fakeKeyAdmin{keys: map[string]*garageadmin.GetKeyInfoResponse{}}
}

func (f *fakeKeyAdmin) NodeID(context.Context) (string, error) {
	return "fake-node", nil
}

func (f *fakeKeyAdmin) GetKeyByID(_ context.Context, id string) (*garageadmin.GetKeyInfoResponse, bool, error) {
	k, ok := f.keys[id]
	return k, ok, nil
}

func (f *fakeKeyAdmin) CreateKey(_ context.Context, body garageadmin.CreateKeyRequest) (*garageadmin.GetKeyInfoResponse, error) {
	f.createCalls++
	f.nextID++
	id := fmt.Sprintf("GK%d", f.nextID)
	name := ""
	if body.Name != nil {
		name = *body.Name
	}
	info := &garageadmin.GetKeyInfoResponse{AccessKeyId: id, Name: name}
	f.keys[id] = info
	// The returned copy carries the secret (only revealed at creation); the stored one does not.
	withSecret := *info
	withSecret.SecretAccessKey = ptr.To("secret-" + id)
	return &withSecret, nil
}

func (f *fakeKeyAdmin) ImportKey(_ context.Context, body garageadmin.ImportKeyRequest) (*garageadmin.GetKeyInfoResponse, error) {
	f.importCalls++
	info := &garageadmin.GetKeyInfoResponse{AccessKeyId: body.AccessKeyId}
	if body.Name != nil {
		info.Name = *body.Name
	}
	f.keys[body.AccessKeyId] = info
	return info, nil
}

func (f *fakeKeyAdmin) UpdateKey(_ context.Context, id string, body garageadmin.UpdateKeyRequestBody) (*garageadmin.GetKeyInfoResponse, error) {
	f.updateCalls++
	info := f.keys[id]
	if info == nil {
		info = &garageadmin.GetKeyInfoResponse{AccessKeyId: id}
		f.keys[id] = info
	}
	if body.Name != nil {
		info.Name = *body.Name
	}
	info.Expiration = body.Expiration
	return info, nil
}

func (f *fakeKeyAdmin) DeleteKey(_ context.Context, id string) error {
	f.deleteCalls++
	delete(f.keys, id)
	return nil
}

func newKeyReconciler(t *testing.T, admin keyAdmin, objs ...client.Object) (*GarageKeyReconciler, client.Client) {
	t.Helper()
	scheme := bucketTestScheme(t)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(&garagev1alpha1.GarageKey{}).
		Build()
	return &GarageKeyReconciler{
		Client:         c,
		Scheme:         scheme,
		NewAdminClient: func(string, string) (keyAdmin, error) { return admin, nil },
		Recorder:       record.NewFakeRecorder(100),
	}, c
}

func reconcileKey(t *testing.T, r *GarageKeyReconciler) ctrl.Result {
	t.Helper()
	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: testKeyName, Namespace: testKeyNS},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	return res
}

func getKey(t *testing.T, c client.Client) *garagev1alpha1.GarageKey {
	t.Helper()
	var k garagev1alpha1.GarageKey
	if err := c.Get(context.Background(), types.NamespacedName{Name: testKeyName, Namespace: testKeyNS}, &k); err != nil {
		t.Fatalf("get key: %v", err)
	}
	return &k
}

// keyCR builds a GarageKey fixture targeting the shared test cluster.
func keyCR(finalized bool, spec garagev1alpha1.GarageKeySpec) *garagev1alpha1.GarageKey {
	spec.ClusterRef = clusterRef()
	objMeta := metav1.ObjectMeta{Name: testKeyName, Namespace: testKeyNS}
	if finalized {
		objMeta.Finalizers = []string{keyFinalizer}
	}
	return &garagev1alpha1.GarageKey{ObjectMeta: objMeta, Spec: spec}
}

// getCredentialsSecret fetches the default output Secret (every fixture in this file uses
// testKeyName's default naming, so the name isn't worth parameterizing).
func getCredentialsSecret(t *testing.T, c client.Client) *corev1.Secret {
	t.Helper()
	name := testKeyName + "-credentials"
	var s corev1.Secret
	if err := c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: testKeyNS}, &s); err != nil {
		t.Fatalf("get secret %q: %v", name, err)
	}
	return &s
}

func TestKeyReconcileAddsFinalizer(t *testing.T) {
	admin := newFakeKeyAdmin()
	r, c := newKeyReconciler(t, admin, keyCR(false, garagev1alpha1.GarageKeySpec{}))

	reconcileKey(t, r)

	if !controllerutil.ContainsFinalizer(getKey(t, c), keyFinalizer) {
		t.Fatal("expected finalizer to be added on first reconcile")
	}
	if admin.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0 before finalizer is established", admin.createCalls)
	}
}

func TestKeyReconcilePendingWhenClusterNotReady(t *testing.T) {
	notReady := &garagev1alpha1.GarageCluster{
		ObjectMeta: metav1.ObjectMeta{Name: testClusterName, Namespace: testClusterNS},
		Spec:       garagev1alpha1.GarageClusterSpec{NodePools: []garagev1alpha1.NodePool{storagePool()}},
	}
	admin := newFakeKeyAdmin()
	r, c := newKeyReconciler(t, admin, keyCR(true, garagev1alpha1.GarageKeySpec{}), notReady)

	res := reconcileKey(t, r)
	if res.RequeueAfter == 0 {
		t.Error("expected a requeue while the cluster is not Ready")
	}
	cond := meta.FindStatusCondition(getKey(t, c).Status.Conditions, conditionReady)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != reasonClusterNotReady {
		t.Fatalf("Ready condition = %+v, want False/ClusterNotReady", cond)
	}
	if admin.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0 while pending", admin.createCalls)
	}
}

// TestKeyReconcileDeniedByReferencePolicy is the controller backstop for the cluster's
// referencePolicy: a key whose namespace the policy forbids must report ReferenceNotAllowed and
// never touch the Admin API, even though the cluster is Ready.
func TestKeyReconcileDeniedByReferencePolicy(t *testing.T) {
	cluster, secret := readyCluster()
	// testKeyNS ("media") is not in the allow-list and the cluster lives in "storage".
	cluster.Spec.ReferencePolicy = &garagev1alpha1.ReferencePolicy{AllowedNamespaces: []string{"other"}}
	admin := newFakeKeyAdmin()
	r, c := newKeyReconciler(t, admin, keyCR(true, garagev1alpha1.GarageKeySpec{}), cluster, secret)

	reconcileKey(t, r)

	cond := meta.FindStatusCondition(getKey(t, c).Status.Conditions, conditionReady)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != reasonReferenceNotAllowed {
		t.Fatalf("Ready condition = %+v, want False/ReferenceNotAllowed", cond)
	}
	if admin.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0 for a denied reference", admin.createCalls)
	}
}

func TestKeyReconcileCreatesKeyAndPublishesSecret(t *testing.T) {
	cluster, secret := readyCluster()
	admin := newFakeKeyAdmin()
	r, c := newKeyReconciler(t, admin, keyCR(true, garagev1alpha1.GarageKeySpec{}), cluster, secret)

	reconcileKey(t, r)

	if admin.createCalls != 1 {
		t.Fatalf("createCalls = %d, want 1", admin.createCalls)
	}
	got := getKey(t, c)
	if got.Status.KeyID == "" {
		t.Error("status.keyId not recorded after creation")
	}
	wantSecret := testKeyName + "-credentials"
	if got.Status.CredentialsSecret != wantSecret {
		t.Errorf("status.credentialsSecret = %q, want %q", got.Status.CredentialsSecret, wantSecret)
	}
	if cond := meta.FindStatusCondition(got.Status.Conditions, conditionReady); cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("Ready condition = %+v, want True", cond)
	}
	if cond := meta.FindStatusCondition(got.Status.Conditions, conditionCredentialsPublished); cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("CredentialsPublished condition = %+v, want True", cond)
	}

	out := getCredentialsSecret(t, c)
	if string(out.Data[credAccessKeyID]) != got.Status.KeyID {
		t.Errorf("secret accessKeyId = %q, want %q", out.Data[credAccessKeyID], got.Status.KeyID)
	}
	if len(out.Data[credSecretAccessKey]) == 0 {
		t.Error("secret access key not published")
	}
	if owner := metav1.GetControllerOf(out); owner == nil || owner.Kind != "GarageKey" {
		t.Errorf("credentials Secret owner = %+v, want controller GarageKey", owner)
	}
}

func TestKeyReconcileImportsExistingCredentials(t *testing.T) {
	cluster, secret := readyCluster()
	importSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: testImportSecretName, Namespace: testKeyNS},
		Data: map[string][]byte{
			credAccessKeyID:     []byte("GKIMPORTED"),
			credSecretAccessKey: []byte("imported-secret"),
		},
	}
	admin := newFakeKeyAdmin()
	key := keyCR(true, garagev1alpha1.GarageKeySpec{Import: &garagev1alpha1.KeyImport{SecretName: testImportSecretName}})
	r, c := newKeyReconciler(t, admin, key, cluster, secret, importSecret)

	reconcileKey(t, r)

	if admin.importCalls != 1 || admin.createCalls != 0 {
		t.Fatalf("import=%d create=%d, want import=1 create=0", admin.importCalls, admin.createCalls)
	}
	got := getKey(t, c)
	if got.Status.KeyID != "GKIMPORTED" {
		t.Errorf("status.keyId = %q, want GKIMPORTED", got.Status.KeyID)
	}
	out := getCredentialsSecret(t, c)
	if string(out.Data[credSecretAccessKey]) != "imported-secret" {
		t.Errorf("published secret = %q, want imported-secret", out.Data[credSecretAccessKey])
	}
}

func TestKeyReconcileAdoptsFromExistingSecret(t *testing.T) {
	// status.keyId was lost (e.g. a failed status write) but the credentials Secret survived and
	// the key still exists in Garage: the controller must adopt it, never mint a duplicate.
	cluster, secret := readyCluster()
	const adoptedID = "GKADOPTED"
	credSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: testKeyName + "-credentials", Namespace: testKeyNS},
		Data: map[string][]byte{
			credAccessKeyID:     []byte(adoptedID),
			credSecretAccessKey: []byte("kept-secret"),
		},
	}
	admin := newFakeKeyAdmin()
	admin.keys[adoptedID] = &garageadmin.GetKeyInfoResponse{AccessKeyId: adoptedID}
	r, c := newKeyReconciler(t, admin, keyCR(true, garagev1alpha1.GarageKeySpec{}), cluster, secret, credSecret)

	reconcileKey(t, r)

	if admin.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0 (key adopted from the surviving Secret)", admin.createCalls)
	}
	if got := getKey(t, c); got.Status.KeyID != adoptedID {
		t.Errorf("status.keyId = %q, want %q", got.Status.KeyID, adoptedID)
	}
}

// TestKeyEnsureFlipsConditionWhenCreatedSecretLost covers the fast path's Secret-loss repair for
// a created key (REVIEW.md #7): status.keyId already matches a live Garage key, but the output
// Secret has been deleted out-of-band. Since a created key's secret access key is unrecoverable
// (Garage reveals it only once), the controller must surface the broken state by flipping
// CredentialsPublished to False rather than keep reporting the stale True.
func TestKeyEnsureFlipsConditionWhenCreatedSecretLost(t *testing.T) {
	admin := newFakeKeyAdmin()
	admin.keys[testGarageKeyID] = &garageadmin.GetKeyInfoResponse{AccessKeyId: testGarageKeyID}
	key := keyCR(true, garagev1alpha1.GarageKeySpec{})
	r, _ := newKeyReconciler(t, admin, key)

	status := &garagev1alpha1.GarageKeyStatus{KeyID: testGarageKeyID}
	// Seed the stale True condition a prior, successful publish would have left behind.
	setKeyCondition(status, conditionCredentialsPublished, metav1.ConditionTrue, "CredentialsPublished", "stale")

	info, err := r.ensureKey(context.Background(), admin, key, status)
	if err != nil {
		t.Fatalf("ensureKey: %v", err)
	}
	if info.AccessKeyId != testGarageKeyID {
		t.Errorf("info.AccessKeyId = %q, want %q", info.AccessKeyId, testGarageKeyID)
	}

	cond := meta.FindStatusCondition(status.Conditions, conditionCredentialsPublished)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "CredentialsLost" {
		t.Fatalf("CredentialsPublished condition = %+v, want False/CredentialsLost", cond)
	}
}

// TestKeyEnsureRepublishesImportSecretWhenLost covers the fast path's Secret-loss repair for an
// import key (REVIEW.md #7): unlike a created key, an import key's secret access key is
// recoverable from spec.Import's Secret, so the controller must re-publish the output Secret
// rather than give up.
func TestKeyEnsureRepublishesImportSecretWhenLost(t *testing.T) {
	admin := newFakeKeyAdmin()
	admin.keys[testGarageKeyID] = &garageadmin.GetKeyInfoResponse{AccessKeyId: testGarageKeyID}
	importSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: testImportSecretName, Namespace: testKeyNS},
		Data: map[string][]byte{
			credAccessKeyID:     []byte(testGarageKeyID),
			credSecretAccessKey: []byte("imported-secret"),
		},
	}
	key := keyCR(true, garagev1alpha1.GarageKeySpec{Import: &garagev1alpha1.KeyImport{SecretName: testImportSecretName}})
	r, c := newKeyReconciler(t, admin, key, importSecret)

	status := &garagev1alpha1.GarageKeyStatus{KeyID: testGarageKeyID}

	info, err := r.ensureKey(context.Background(), admin, key, status)
	if err != nil {
		t.Fatalf("ensureKey: %v", err)
	}
	if info.AccessKeyId != testGarageKeyID {
		t.Errorf("info.AccessKeyId = %q, want %q", info.AccessKeyId, testGarageKeyID)
	}

	out := getCredentialsSecret(t, c)
	if string(out.Data[credAccessKeyID]) != testGarageKeyID {
		t.Errorf("secret accessKeyId = %q, want %q", out.Data[credAccessKeyID], testGarageKeyID)
	}
	if string(out.Data[credSecretAccessKey]) != "imported-secret" {
		t.Errorf("secret accessKey = %q, want imported-secret", out.Data[credSecretAccessKey])
	}
	cond := meta.FindStatusCondition(status.Conditions, conditionCredentialsPublished)
	if cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("CredentialsPublished condition = %+v, want True", cond)
	}
}

func TestKeyDeleteDeletesKey(t *testing.T) {
	cluster, secret := readyCluster()
	key := keyCR(true, garagev1alpha1.GarageKeySpec{DeletionPolicy: garagev1alpha1.KeyDeletionDelete})
	key.Status = garagev1alpha1.GarageKeyStatus{KeyID: testGarageKeyID}
	admin := newFakeKeyAdmin()
	admin.keys[testGarageKeyID] = &garageadmin.GetKeyInfoResponse{AccessKeyId: testGarageKeyID}
	r, c := newKeyReconciler(t, admin, key, cluster, secret)

	if err := c.Delete(context.Background(), key); err != nil {
		t.Fatalf("delete: %v", err)
	}
	reconcileKey(t, r)

	if admin.deleteCalls != 1 {
		t.Errorf("deleteCalls = %d, want 1 under Delete", admin.deleteCalls)
	}
	var k garagev1alpha1.GarageKey
	if err := c.Get(context.Background(), types.NamespacedName{Name: testKeyName, Namespace: testKeyNS}, &k); !apierrors.IsNotFound(err) {
		t.Errorf("key still present after deletion: err=%v", err)
	}
}

func TestKeyDeleteRetainKeepsKeyAndReleasesSecret(t *testing.T) {
	cluster, secret := readyCluster()
	key := keyCR(true, garagev1alpha1.GarageKeySpec{DeletionPolicy: garagev1alpha1.KeyDeletionRetain})
	key.Status = garagev1alpha1.GarageKeyStatus{KeyID: testGarageKeyID, CredentialsSecret: testKeyName + "-credentials"}
	admin := newFakeKeyAdmin()
	admin.keys[testGarageKeyID] = &garageadmin.GetKeyInfoResponse{AccessKeyId: testGarageKeyID}

	// The credentials Secret is owned by the key, as a live one would be.
	credSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: testKeyName + "-credentials", Namespace: testKeyNS},
		Data:       map[string][]byte{credAccessKeyID: []byte(testGarageKeyID), credSecretAccessKey: []byte("s")},
	}
	scheme := bucketTestScheme(t)
	if err := controllerutil.SetControllerReference(key, credSecret, scheme); err != nil {
		t.Fatalf("set owner ref: %v", err)
	}
	r, c := newKeyReconciler(t, admin, key, cluster, secret, credSecret)

	if err := c.Delete(context.Background(), key); err != nil {
		t.Fatalf("delete: %v", err)
	}
	reconcileKey(t, r)

	if admin.deleteCalls != 0 {
		t.Errorf("deleteCalls = %d, want 0 under Retain", admin.deleteCalls)
	}
	// The Secret must survive the CR (owner ref released) so retained credentials stay usable.
	out := getCredentialsSecret(t, c)
	if owner := metav1.GetControllerOf(out); owner != nil {
		t.Errorf("Secret still owned after Retain delete: %+v", owner)
	}
	var k garagev1alpha1.GarageKey
	if err := c.Get(context.Background(), types.NamespacedName{Name: testKeyName, Namespace: testKeyNS}, &k); !apierrors.IsNotFound(err) {
		t.Errorf("key CR still present after finalizer drop: err=%v", err)
	}
}
