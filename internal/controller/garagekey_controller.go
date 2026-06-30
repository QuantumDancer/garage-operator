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
	"time"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	garagev1alpha1 "github.com/QuantumDancer/garage-operator/api/v1alpha1"
	"github.com/QuantumDancer/garage-operator/internal/garageadmin"
	"github.com/QuantumDancer/garage-operator/internal/refpolicy"
)

// keyFinalizer guards the Garage key against the CR vanishing before the operator has honoured
// deletionPolicy.
const keyFinalizer = "garage.rottler.io/key-protection"

// Condition reported in addition to Ready on a GarageKey.
const conditionCredentialsPublished = "CredentialsPublished"

// Data keys written into the output credentials Secret. Workloads remap these to AWS-style env
// vars at consumption time.
const (
	credAccessKeyID     = "accessKeyId"
	credSecretAccessKey = "secretAccessKey"
)

// keyAdmin is the slice of the Garage Admin API the key controller needs, behind an interface
// so reconcile logic can be exercised against a fake in tests.
type keyAdmin interface {
	NodeID(ctx context.Context) (string, error)

	GetKeyByID(ctx context.Context, id string) (*garageadmin.GetKeyInfoResponse, bool, error)
	CreateKey(ctx context.Context, body garageadmin.CreateKeyRequest) (*garageadmin.GetKeyInfoResponse, error)
	ImportKey(ctx context.Context, body garageadmin.ImportKeyRequest) (*garageadmin.GetKeyInfoResponse, error)
	UpdateKey(ctx context.Context, id string, body garageadmin.UpdateKeyRequestBody) (*garageadmin.GetKeyInfoResponse, error)
	DeleteKey(ctx context.Context, id string) error
}

// GarageKeyReconciler reconciles a GarageKey object
type GarageKeyReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// NewAdminClient builds an admin client for a cluster's endpoint. Defaulted to the real
	// Garage Admin API client; overridden in tests.
	NewAdminClient func(baseURL, token string) (keyAdmin, error)

	// Recorder emits Events onto the CR (e.g. why a reference was denied) so the reason is
	// visible in `kubectl describe`, not just in a status condition.
	Recorder record.EventRecorder
}

func defaultKeyAdminFactory(baseURL, token string) (keyAdmin, error) {
	return garageadmin.NewAdminClient(baseURL, token)
}

// +kubebuilder:rbac:groups=garage.rottler.io,resources=garagekeys,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=garage.rottler.io,resources=garagekeys/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=garage.rottler.io,resources=garagekeys/finalizers,verbs=update
// +kubebuilder:rbac:groups=garage.rottler.io,resources=garageclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=namespaces,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch

// Reconcile drives a Garage access key toward the GarageKey spec: it resolves the referenced
// cluster's Admin API, creates or imports the key, publishes its credentials into a Secret, and
// converges the key's name, permissions and expiration.
func (r *GarageKeyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if r.NewAdminClient == nil {
		r.NewAdminClient = defaultKeyAdminFactory
	}

	var key garagev1alpha1.GarageKey
	if err := r.Get(ctx, req.NamespacedName, &key); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !key.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &key)
	}

	if !controllerutil.ContainsFinalizer(&key, keyFinalizer) {
		controllerutil.AddFinalizer(&key, keyFinalizer)
		if err := r.Update(ctx, &key); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	status := key.Status.DeepCopy()
	status.ObservedGeneration = key.Generation

	// Hard backstop for the cluster's referencePolicy (see the bucket controller): a denied
	// reference never reaches the Admin API. The key re-reconciles on cluster changes via the
	// GarageCluster watch, so no requeue is needed.
	allowed, reason, err := refpolicy.Check(ctx, r.Client, key.Spec.ClusterRef, key.Namespace)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !allowed {
		log.Info("Reference not permitted by cluster referencePolicy", "reason", reason)
		setKeyCondition(status, conditionReady, metav1.ConditionFalse, reasonReferenceNotAllowed, reason)
		r.Recorder.Event(&key, corev1.EventTypeWarning, reasonReferenceNotAllowed, reason)
		return r.finish(ctx, &key, status, ctrl.Result{})
	}

	conn, state, err := resolveClusterConnection(ctx, r.Client, key.Spec.ClusterRef, key.Namespace)
	if err != nil {
		return ctrl.Result{}, err
	}
	if state != resolveReady {
		reason, msg := pendingReason(state, key.Spec.ClusterRef)
		log.Info("Waiting for referenced cluster", "reason", reason)
		setKeyCondition(status, conditionReady, metav1.ConditionFalse, reason, msg)
		return r.finish(ctx, &key, status, ctrl.Result{RequeueAfter: clusterPendingRequeue})
	}

	admin, err := firstReachableAdmin(ctx, r.NewAdminClient, conn.baseURLs, conn.token)
	if err != nil {
		log.Info("No reachable Garage admin endpoint; requeuing", "error", err.Error())
		setKeyCondition(status, conditionReady, metav1.ConditionFalse, reasonClusterUnreachable, err.Error())
		return r.finish(ctx, &key, status, ctrl.Result{RequeueAfter: clusterPendingRequeue})
	}

	if err := r.converge(ctx, admin, &key, status); err != nil {
		setKeyCondition(status, conditionReady, metav1.ConditionFalse, "ReconcileError", err.Error())
		_, _ = r.finish(ctx, &key, status, ctrl.Result{})
		return ctrl.Result{}, err
	}

	setKeyCondition(status, conditionReady, metav1.ConditionTrue, "KeyReady", "Key is reconciled")
	return r.finish(ctx, &key, status, ctrl.Result{})
}

// converge ensures the key exists, its credentials are published, and its name/permissions/
// expiration match the spec. The key id is recorded into status as soon as the key is known so a
// later partial failure never loses track of which Garage key the CR owns.
func (r *GarageKeyReconciler) converge(
	ctx context.Context,
	admin keyAdmin,
	key *garagev1alpha1.GarageKey,
	status *garagev1alpha1.GarageKeyStatus,
) error {
	if _, err := r.ensureKey(ctx, admin, key, status); err != nil {
		return err
	}

	body, err := garageadmin.NewKeyUpdateBody(desiredKeyName(key), key.Spec.Permissions.CreateBucket, expirationTime(key.Spec.Expiration))
	if err != nil {
		return err
	}
	updated, err := admin.UpdateKey(ctx, status.KeyID, body)
	if err != nil {
		return err
	}

	status.Expiration = wrapTime(updated.Expiration)
	status.Expired = updated.Expired
	return nil
}

// ensureKey resolves the CR to a live Garage key, creating or importing it if necessary, and
// returns its info. Lookup order: the id already recorded in status, then an already-published
// output Secret (status lost but credentials persisted — adopt by the unique access key id),
// then import or create. On create/import the freshly returned secret key is published to the
// output Secret immediately, because Garage only reveals it once.
func (r *GarageKeyReconciler) ensureKey(
	ctx context.Context,
	admin keyAdmin,
	key *garagev1alpha1.GarageKey,
	status *garagev1alpha1.GarageKeyStatus,
) (*garageadmin.GetKeyInfoResponse, error) {
	if status.KeyID != "" {
		if info, found, err := admin.GetKeyByID(ctx, status.KeyID); err != nil {
			return nil, err
		} else if found {
			return info, nil
		}
		// The recorded key is gone (deleted out-of-band); fall through and recreate.
	}

	secretName := outputSecretName(key)
	if creds, ok, err := r.readSecret(ctx, key.Namespace, secretName, credAccessKeyID, credSecretAccessKey); err != nil {
		return nil, err
	} else if ok {
		if info, found, err := admin.GetKeyByID(ctx, creds[credAccessKeyID]); err != nil {
			return nil, err
		} else if found {
			r.markPublished(status, secretName, info.AccessKeyId)
			return info, nil
		}
	}

	info, secretAccessKey, err := r.createOrImportKey(ctx, admin, key)
	if err != nil {
		return nil, err
	}
	if err := r.publishCredentials(ctx, key, info.AccessKeyId, secretAccessKey); err != nil {
		return nil, err
	}
	r.markPublished(status, secretName, info.AccessKeyId)
	return info, nil
}

// createOrImportKey creates a fresh key or imports caller-supplied credentials, returning the
// key info together with the secret access key (which Garage only returns at creation/import).
func (r *GarageKeyReconciler) createOrImportKey(
	ctx context.Context,
	admin keyAdmin,
	key *garagev1alpha1.GarageKey,
) (*garageadmin.GetKeyInfoResponse, string, error) {
	if key.Spec.Import != nil {
		creds, ok, err := r.readSecret(ctx, key.Namespace, key.Spec.Import.SecretName,
			importAccessKeyIDKey(key.Spec.Import), importSecretAccessKeyKey(key.Spec.Import))
		if err != nil {
			return nil, "", err
		}
		if !ok {
			return nil, "", fmt.Errorf("import Secret %q is missing or incomplete", key.Spec.Import.SecretName)
		}
		name := desiredKeyName(key)
		info, err := admin.ImportKey(ctx, garageadmin.ImportKeyRequest{
			AccessKeyId:     creds[importAccessKeyIDKey(key.Spec.Import)],
			SecretAccessKey: creds[importSecretAccessKeyKey(key.Spec.Import)],
			Name:            &name,
		})
		if err != nil {
			return nil, "", err
		}
		return info, creds[importSecretAccessKeyKey(key.Spec.Import)], nil
	}

	body, err := garageadmin.NewKeyUpdateBody(desiredKeyName(key), key.Spec.Permissions.CreateBucket, expirationTime(key.Spec.Expiration))
	if err != nil {
		return nil, "", err
	}
	info, err := admin.CreateKey(ctx, body)
	if err != nil {
		return nil, "", err
	}
	if info.SecretAccessKey == nil {
		return nil, "", fmt.Errorf("CreateKey did not return a secret access key for %q", info.AccessKeyId)
	}
	return info, *info.SecretAccessKey, nil
}

// publishCredentials writes the access credentials into the output Secret, owned by the CR so
// it is garbage-collected with a Delete-policy key and watched via Owns().
func (r *GarageKeyReconciler) publishCredentials(ctx context.Context, key *garagev1alpha1.GarageKey, accessKeyID, secretAccessKey string) error {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: outputSecretName(key), Namespace: key.Namespace},
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, secret, func() error {
		if err := ctrl.SetControllerReference(key, secret, r.Scheme); err != nil {
			return err
		}
		if secret.Data == nil {
			secret.Data = map[string][]byte{}
		}
		secret.Data[credAccessKeyID] = []byte(accessKeyID)
		secret.Data[credSecretAccessKey] = []byte(secretAccessKey)
		return nil
	})
	return err
}

// markPublished records the bound key id and Secret in status and flips the
// CredentialsPublished condition true.
func (r *GarageKeyReconciler) markPublished(status *garagev1alpha1.GarageKeyStatus, secretName, keyID string) {
	status.KeyID = keyID
	status.CredentialsSecret = secretName
	setKeyCondition(status, conditionCredentialsPublished, metav1.ConditionTrue, "CredentialsPublished",
		fmt.Sprintf("Credentials written to Secret %q", secretName))
}

// reconcileDelete honours deletionPolicy when the CR is being deleted. Retain (or a key that
// was never created) drops the finalizer, first releasing the operator's ownership of the
// credentials Secret so it survives. Delete removes the Garage key and lets owner-ref GC reclaim
// the Secret. Unlike a bucket, a key has no emptiness gate, so deletion is unconditional.
func (r *GarageKeyReconciler) reconcileDelete(ctx context.Context, key *garagev1alpha1.GarageKey) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if !controllerutil.ContainsFinalizer(key, keyFinalizer) {
		return ctrl.Result{}, nil
	}

	if key.Spec.DeletionPolicy == garagev1alpha1.KeyDeletionRetain {
		if err := r.releaseSecretOwnership(ctx, key); err != nil {
			return ctrl.Result{}, err
		}
		return r.removeFinalizer(ctx, key)
	}

	if key.Status.KeyID != "" {
		conn, state, err := resolveClusterConnection(ctx, r.Client, key.Spec.ClusterRef, key.Namespace)
		if err != nil {
			return ctrl.Result{}, err
		}
		switch state {
		case resolveClusterMissing:
			// The cluster is gone, so the key went with it; nothing left to delete.
			log.Info("Referenced cluster is gone; releasing key finalizer")
		case resolveClusterNotReady:
			log.Info("Waiting for referenced cluster to become Ready before deleting key")
			return ctrl.Result{RequeueAfter: clusterPendingRequeue}, nil
		default:
			admin, err := firstReachableAdmin(ctx, r.NewAdminClient, conn.baseURLs, conn.token)
			if err != nil {
				log.Info("No reachable Garage admin endpoint; requeuing key deletion", "error", err.Error())
				return ctrl.Result{RequeueAfter: clusterPendingRequeue}, nil
			}
			if err := admin.DeleteKey(ctx, key.Status.KeyID); err != nil {
				return ctrl.Result{}, err
			}
		}
	}
	return r.removeFinalizer(ctx, key)
}

// releaseSecretOwnership strips the controller owner reference from the credentials Secret so a
// Retain delete leaves it (and the still-valid credentials) behind.
func (r *GarageKeyReconciler) releaseSecretOwnership(ctx context.Context, key *garagev1alpha1.GarageKey) error {
	if key.Status.CredentialsSecret == "" {
		return nil
	}
	var secret corev1.Secret
	err := r.Get(ctx, client.ObjectKey{Name: key.Status.CredentialsSecret, Namespace: key.Namespace}, &secret)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	owned, err := controllerutil.HasOwnerReference(secret.GetOwnerReferences(), key, r.Scheme)
	if err != nil {
		return err
	}
	if !owned {
		return nil
	}
	if err := controllerutil.RemoveOwnerReference(key, &secret, r.Scheme); err != nil {
		return err
	}
	return r.Update(ctx, &secret)
}

func (r *GarageKeyReconciler) removeFinalizer(ctx context.Context, key *garagev1alpha1.GarageKey) (ctrl.Result, error) {
	controllerutil.RemoveFinalizer(key, keyFinalizer)
	return ctrl.Result{}, r.Update(ctx, key)
}

// finish writes status only when it changed, avoiding a status hot-loop, then returns result.
func (r *GarageKeyReconciler) finish(
	ctx context.Context,
	key *garagev1alpha1.GarageKey,
	status *garagev1alpha1.GarageKeyStatus,
	result ctrl.Result,
) (ctrl.Result, error) {
	if apiequality.Semantic.DeepEqual(&key.Status, status) {
		return result, nil
	}
	key.Status = *status
	if err := r.Status().Update(ctx, key); err != nil {
		return ctrl.Result{}, err
	}
	return result, nil
}

// readSecret loads the named Secret and returns the requested data keys. ok is false (nil error)
// when the Secret is absent or any requested key is missing/empty, so callers treat an
// incomplete Secret the same as an absent one.
func (r *GarageKeyReconciler) readSecret(ctx context.Context, namespace, name string, keys ...string) (map[string]string, bool, error) {
	var secret corev1.Secret
	if err := r.Get(ctx, client.ObjectKey{Name: name, Namespace: namespace}, &secret); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	out := make(map[string]string, len(keys))
	for _, k := range keys {
		v, ok := secret.Data[k]
		if !ok || len(v) == 0 {
			return nil, false, nil
		}
		out[k] = string(v)
	}
	return out, true, nil
}

func setKeyCondition(status *garagev1alpha1.GarageKeyStatus, condType string, s metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&status.Conditions, metav1.Condition{
		Type:               condType,
		Status:             s,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: status.ObservedGeneration,
	})
}

// desiredKeyName is the Garage display name: spec.name when set, otherwise the CR name.
func desiredKeyName(key *garagev1alpha1.GarageKey) string {
	if key.Spec.Name != "" {
		return key.Spec.Name
	}
	return key.Name
}

// outputSecretName is where credentials are published: spec.output.secretName when set,
// otherwise <cr-name>-credentials.
func outputSecretName(key *garagev1alpha1.GarageKey) string {
	if key.Spec.Output.SecretName != "" {
		return key.Spec.Output.SecretName
	}
	return key.Name + "-credentials"
}

func importAccessKeyIDKey(imp *garagev1alpha1.KeyImport) string {
	if imp.AccessKeyIDKey != "" {
		return imp.AccessKeyIDKey
	}
	return credAccessKeyID
}

func importSecretAccessKeyKey(imp *garagev1alpha1.KeyImport) string {
	if imp.SecretAccessKeyKey != "" {
		return imp.SecretAccessKeyKey
	}
	return credSecretAccessKey
}

func expirationTime(t *metav1.Time) *time.Time {
	if t == nil {
		return nil
	}
	return &t.Time
}

func wrapTime(t *time.Time) *metav1.Time {
	if t == nil {
		return nil
	}
	return &metav1.Time{Time: *t}
}

// keysForCluster maps a GarageCluster event to reconcile requests for every GarageKey that
// targets it, so keys re-reconcile when their cluster becomes Ready.
func (r *GarageKeyReconciler) keysForCluster(ctx context.Context, obj client.Object) []reconcile.Request {
	var keys garagev1alpha1.GarageKeyList
	if err := r.List(ctx, &keys); err != nil {
		return nil
	}
	var requests []reconcile.Request
	for i := range keys.Items {
		k := &keys.Items[i]
		namespace := k.Spec.ClusterRef.Namespace
		if namespace == "" {
			namespace = k.Namespace
		}
		if k.Spec.ClusterRef.Name == obj.GetName() && namespace == obj.GetNamespace() {
			requests = append(requests, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: k.Name, Namespace: k.Namespace},
			})
		}
	}
	return requests
}

// SetupWithManager sets up the controller with the Manager.
func (r *GarageKeyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.NewAdminClient == nil {
		r.NewAdminClient = defaultKeyAdminFactory
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&garagev1alpha1.GarageKey{}).
		Owns(&corev1.Secret{}).
		Watches(&garagev1alpha1.GarageCluster{}, handler.EnqueueRequestsFromMapFunc(r.keysForCluster)).
		Named("garagekey").
		Complete(r)
}
