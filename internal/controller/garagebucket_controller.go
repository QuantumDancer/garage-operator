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
	"slices"
	"time"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	garagev1alpha1 "github.com/QuantumDancer/garage-operator/api/v1alpha1"
	"github.com/QuantumDancer/garage-operator/internal/garageadmin"
)

// bucketFinalizer guards the Garage bucket against the CR vanishing before the operator has
// had a chance to honour deletionPolicy.
const bucketFinalizer = "garage.rottler.io/bucket-protection"

// clusterPendingRequeue is how long to wait before re-checking a referenced cluster that is
// not Ready yet. Buckets also watch their cluster, so this is a backstop, not the primary
// wake-up.
const clusterPendingRequeue = 15 * time.Second

// bucketAdmin is the slice of the Garage Admin API the bucket controller needs. It is an
// interface so reconcile logic can be exercised against a fake in tests.
type bucketAdmin interface {
	GetBucketByID(ctx context.Context, id string) (*garageadmin.GetBucketInfoResponse, bool, error)
	GetBucketByGlobalAlias(ctx context.Context, alias string) (*garageadmin.GetBucketInfoResponse, bool, error)
	CreateBucket(ctx context.Context, globalAlias string) (string, error)
	UpdateBucket(ctx context.Context, id string, body garageadmin.UpdateBucketRequestBody) error
	AddBucketGlobalAlias(ctx context.Context, bucketID, alias string) error
	RemoveBucketGlobalAlias(ctx context.Context, bucketID, alias string) error
	DeleteBucket(ctx context.Context, id string) error
}

// GarageBucketReconciler reconciles a GarageBucket object
type GarageBucketReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// NewAdminClient builds an admin client for a cluster's endpoint. Defaulted to the real
	// Garage Admin API client; overridden in tests.
	NewAdminClient func(baseURL, token string) (bucketAdmin, error)
}

func defaultBucketAdminFactory(baseURL, token string) (bucketAdmin, error) {
	return garageadmin.NewAdminClient(baseURL, token)
}

// +kubebuilder:rbac:groups=garage.rottler.io,resources=garagebuckets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=garage.rottler.io,resources=garagebuckets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=garage.rottler.io,resources=garagebuckets/finalizers,verbs=update
// +kubebuilder:rbac:groups=garage.rottler.io,resources=garageclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch

// Reconcile drives a Garage bucket toward the GarageBucket spec. It resolves the referenced
// cluster's Admin API, creates the bucket if absent, and converges global aliases, website,
// quotas, CORS and lifecycle. Local aliases and grants are reconciled in Phase 3.
func (r *GarageBucketReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if r.NewAdminClient == nil {
		r.NewAdminClient = defaultBucketAdminFactory
	}

	var bucket garagev1alpha1.GarageBucket
	if err := r.Get(ctx, req.NamespacedName, &bucket); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !bucket.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &bucket)
	}

	if !controllerutil.ContainsFinalizer(&bucket, bucketFinalizer) {
		controllerutil.AddFinalizer(&bucket, bucketFinalizer)
		if err := r.Update(ctx, &bucket); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	status := bucket.Status.DeepCopy()
	status.ObservedGeneration = bucket.Generation

	conn, state, err := resolveClusterConnection(ctx, r.Client, bucket.Spec.ClusterRef, bucket.Namespace)
	if err != nil {
		return ctrl.Result{}, err
	}
	if state != resolveReady {
		reason, msg := pendingReason(state, bucket.Spec.ClusterRef)
		log.Info("Waiting for referenced cluster", "reason", reason)
		setBucketCondition(status, metav1.ConditionFalse, reason, msg)
		return r.finish(ctx, &bucket, status, ctrl.Result{RequeueAfter: clusterPendingRequeue})
	}

	admin, err := r.NewAdminClient(conn.baseURL, conn.token)
	if err != nil {
		return ctrl.Result{}, err
	}

	if err := r.converge(ctx, admin, &bucket, status); err != nil {
		setBucketCondition(status, metav1.ConditionFalse, "ReconcileError", err.Error())
		_, _ = r.finish(ctx, &bucket, status, ctrl.Result{})
		return ctrl.Result{}, err
	}

	setBucketCondition(status, metav1.ConditionTrue, "BucketReady", "Bucket is reconciled")
	return r.finish(ctx, &bucket, status, ctrl.Result{})
}

// converge ensures the bucket exists and its settings match the spec. It records the bound
// bucket id into status as soon as the bucket is known, so a later partial failure never
// loses track of which bucket the CR owns.
func (r *GarageBucketReconciler) converge(
	ctx context.Context,
	admin bucketAdmin,
	bucket *garagev1alpha1.GarageBucket,
	status *garagev1alpha1.GarageBucketStatus,
) error {
	info, err := r.ensureBucket(ctx, admin, bucket, status)
	if err != nil {
		return err
	}

	if err := reconcileGlobalAliases(ctx, admin, info.Id, info.GlobalAliases, bucket.Spec.GlobalAliases); err != nil {
		return err
	}

	return admin.UpdateBucket(ctx, info.Id, buildUpdateBody(&bucket.Spec))
}

// ensureBucket resolves the CR to a live Garage bucket, creating it if necessary, and returns
// its authoritative info. Lookup order: the id already recorded in status, then a global alias
// (adopting a pre-existing bucket), then create.
func (r *GarageBucketReconciler) ensureBucket(
	ctx context.Context,
	admin bucketAdmin,
	bucket *garagev1alpha1.GarageBucket,
	status *garagev1alpha1.GarageBucketStatus,
) (*garageadmin.GetBucketInfoResponse, error) {
	if status.BucketID != "" {
		if info, found, err := admin.GetBucketByID(ctx, status.BucketID); err != nil {
			return nil, err
		} else if found {
			return info, nil
		}
		// The recorded bucket is gone (deleted out-of-band); fall through and recreate.
	}

	for _, alias := range bucket.Spec.GlobalAliases {
		info, found, err := admin.GetBucketByGlobalAlias(ctx, alias)
		if err != nil {
			return nil, err
		}
		if found {
			status.BucketID = info.Id
			return info, nil
		}
	}

	var initialAlias string
	if len(bucket.Spec.GlobalAliases) > 0 {
		initialAlias = bucket.Spec.GlobalAliases[0]
	}
	id, err := admin.CreateBucket(ctx, initialAlias)
	if err != nil {
		return nil, err
	}
	status.BucketID = id

	info, found, err := admin.GetBucketByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("bucket %q not found immediately after creation", id)
	}
	return info, nil
}

// reconcileDelete honours deletionPolicy when the CR is being deleted. A Retain policy (or a
// bucket that was never created) just drops the finalizer. A Delete policy removes the Garage
// bucket first, refusing while it is non-empty so data is never silently discarded.
func (r *GarageBucketReconciler) reconcileDelete(ctx context.Context, bucket *garagev1alpha1.GarageBucket) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if !controllerutil.ContainsFinalizer(bucket, bucketFinalizer) {
		return ctrl.Result{}, nil
	}

	if bucket.Spec.DeletionPolicy != garagev1alpha1.BucketDeletionDelete || bucket.Status.BucketID == "" {
		return r.removeFinalizer(ctx, bucket)
	}

	conn, state, err := resolveClusterConnection(ctx, r.Client, bucket.Spec.ClusterRef, bucket.Namespace)
	if err != nil {
		return ctrl.Result{}, err
	}
	switch state {
	case resolveClusterMissing:
		// The cluster is gone, so the bucket went with it; nothing left to delete.
		log.Info("Referenced cluster is gone; releasing bucket finalizer")
		return r.removeFinalizer(ctx, bucket)
	case resolveClusterNotReady:
		log.Info("Waiting for referenced cluster to become Ready before deleting bucket")
		return ctrl.Result{RequeueAfter: clusterPendingRequeue}, nil
	}

	admin, err := r.NewAdminClient(conn.baseURL, conn.token)
	if err != nil {
		return ctrl.Result{}, err
	}

	info, found, err := admin.GetBucketByID(ctx, bucket.Status.BucketID)
	if err != nil {
		return ctrl.Result{}, err
	}
	if found {
		if info.Objects > 0 {
			return r.blockDeletion(ctx, bucket, info.Objects)
		}
		if err := admin.DeleteBucket(ctx, bucket.Status.BucketID); err != nil {
			return ctrl.Result{}, err
		}
	}
	return r.removeFinalizer(ctx, bucket)
}

// blockDeletion refuses to delete a non-empty bucket, surfacing why on the CR and requeueing
// so deletion proceeds automatically once the bucket is emptied.
func (r *GarageBucketReconciler) blockDeletion(ctx context.Context, bucket *garagev1alpha1.GarageBucket, objects int64) (ctrl.Result, error) {
	status := bucket.Status.DeepCopy()
	msg := fmt.Sprintf("Refusing to delete non-empty bucket (%d objects); empty it or set deletionPolicy: Retain", objects)
	setBucketCondition(status, metav1.ConditionFalse, "DeletionBlocked", msg)
	if _, err := r.finish(ctx, bucket, status, ctrl.Result{}); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: clusterPendingRequeue}, nil
}

func (r *GarageBucketReconciler) removeFinalizer(ctx context.Context, bucket *garagev1alpha1.GarageBucket) (ctrl.Result, error) {
	controllerutil.RemoveFinalizer(bucket, bucketFinalizer)
	return ctrl.Result{}, r.Update(ctx, bucket)
}

// finish writes status only when it changed, avoiding a status hot-loop, then returns result.
func (r *GarageBucketReconciler) finish(
	ctx context.Context,
	bucket *garagev1alpha1.GarageBucket,
	status *garagev1alpha1.GarageBucketStatus,
	result ctrl.Result,
) (ctrl.Result, error) {
	if apiequality.Semantic.DeepEqual(&bucket.Status, status) {
		return result, nil
	}
	bucket.Status = *status
	if err := r.Status().Update(ctx, bucket); err != nil {
		return ctrl.Result{}, err
	}
	return result, nil
}

// reconcileGlobalAliases converges the bucket's cluster-wide aliases: adds every desired alias
// not yet present and removes every present alias no longer desired.
func reconcileGlobalAliases(ctx context.Context, admin bucketAdmin, bucketID string, current, desired []string) error {
	for _, alias := range desired {
		if !slices.Contains(current, alias) {
			if err := admin.AddBucketGlobalAlias(ctx, bucketID, alias); err != nil {
				return err
			}
		}
	}
	for _, alias := range current {
		if !slices.Contains(desired, alias) {
			if err := admin.RemoveBucketGlobalAlias(ctx, bucketID, alias); err != nil {
				return err
			}
		}
	}
	return nil
}

func pendingReason(state resolveState, ref garagev1alpha1.ClusterReference) (reason, message string) {
	if state == resolveClusterMissing {
		return "ClusterNotFound", fmt.Sprintf("Referenced GarageCluster %q not found", ref.Name)
	}
	return "ClusterNotReady", fmt.Sprintf("Referenced GarageCluster %q is not Ready", ref.Name)
}

func setBucketCondition(status *garagev1alpha1.GarageBucketStatus, s metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&status.Conditions, metav1.Condition{
		Type:               conditionReady,
		Status:             s,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: status.ObservedGeneration,
	})
}

// bucketsForCluster maps a GarageCluster event to reconcile requests for every GarageBucket
// that targets it, so buckets re-reconcile when their cluster becomes Ready.
func (r *GarageBucketReconciler) bucketsForCluster(ctx context.Context, obj client.Object) []reconcile.Request {
	var buckets garagev1alpha1.GarageBucketList
	if err := r.List(ctx, &buckets); err != nil {
		return nil
	}
	var requests []reconcile.Request
	for i := range buckets.Items {
		b := &buckets.Items[i]
		namespace := b.Spec.ClusterRef.Namespace
		if namespace == "" {
			namespace = b.Namespace
		}
		if b.Spec.ClusterRef.Name == obj.GetName() && namespace == obj.GetNamespace() {
			requests = append(requests, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: b.Name, Namespace: b.Namespace},
			})
		}
	}
	return requests
}

// SetupWithManager sets up the controller with the Manager.
func (r *GarageBucketReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.NewAdminClient == nil {
		r.NewAdminClient = defaultBucketAdminFactory
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&garagev1alpha1.GarageBucket{}).
		Watches(&garagev1alpha1.GarageCluster{}, handler.EnqueueRequestsFromMapFunc(r.bucketsForCluster)).
		Named("garagebucket").
		Complete(r)
}
