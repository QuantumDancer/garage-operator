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
	"errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	garagev1alpha1 "github.com/QuantumDancer/garage-operator/api/v1alpha1"
	"github.com/QuantumDancer/garage-operator/internal/garageadmin"
)

// errKeyNotReady signals that a grant or local alias references a GarageKey that has not yet
// published a key id. It is a "not ready yet" outcome, not a failure: the bucket controller
// turns it into a KeyNotReady condition + requeue rather than an error backoff.
var errKeyNotReady = errors.New("referenced GarageKey is not Ready")

// localAliasKey identifies a per-key alias on a bucket: a (key, alias) pair.
type localAliasKey struct {
	accessKeyID string
	alias       string
}

// reconcileGrantsAndAliases converges the bucket's key-scoped settings: per-key permission
// grants and per-key local aliases. Both resolve a keyRef to a Garage access key id via the
// referenced GarageKey's status; an unresolved reference defers (errKeyNotReady) only after the
// resolvable entries have been applied, so a single not-ready key never blocks the rest.
func (r *GarageBucketReconciler) reconcileGrantsAndAliases(
	ctx context.Context,
	admin bucketAdmin,
	bucket *garagev1alpha1.GarageBucket,
	info *garageadmin.GetBucketInfoResponse,
) error {
	pending := false

	desiredGrants := map[string]garagev1alpha1.BucketGrant{}
	for i := range bucket.Spec.Grants {
		g := &bucket.Spec.Grants[i]
		keyID, ok, err := r.resolveKeyID(ctx, g.KeyRef, bucket.Namespace)
		if err != nil {
			return err
		}
		if !ok {
			pending = true
			continue
		}
		desiredGrants[keyID] = *g
	}
	if err := reconcileGrants(ctx, admin, info, desiredGrants); err != nil {
		return err
	}

	desiredAliases := map[localAliasKey]bool{}
	for i := range bucket.Spec.LocalAliases {
		la := &bucket.Spec.LocalAliases[i]
		keyID, ok, err := r.resolveKeyID(ctx, la.KeyRef, bucket.Namespace)
		if err != nil {
			return err
		}
		if !ok {
			pending = true
			continue
		}
		desiredAliases[localAliasKey{accessKeyID: keyID, alias: la.Alias}] = true
	}
	if err := reconcileLocalAliases(ctx, admin, info, desiredAliases); err != nil {
		return err
	}

	if pending {
		return errKeyNotReady
	}
	return nil
}

// reconcileGrants makes the bucket authoritative over key permissions: each desired key is
// granted exactly its read/write/owner set, and any key currently holding permissions but no
// longer listed is fully revoked.
func reconcileGrants(
	ctx context.Context,
	admin bucketAdmin,
	info *garageadmin.GetBucketInfoResponse,
	desired map[string]garagev1alpha1.BucketGrant,
) error {
	current := map[string]garageadmin.ApiBucketKeyPerm{}
	for i := range info.Keys {
		current[info.Keys[i].AccessKeyId] = info.Keys[i].Permissions
	}

	for keyID, grant := range desired {
		allow, deny := grantDiff(grant, current[keyID])
		if hasAnyPerm(allow) {
			if err := admin.AllowBucketKey(ctx, info.Id, keyID, allow); err != nil {
				return err
			}
		}
		if hasAnyPerm(deny) {
			if err := admin.DenyBucketKey(ctx, info.Id, keyID, deny); err != nil {
				return err
			}
		}
	}

	for keyID, perm := range current {
		if _, ok := desired[keyID]; ok {
			continue
		}
		deny := revokeAll(perm)
		if hasAnyPerm(deny) {
			if err := admin.DenyBucketKey(ctx, info.Id, keyID, deny); err != nil {
				return err
			}
		}
	}
	return nil
}

// grantDiff computes the permissions to allow (turn on) and deny (turn off) so the key ends up
// holding exactly the grant's read/write/owner set, given its current permissions.
func grantDiff(grant garagev1alpha1.BucketGrant, current garageadmin.ApiBucketKeyPerm) (allow, deny garageadmin.ApiBucketKeyPerm) {
	set := func(desired bool, cur *bool, allowField, denyField **bool) {
		has := cur != nil && *cur
		switch {
		case desired && !has:
			*allowField = ptr.To(true)
		case !desired && has:
			*denyField = ptr.To(true)
		}
	}
	set(grant.Read, current.Read, &allow.Read, &deny.Read)
	set(grant.Write, current.Write, &allow.Write, &deny.Write)
	set(grant.Owner, current.Owner, &allow.Owner, &deny.Owner)
	return allow, deny
}

// revokeAll returns a permission set denying every permission the key currently holds.
func revokeAll(current garageadmin.ApiBucketKeyPerm) garageadmin.ApiBucketKeyPerm {
	var deny garageadmin.ApiBucketKeyPerm
	if ptr.Deref(current.Read, false) {
		deny.Read = ptr.To(true)
	}
	if ptr.Deref(current.Write, false) {
		deny.Write = ptr.To(true)
	}
	if ptr.Deref(current.Owner, false) {
		deny.Owner = ptr.To(true)
	}
	return deny
}

func hasAnyPerm(p garageadmin.ApiBucketKeyPerm) bool {
	return p.Read != nil || p.Write != nil || p.Owner != nil
}

// reconcileLocalAliases makes the bucket authoritative over its per-key aliases: adds every
// desired (key, alias) pair not yet present and removes every present pair no longer desired.
func reconcileLocalAliases(
	ctx context.Context,
	admin bucketAdmin,
	info *garageadmin.GetBucketInfoResponse,
	desired map[localAliasKey]bool,
) error {
	current := map[localAliasKey]bool{}
	for i := range info.Keys {
		k := &info.Keys[i]
		for _, alias := range k.BucketLocalAliases {
			current[localAliasKey{accessKeyID: k.AccessKeyId, alias: alias}] = true
		}
	}

	for ak := range desired {
		if !current[ak] {
			if err := admin.AddBucketLocalAlias(ctx, info.Id, ak.accessKeyID, ak.alias); err != nil {
				return err
			}
		}
	}
	for ak := range current {
		if !desired[ak] {
			if err := admin.RemoveBucketLocalAlias(ctx, info.Id, ak.accessKeyID, ak.alias); err != nil {
				return err
			}
		}
	}
	return nil
}

// resolveKeyID resolves a KeyReference to a Garage access key id via the referenced GarageKey's
// status. ok is false (nil error) when the key does not exist yet or has not published an id, so
// the caller defers rather than failing.
func (r *GarageBucketReconciler) resolveKeyID(ctx context.Context, ref garagev1alpha1.KeyReference, defaultNamespace string) (string, bool, error) {
	namespace := ref.Namespace
	if namespace == "" {
		namespace = defaultNamespace
	}
	var key garagev1alpha1.GarageKey
	if err := r.Get(ctx, client.ObjectKey{Name: ref.Name, Namespace: namespace}, &key); err != nil {
		if apierrors.IsNotFound(err) {
			return "", false, nil
		}
		return "", false, err
	}
	if key.Status.KeyID == "" {
		return "", false, nil
	}
	return key.Status.KeyID, true, nil
}

// bucketsForKey maps a GarageKey event to reconcile requests for every GarageBucket that
// references it (in a grant or a local alias), so those buckets reconcile when the key becomes
// Ready.
func (r *GarageBucketReconciler) bucketsForKey(ctx context.Context, obj client.Object) []reconcile.Request {
	var buckets garagev1alpha1.GarageBucketList
	if err := r.List(ctx, &buckets); err != nil {
		return nil
	}
	var requests []reconcile.Request
	for i := range buckets.Items {
		b := &buckets.Items[i]
		if bucketReferencesKey(b, obj.GetName(), obj.GetNamespace()) {
			requests = append(requests, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: b.Name, Namespace: b.Namespace},
			})
		}
	}
	return requests
}

func bucketReferencesKey(bucket *garagev1alpha1.GarageBucket, keyName, keyNamespace string) bool {
	matches := func(ref garagev1alpha1.KeyReference) bool {
		ns := ref.Namespace
		if ns == "" {
			ns = bucket.Namespace
		}
		return ref.Name == keyName && ns == keyNamespace
	}
	for i := range bucket.Spec.Grants {
		if matches(bucket.Spec.Grants[i].KeyRef) {
			return true
		}
	}
	for i := range bucket.Spec.LocalAliases {
		if matches(bucket.Spec.LocalAliases[i].KeyRef) {
			return true
		}
	}
	return false
}
