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

package v1alpha1

import (
	"context"

	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	garagev1alpha1 "github.com/QuantumDancer/garage-operator/api/v1alpha1"
	"github.com/QuantumDancer/garage-operator/internal/refpolicy"
)

// log is for logging in this package.
var garagebucketlog = logf.Log.WithName("garagebucket-resource")

// SetupGarageBucketWebhookWithManager registers the webhook for GarageBucket in the manager.
func SetupGarageBucketWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &garagev1alpha1.GarageBucket{}).
		WithValidator(&GarageBucketCustomValidator{Reader: mgr.GetClient()}).
		Complete()
}

// failurePolicy=ignore: this webhook is advisory fast feedback, not the security boundary. The
// controller re-checks the same referencePolicy on every reconcile and refuses to act on a
// denied reference, so a webhook outage must not wedge GarageBucket applies.
// +kubebuilder:webhook:path=/validate-garage-rottler-io-v1alpha1-garagebucket,mutating=false,failurePolicy=ignore,sideEffects=None,groups=garage.rottler.io,resources=garagebuckets,verbs=create;update,versions=v1alpha1,name=vgaragebucket-v1alpha1.kb.io,admissionReviewVersions=v1

// GarageBucketCustomValidator validates that a GarageBucket may reference its target cluster,
// per that cluster's spec.referencePolicy.
type GarageBucketCustomValidator struct {
	// Reader loads the referenced GarageCluster and, when a namespaceSelector is in play, the
	// referencing Namespace's labels.
	Reader client.Reader
}

var _ admission.Validator[*garagev1alpha1.GarageBucket] = &GarageBucketCustomValidator{}

// ValidateCreate validates the clusterRef against the target cluster's referencePolicy.
func (v *GarageBucketCustomValidator) ValidateCreate(ctx context.Context, bucket *garagev1alpha1.GarageBucket) (admission.Warnings, error) {
	return v.validateReference(ctx, bucket)
}

// ValidateUpdate re-validates only when clusterRef changes — i.e. the reference was repointed at
// a different cluster. The namespace (which drives the decision) is immutable, so an update that
// leaves clusterRef untouched can never flip the verdict. Skipping those is essential: the
// operator removes its finalizer via an Update, so re-validating every update would let a
// later-tightened policy wedge a bucket in Terminating.
func (v *GarageBucketCustomValidator) ValidateUpdate(ctx context.Context, oldBucket, newBucket *garagev1alpha1.GarageBucket) (admission.Warnings, error) {
	if oldBucket.Spec.ClusterRef == newBucket.Spec.ClusterRef {
		return nil, nil
	}
	return v.validateReference(ctx, newBucket)
}

// ValidateDelete is a no-op: the reference policy gates active management, not teardown, so a
// later-tightened policy must never strand a bucket by blocking its deletion.
func (v *GarageBucketCustomValidator) ValidateDelete(_ context.Context, _ *garagev1alpha1.GarageBucket) (admission.Warnings, error) {
	return nil, nil
}

func (v *GarageBucketCustomValidator) validateReference(ctx context.Context, bucket *garagev1alpha1.GarageBucket) (admission.Warnings, error) {
	allowed, reason, err := refpolicy.Check(ctx, v.Reader, bucket.Spec.ClusterRef, bucket.Namespace)
	if err != nil {
		// Fail open: the controller backstop enforces the policy regardless, so a transient
		// read failure here must not reject an otherwise-valid apply.
		garagebucketlog.Error(err, "Could not evaluate referencePolicy; allowing", "name", bucket.Name)
		return nil, nil
	}
	if !allowed {
		return nil, field.Forbidden(field.NewPath("spec", "clusterRef"), reason)
	}
	return nil, nil
}
