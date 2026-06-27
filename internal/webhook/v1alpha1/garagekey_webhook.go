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
var garagekeylog = logf.Log.WithName("garagekey-resource")

// SetupGarageKeyWebhookWithManager registers the webhook for GarageKey in the manager.
func SetupGarageKeyWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &garagev1alpha1.GarageKey{}).
		WithValidator(&GarageKeyCustomValidator{Reader: mgr.GetClient()}).
		Complete()
}

// failurePolicy=ignore: advisory fast feedback only; the controller backstop re-checks the
// referencePolicy every reconcile, so a webhook outage must not wedge GarageKey applies.
// +kubebuilder:webhook:path=/validate-garage-rottler-io-v1alpha1-garagekey,mutating=false,failurePolicy=ignore,sideEffects=None,groups=garage.rottler.io,resources=garagekeys,verbs=create;update,versions=v1alpha1,name=vgaragekey-v1alpha1.kb.io,admissionReviewVersions=v1

// GarageKeyCustomValidator validates that a GarageKey may reference its target cluster, per
// that cluster's spec.referencePolicy.
type GarageKeyCustomValidator struct {
	// Reader loads the referenced GarageCluster and, when a namespaceSelector is in play, the
	// referencing Namespace's labels.
	Reader client.Reader
}

var _ admission.Validator[*garagev1alpha1.GarageKey] = &GarageKeyCustomValidator{}

// ValidateCreate validates the clusterRef against the target cluster's referencePolicy.
func (v *GarageKeyCustomValidator) ValidateCreate(ctx context.Context, key *garagev1alpha1.GarageKey) (admission.Warnings, error) {
	return v.validateReference(ctx, key)
}

// ValidateUpdate re-validates only when clusterRef changes — i.e. the reference was repointed at
// a different cluster. The namespace (which drives the decision) is immutable, so an update that
// leaves clusterRef untouched can never flip the verdict. Skipping those is essential: the
// operator removes its finalizer via an Update, so re-validating every update would let a
// later-tightened policy wedge a key in Terminating.
func (v *GarageKeyCustomValidator) ValidateUpdate(ctx context.Context, oldKey, newKey *garagev1alpha1.GarageKey) (admission.Warnings, error) {
	if oldKey.Spec.ClusterRef == newKey.Spec.ClusterRef {
		return nil, nil
	}
	return v.validateReference(ctx, newKey)
}

// ValidateDelete is a no-op: the reference policy gates active management, not teardown, so a
// later-tightened policy must never strand a key by blocking its deletion.
func (v *GarageKeyCustomValidator) ValidateDelete(_ context.Context, _ *garagev1alpha1.GarageKey) (admission.Warnings, error) {
	return nil, nil
}

func (v *GarageKeyCustomValidator) validateReference(ctx context.Context, key *garagev1alpha1.GarageKey) (admission.Warnings, error) {
	allowed, reason, err := refpolicy.Check(ctx, v.Reader, key.Spec.ClusterRef, key.Namespace)
	if err != nil {
		// Fail open: the controller backstop enforces the policy regardless.
		garagekeylog.Error(err, "Could not evaluate referencePolicy; allowing", "name", key.Name)
		return nil, nil
	}
	if !allowed {
		return nil, field.Forbidden(field.NewPath("spec", "clusterRef"), reason)
	}
	return nil, nil
}
