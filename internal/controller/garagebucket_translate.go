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
	garagev1alpha1 "github.com/QuantumDancer/garage-operator/api/v1alpha1"
	"github.com/QuantumDancer/garage-operator/internal/garageadmin"
)

// buildUpdateBody renders the CR's website, quotas, CORS and lifecycle into an
// UpdateBucketRequestBody. Every field is sent on each reconcile (never left nil) so the CR is
// authoritative: clearing a section in the spec clears it in Garage, rather than leaving stale
// settings behind.
func buildUpdateBody(spec *garagev1alpha1.GarageBucketSpec) garageadmin.UpdateBucketRequestBody {
	cors := translateCORS(spec.CORS)
	lifecycle := translateLifecycle(spec.Lifecycle)
	return garageadmin.UpdateBucketRequestBody{
		WebsiteAccess:  translateWebsite(spec.Website),
		Quotas:         translateQuotas(spec.Quotas),
		CorsRules:      &cors,
		LifecycleRules: &lifecycle,
	}
}

func translateWebsite(w *garagev1alpha1.BucketWebsite) *garageadmin.UpdateBucketWebsiteAccess {
	access := &garageadmin.UpdateBucketWebsiteAccess{}
	if w == nil {
		return access // Enabled defaults to false, disabling website hosting.
	}
	access.Enabled = w.Enabled
	access.IndexDocument = stringPtrOrNil(w.IndexDocument)
	access.ErrorDocument = stringPtrOrNil(w.ErrorDocument)
	return access
}

func translateQuotas(q *garagev1alpha1.BucketQuotas) *garageadmin.ApiBucketQuotas {
	quotas := &garageadmin.ApiBucketQuotas{}
	if q == nil {
		return quotas // Both limits nil -> unlimited.
	}
	quotas.MaxObjects = q.MaxObjects
	if q.MaxSize != nil {
		bytes := q.MaxSize.Value()
		quotas.MaxSize = &bytes
	}
	return quotas
}

func translateCORS(rules []garagev1alpha1.CORSRule) []garageadmin.CorsRule {
	out := make([]garageadmin.CorsRule, 0, len(rules))
	for i := range rules {
		r := &rules[i]
		out = append(out, garageadmin.CorsRule{
			ID:            stringValueOrNil(r.ID),
			AllowedOrigin: toAnySlice(r.AllowedOrigins),
			AllowedMethod: toAnySlice(r.AllowedMethods),
			AllowedHeader: toAnySlicePtr(r.AllowedHeaders),
			ExposeHeader:  toAnySlicePtr(r.ExposeHeaders),
			MaxAgeSeconds: r.MaxAgeSeconds,
		})
	}
	return out
}

func translateLifecycle(rules []garagev1alpha1.LifecycleRule) []garageadmin.LifecycleRule {
	out := make([]garageadmin.LifecycleRule, 0, len(rules))
	for i := range rules {
		r := &rules[i]
		out = append(out, garageadmin.LifecycleRule{
			ID:                             stringValueOrNil(r.ID),
			Status:                         string(r.Status),
			Filter:                         translateLifecycleFilter(r.Filter),
			Expiration:                     translateLifecycleExpiration(r.Expiration),
			AbortIncompleteMultipartUpload: translateAbort(r.AbortIncompleteMultipartUpload),
		})
	}
	return out
}

func translateLifecycleFilter(f *garagev1alpha1.LifecycleFilter) *garageadmin.LifecycleFilter {
	if f == nil {
		return nil
	}
	return &garageadmin.LifecycleFilter{
		Prefix:                stringValueOrNil(f.Prefix),
		ObjectSizeGreaterThan: f.ObjectSizeGreaterThan,
		ObjectSizeLessThan:    f.ObjectSizeLessThan,
	}
}

func translateLifecycleExpiration(e *garagev1alpha1.LifecycleExpiration) *garageadmin.LifecycleExpiration {
	if e == nil {
		return nil
	}
	out := &garageadmin.LifecycleExpiration{Date: stringValueOrNil(e.Date)}
	if e.Days != nil {
		days := int64(*e.Days)
		out.Days = &days
	}
	return out
}

func translateAbort(a *garagev1alpha1.AbortIncompleteMultipartUpload) *garageadmin.LifecycleAbortIncompleteMpu {
	if a == nil {
		return nil
	}
	return &garageadmin.LifecycleAbortIncompleteMpu{DaysAfterInitiation: int64(a.DaysAfterInitiation)}
}

// toAnySlice widens a []string to the []interface{} the generated S3-shaped types use (their
// schema declared untyped array items). Always returns a non-nil slice for required fields.
func toAnySlice(in []string) []any {
	out := make([]any, len(in))
	for i, v := range in {
		out[i] = v
	}
	return out
}

func toAnySlicePtr(in []string) *[]any {
	if len(in) == 0 {
		return nil
	}
	out := toAnySlice(in)
	return &out
}

func stringPtrOrNil(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// stringValueOrNil returns s as an interface{} value, or nil when empty, matching the
// omitempty semantics of the generated untyped (interface{}) fields.
func stringValueOrNil(s string) any {
	if s == "" {
		return nil
	}
	return s
}
