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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// ClusterReference points at a GarageCluster, possibly in another namespace.
type ClusterReference struct {
	// name is the referenced GarageCluster's name.
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// namespace is the referenced GarageCluster's namespace; defaults to the referencing
	// object's own namespace when omitted.
	// +optional
	Namespace string `json:"namespace,omitempty"`
}

// KeyReference points at a GarageKey, possibly in another namespace.
type KeyReference struct {
	// name is the referenced GarageKey's name.
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// namespace is the referenced GarageKey's namespace; defaults to the referencing
	// object's own namespace when omitted.
	// +optional
	Namespace string `json:"namespace,omitempty"`
}

// BucketWebsite configures static website hosting for a bucket.
type BucketWebsite struct {
	// enabled turns website serving on for the bucket.
	// +kubebuilder:validation:Required
	Enabled bool `json:"enabled"`

	// indexDocument is the object key served at the root of the site (e.g. index.html).
	// +optional
	IndexDocument string `json:"indexDocument,omitempty"`

	// errorDocument is the object key served for 4xx responses (e.g. error.html).
	// +optional
	ErrorDocument string `json:"errorDocument,omitempty"`
}

// BucketQuotas caps a bucket's size and/or object count. Omit a field to leave it unlimited.
type BucketQuotas struct {
	// maxSize is the maximum total size of all objects in the bucket (e.g. 50Gi).
	// +optional
	MaxSize *resource.Quantity `json:"maxSize,omitempty"`

	// maxObjects is the maximum number of objects in the bucket.
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxObjects *int64 `json:"maxObjects,omitempty"`
}

// CORSRule is a single cross-origin resource sharing rule. It mirrors the S3 CORS rule
// Garage accepts, rendered in Kubernetes camelCase.
type CORSRule struct {
	// id optionally identifies the rule.
	// +optional
	ID string `json:"id,omitempty"`

	// allowedOrigins are the origins (e.g. "*", "https://example.com") the rule applies to.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinItems=1
	AllowedOrigins []string `json:"allowedOrigins"`

	// allowedMethods are the HTTP methods (e.g. GET, PUT) permitted for the origins.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinItems=1
	AllowedMethods []string `json:"allowedMethods"`

	// allowedHeaders are request headers permitted in a preflight request.
	// +optional
	AllowedHeaders []string `json:"allowedHeaders,omitempty"`

	// exposeHeaders are response headers exposed to the browser.
	// +optional
	ExposeHeaders []string `json:"exposeHeaders,omitempty"`

	// maxAgeSeconds is how long a browser may cache a preflight response.
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxAgeSeconds *int64 `json:"maxAgeSeconds,omitempty"`
}

// LifecycleRuleStatus enables or disables a lifecycle rule.
// +kubebuilder:validation:Enum=Enabled;Disabled
type LifecycleRuleStatus string

const (
	LifecycleRuleEnabled  LifecycleRuleStatus = "Enabled"
	LifecycleRuleDisabled LifecycleRuleStatus = "Disabled"
)

// LifecycleFilter narrows which objects a lifecycle rule applies to. All set conditions
// must match (logical AND).
// +kubebuilder:validation:XValidation:rule="!has(self.objectSizeGreaterThan) || !has(self.objectSizeLessThan) || self.objectSizeGreaterThan < self.objectSizeLessThan",message="objectSizeGreaterThan must be less than objectSizeLessThan"
type LifecycleFilter struct {
	// prefix matches object keys beginning with this string.
	// +optional
	Prefix string `json:"prefix,omitempty"`

	// objectSizeGreaterThan matches objects strictly larger than this many bytes.
	// +kubebuilder:validation:Minimum=0
	// +optional
	ObjectSizeGreaterThan *int64 `json:"objectSizeGreaterThan,omitempty"`

	// objectSizeLessThan matches objects strictly smaller than this many bytes.
	// +kubebuilder:validation:Minimum=0
	// +optional
	ObjectSizeLessThan *int64 `json:"objectSizeLessThan,omitempty"`
}

// LifecycleExpiration expires matching objects either after a number of days or on an
// absolute date. Set exactly one of days or date.
// +kubebuilder:validation:XValidation:rule="has(self.days) != (has(self.date) && size(self.date) > 0)",message="expiration must set exactly one of days or date"
type LifecycleExpiration struct {
	// days expires objects this many days after creation.
	// +kubebuilder:validation:Minimum=1
	// +optional
	Days *int32 `json:"days,omitempty"`

	// date expires objects on this RFC 3339 calendar date (e.g. "2026-01-01").
	// +optional
	Date string `json:"date,omitempty"`
}

// AbortIncompleteMultipartUpload aborts unfinished multipart uploads after a delay.
type AbortIncompleteMultipartUpload struct {
	// daysAfterInitiation aborts uploads this many days after they were started.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Required
	DaysAfterInitiation int32 `json:"daysAfterInitiation"`
}

// LifecycleRule is one object-lifecycle rule on the bucket.
// +kubebuilder:validation:XValidation:rule="has(self.expiration) || has(self.abortIncompleteMultipartUpload)",message="a lifecycle rule must set expiration or abortIncompleteMultipartUpload"
type LifecycleRule struct {
	// id optionally identifies the rule.
	// +optional
	ID string `json:"id,omitempty"`

	// status enables or disables the rule.
	// +kubebuilder:validation:Required
	Status LifecycleRuleStatus `json:"status"`

	// filter narrows which objects the rule applies to; omitted means the whole bucket.
	// +optional
	Filter *LifecycleFilter `json:"filter,omitempty"`

	// expiration expires matching objects.
	// +optional
	Expiration *LifecycleExpiration `json:"expiration,omitempty"`

	// abortIncompleteMultipartUpload cleans up unfinished multipart uploads.
	// +optional
	AbortIncompleteMultipartUpload *AbortIncompleteMultipartUpload `json:"abortIncompleteMultipartUpload,omitempty"`
}

// LocalAlias binds a per-key alias name to a GarageKey for this bucket.
//
// NOTE: local aliases are reconciled in Phase 3, once the GarageKey controller exists to
// resolve keyRef to a Garage access key id. The field is accepted now but not yet acted on.
type LocalAlias struct {
	// keyRef is the GarageKey the alias is scoped to.
	// +kubebuilder:validation:Required
	KeyRef KeyReference `json:"keyRef"`

	// alias is the bucket name visible to that key.
	// +kubebuilder:validation:Required
	Alias string `json:"alias"`
}

// BucketGrant grants a GarageKey permissions on this bucket.
//
// NOTE: grants are reconciled in Phase 3, once the GarageKey controller exists to resolve
// keyRef to a Garage access key id. The field is accepted now but not yet acted on.
type BucketGrant struct {
	// keyRef is the GarageKey the permissions are granted to.
	// +kubebuilder:validation:Required
	KeyRef KeyReference `json:"keyRef"`

	// read allows the key to read objects from the bucket.
	// +optional
	Read bool `json:"read,omitempty"`

	// write allows the key to write objects to the bucket.
	// +optional
	Write bool `json:"write,omitempty"`

	// owner allows the key to manage the bucket itself (aliases, website, quotas).
	// +optional
	Owner bool `json:"owner,omitempty"`
}

// BucketDeletionPolicy controls what happens to the underlying Garage bucket when the
// GarageBucket CR is deleted.
// +kubebuilder:validation:Enum=Retain;Delete
type BucketDeletionPolicy string

const (
	// BucketDeletionRetain leaves the Garage bucket in place when the CR is deleted.
	BucketDeletionRetain BucketDeletionPolicy = "Retain"
	// BucketDeletionDelete deletes the Garage bucket when the CR is deleted.
	BucketDeletionDelete BucketDeletionPolicy = "Delete"
)

// GarageBucketSpec defines the desired state of GarageBucket.
type GarageBucketSpec struct {
	// clusterRef selects the GarageCluster the bucket lives in.
	// +kubebuilder:validation:Required
	ClusterRef ClusterReference `json:"clusterRef"`

	// globalAliases are cluster-wide unique aliases for the bucket. Empty means the bucket
	// has no global alias and is addressable only by its id. Aliases must be unique within
	// this bucket; cluster-wide uniqueness is enforced by Garage and surfaced via status.
	// +listType=set
	// +optional
	GlobalAliases []string `json:"globalAliases,omitempty"`

	// localAliases are per-key aliases. Reconciled in Phase 3 (see LocalAlias).
	// +optional
	LocalAliases []LocalAlias `json:"localAliases,omitempty"`

	// website configures static website hosting.
	// +optional
	Website *BucketWebsite `json:"website,omitempty"`

	// quotas caps the bucket's size and/or object count.
	// +optional
	Quotas *BucketQuotas `json:"quotas,omitempty"`

	// cors are the bucket's cross-origin resource sharing rules.
	// +optional
	CORS []CORSRule `json:"cors,omitempty"`

	// lifecycle are the bucket's object-lifecycle rules.
	// +optional
	Lifecycle []LifecycleRule `json:"lifecycle,omitempty"`

	// grants grant GarageKeys permissions on the bucket. Reconciled in Phase 3 (see BucketGrant).
	// +optional
	Grants []BucketGrant `json:"grants,omitempty"`

	// deletionPolicy controls whether the Garage bucket is deleted with the CR. The default
	// is Delete (fully-managed: deleting the CR deletes the bucket); a non-empty bucket is
	// refused until emptied. Set Retain to leave the bucket in place when the CR is deleted.
	// +kubebuilder:default=Delete
	// +optional
	DeletionPolicy BucketDeletionPolicy `json:"deletionPolicy,omitempty"`
}

// GarageBucketStatus defines the observed state of GarageBucket.
type GarageBucketStatus struct {
	// conditions represent the current state of the GarageBucket resource.
	// Known types: Ready.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// bucketId is the Garage bucket identifier the CR is bound to.
	// +optional
	BucketID string `json:"bucketId,omitempty"`

	// observedGeneration is the generation most recently reconciled into status.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="BucketId",type=string,JSONPath=".status.bucketId"
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=".spec.clusterRef.name"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// GarageBucket is the Schema for the garagebuckets API
type GarageBucket struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of GarageBucket
	// +required
	Spec GarageBucketSpec `json:"spec"`

	// status defines the observed state of GarageBucket
	// +optional
	Status GarageBucketStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// GarageBucketList contains a list of GarageBucket
type GarageBucketList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []GarageBucket `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &GarageBucket{}, &GarageBucketList{})
		return nil
	})
}
