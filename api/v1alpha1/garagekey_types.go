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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// KeyPermissions are the global capabilities of an access key (as opposed to per-bucket
// grants, which live on GarageBucket).
type KeyPermissions struct {
	// createBucket lets the key create new buckets on its own (Garage KeyPerm.createBucket).
	// +optional
	CreateBucket bool `json:"createBucket,omitempty"`
}

// KeyImport adopts an existing S3 access key instead of generating a fresh one. Both halves
// of the credential are read from a single Secret.
type KeyImport struct {
	// secretName is the Secret holding the credentials to import.
	// +kubebuilder:validation:Required
	SecretName string `json:"secretName"`

	// accessKeyIdKey is the Secret data key holding the access key id; defaults to accessKeyId.
	// +optional
	AccessKeyIDKey string `json:"accessKeyIdKey,omitempty"`

	// secretAccessKeyKey is the Secret data key holding the secret key; defaults to secretAccessKey.
	// +optional
	SecretAccessKeyKey string `json:"secretAccessKeyKey,omitempty"`
}

// KeyOutput controls where the resulting credentials are published.
type KeyOutput struct {
	// secretName is the Secret the operator writes the credentials into; defaults to
	// <cr-name>-credentials. The Secret carries data keys accessKeyId and secretAccessKey.
	// +optional
	SecretName string `json:"secretName,omitempty"`
}

// KeyDeletionPolicy controls what happens to the underlying Garage key (and its credentials
// Secret) when the GarageKey CR is deleted.
// +kubebuilder:validation:Enum=Retain;Delete
type KeyDeletionPolicy string

const (
	// KeyDeletionRetain leaves the Garage key and its Secret in place when the CR is deleted.
	KeyDeletionRetain KeyDeletionPolicy = "Retain"
	// KeyDeletionDelete deletes the Garage key (and its Secret) when the CR is deleted.
	KeyDeletionDelete KeyDeletionPolicy = "Delete"
)

// GarageKeySpec defines the desired state of GarageKey.
// +kubebuilder:validation:XValidation:rule="!has(self.renewBefore) || has(self.expiration)",message="renewBefore requires expiration to be set"
type GarageKeySpec struct {
	// clusterRef selects the GarageCluster the key belongs to.
	// +kubebuilder:validation:Required
	ClusterRef ClusterReference `json:"clusterRef"`

	// name is the display name in Garage; defaults to metadata.name. Garage key names are
	// freeform, non-unique labels (keys are identified by id), so this is only a human label.
	// +optional
	Name string `json:"name,omitempty"`

	// permissions are the key's global capabilities.
	// +optional
	Permissions KeyPermissions `json:"permissions,omitempty"`

	// expiration is when the key stops working (RFC 3339). Omitted means the key never expires.
	// +optional
	Expiration *metav1.Time `json:"expiration,omitempty"`

	// renewBefore asks the operator to rotate the key this long before expiration. Reconciled
	// once the §10 rotation flow lands; ignored without expiration and currently only stored.
	// +optional
	RenewBefore *metav1.Duration `json:"renewBefore,omitempty"`

	// import adopts an existing key instead of generating one.
	// +optional
	Import *KeyImport `json:"import,omitempty"`

	// output controls where the resulting credentials Secret is written.
	// +optional
	Output KeyOutput `json:"output,omitempty"`

	// deletionPolicy controls whether the Garage key is deleted with the CR. The default is
	// Delete; set Retain to leave the key and its Secret in place when the CR is deleted.
	// +kubebuilder:default=Delete
	// +optional
	DeletionPolicy KeyDeletionPolicy `json:"deletionPolicy,omitempty"`
}

// GarageKeyStatus defines the observed state of GarageKey.
type GarageKeyStatus struct {
	// conditions represent the current state of the GarageKey resource.
	// Known types: Ready, CredentialsPublished.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// keyId is the Garage access key id the CR is bound to.
	// +optional
	KeyID string `json:"keyId,omitempty"`

	// credentialsSecret is the Secret the access credentials were written to.
	// +optional
	CredentialsSecret string `json:"credentialsSecret,omitempty"`

	// expiration mirrors GetKeyInfo; absent when the key never expires.
	// +optional
	Expiration *metav1.Time `json:"expiration,omitempty"`

	// expired mirrors GetKeyInfo.expired.
	// +optional
	Expired bool `json:"expired,omitempty"`

	// observedGeneration is the generation most recently reconciled into status.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="KeyId",type=string,JSONPath=".status.keyId"
// +kubebuilder:printcolumn:name="Secret",type=string,JSONPath=".status.credentialsSecret"
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=".spec.clusterRef.name"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// GarageKey is the Schema for the garagekeys API
type GarageKey struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of GarageKey
	// +required
	Spec GarageKeySpec `json:"spec"`

	// status defines the observed state of GarageKey
	// +optional
	Status GarageKeyStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// GarageKeyList contains a list of GarageKey
type GarageKeyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []GarageKey `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &GarageKey{}, &GarageKeyList{})
		return nil
	})
}
