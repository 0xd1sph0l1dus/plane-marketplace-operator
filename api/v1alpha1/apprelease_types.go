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

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// AppReleaseSpec defines the desired state of AppRelease
type AppReleaseSpec struct {
	// AppName is the stable identity of the marketplace application (e.g. "streaming-film").
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	AppName string `json:"appName"`

	// Version is the desired version to run. Upgrading means changing this field.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^v?[0-9]+\.[0-9]+\.[0-9]+$`
	Version string `json:"version"`

	// Source is the S3 location of the Helm chart archive.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^s3://[a-z0-9.-]+/[a-zA-Z0-9._/-]+$`
	Source string `json:"source"`

	// Digest is the sha256 checksum of the chart archive, used to verify integrity after transfer.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^sha256:[a-f0-9]{64}$`
	Digest string `json:"digest"`
}

// AppReleaseStatus defines the observed state of AppRelease.
type AppReleaseStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// For Kubernetes API conventions, see:
	// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#typical-status-properties

	// conditions represent the current state of the AppRelease resource.
	// Each condition has a unique type and reflects the status of a specific aspect of the resource.
	//
	// Standard condition types include:
	// - "Available": the resource is fully functional
	// - "Progressing": the resource is being created or updated
	// - "Degraded": the resource failed to reach or maintain its desired state
	//
	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// InstalledVersion is the version currently running on the aircraft.
	// +optional
	InstalledVersion string `json:"installedVersion,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// AppRelease is the Schema for the appreleases API
type AppRelease struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of AppRelease
	// +required
	Spec AppReleaseSpec `json:"spec"`

	// status defines the observed state of AppRelease
	// +optional
	Status AppReleaseStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// AppReleaseList contains a list of AppRelease
type AppReleaseList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []AppRelease `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &AppRelease{}, &AppReleaseList{})
		return nil
	})
}
