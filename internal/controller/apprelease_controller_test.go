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
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	marketplacev1alpha1 "github.com/0xd1sph0l1dus/plane-marketplace-operator/api/v1alpha1"
)

var _ = Describe("AppRelease Controller", func() {
	// Reconciler "hors-ligne" : S3 pointé sur un port fermé (127.0.0.1:1) →
	// échec immédiat et déterministe, sans réseau ni credentials.
	const (
		testNamespace     = "default"
		noDriftName       = "no-drift"
		driftResourceName = "drift-unreachable"
	)
	newReconciler := func() *AppReleaseReconciler {
		return &AppReleaseReconciler{
			Client: k8sClient,
			Scheme: scheme.Scheme,
			S3Client: s3.New(s3.Options{
				Region:       "us-east-1",
				BaseEndpoint: aws.String("http://127.0.0.1:1"),
			}),
			ArtifactDir: GinkgoT().TempDir(),
		}
	}

	It("does nothing when the desired version is already installed", func() {
		ar := &marketplacev1alpha1.AppRelease{
			ObjectMeta: metav1.ObjectMeta{Name: noDriftName, Namespace: testNamespace},
			Spec: marketplacev1alpha1.AppReleaseSpec{
				AppName: noDriftName,
				Version: "1.0.0",
				Source:  "s3://bucket/chart.tgz",
				Digest:  "sha256:" + strings.Repeat("0", 64),
			},
		}
		Expect(k8sClient.Create(ctx, ar)).To(Succeed())

		ar.Status.InstalledVersion = "1.0.0"
		Expect(k8sClient.Status().Update(ctx, ar)).To(Succeed())

		_, err := newReconciler().Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: noDriftName},
		})
		Expect(err).NotTo(HaveOccurred())

		updated := &marketplacev1alpha1.AppRelease{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: noDriftName}, updated)).To(Succeed())
		cond := meta.FindStatusCondition(updated.Status.Conditions, "Ready")
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionTrue))
		Expect(cond.Reason).To(Equal("UpToDate"))
	})

	It("marks DownloadError when there is drift but the artifact is unreachable", func() {
		ar := &marketplacev1alpha1.AppRelease{
			ObjectMeta: metav1.ObjectMeta{Name: driftResourceName, Namespace: testNamespace},
			Spec: marketplacev1alpha1.AppReleaseSpec{
				AppName: driftResourceName,
				Version: "2.0.0",
				Source:  "s3://bucket/chart.tgz",
				Digest:  "sha256:" + strings.Repeat("0", 64),
			},
		}
		Expect(k8sClient.Create(ctx, ar)).To(Succeed())

		_, err := newReconciler().Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: driftResourceName},
		})
		Expect(err).To(HaveOccurred()) // erreur transitoire : le framework réessaierait avec backoff

		updated := &marketplacev1alpha1.AppRelease{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: driftResourceName}, updated)).To(Succeed())
		cond := meta.FindStatusCondition(updated.Status.Conditions, "Ready")
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal("DownloadError"))
	})
})
