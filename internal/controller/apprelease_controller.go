package controller

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/cli"
	"helm.sh/helm/v3/pkg/storage/driver"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	marketplacev1alpha1 "github.com/0xd1sph0l1dus/plane-marketplace-operator/api/v1alpha1"
)

const marketplaceFinalizer = "marketplace.edge-ops.dev/finalizer"
const conditionReady = "Ready"

// AppReleaseReconciler reconciles a AppRelease object
type AppReleaseReconciler struct {
	client.Client
	Scheme      *runtime.Scheme
	S3Client    *s3.Client
	ArtifactDir string
}

// +kubebuilder:rbac:groups=marketplace.edge-ops.dev,resources=appreleases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=marketplace.edge-ops.dev,resources=appreleases/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=marketplace.edge-ops.dev,resources=appreleases/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the AppRelease object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.25.0/pkg/reconcile
func (r *AppReleaseReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// 1. Lire le bon de commande
	ar := &marketplacev1alpha1.AppRelease{}
	if err := r.Get(ctx, req.NamespacedName, ar); err != nil {
		if client.IgnoreNotFound(err) == nil {
			log.Info("AppRelease deleted, nothing to do", "name", req.Name)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Désinstallation : CR marqué pour suppression → nettoyer le release Helm
	if !ar.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(ar, marketplaceFinalizer) {
			log.Info("CR is being deleted, uninstalling Helm release", "app", ar.Spec.AppName)
			if err := r.uninstallChart(ar.Spec.AppName, "marketplace"); err != nil {
				return ctrl.Result{}, fmt.Errorf("uninstall chart: %w", err)
			}
			controllerutil.RemoveFinalizer(ar, marketplaceFinalizer)
			if err := r.Update(ctx, ar); err != nil {
				return ctrl.Result{}, err
			}
			log.Info("App uninstalled", "app", ar.Spec.AppName)
		}
		return ctrl.Result{}, nil
	}

	// À la création : poser le finalizer pour garantir la désinstallation future
	if !controllerutil.ContainsFinalizer(ar, marketplaceFinalizer) {
		controllerutil.AddFinalizer(ar, marketplaceFinalizer)
		if err := r.Update(ctx, ar); err != nil {
			return ctrl.Result{}, err
		}
	}

	// 2. Pas d'écart désiré/réel ? Rien à faire (idempotence)
	if ar.Status.InstalledVersion == ar.Spec.Version {
		meta.SetStatusCondition(&ar.Status.Conditions, metav1.Condition{
			Type:    conditionReady,
			Status:  metav1.ConditionTrue,
			Reason:  "UpToDate",
			Message: fmt.Sprintf("Version %s is installed", ar.Spec.Version),
		})
		if err := r.updateStatus(ctx, ar); err != nil {
			return ctrl.Result{}, err
		}
		ReconcileTotal.WithLabelValues(ar.Spec.AppName, "noop").Inc()
		return ctrl.Result{}, nil
	}

	log.Info("Drift detected, starting install",
		"app", ar.Spec.AppName, "desired", ar.Spec.Version, "installed", ar.Status.InstalledVersion)

	// 3. Annoncer le travail en cours (le cloud verra Progressing)
	meta.SetStatusCondition(&ar.Status.Conditions, metav1.Condition{
		Type:    conditionReady,
		Status:  metav1.ConditionFalse,
		Reason:  "Progressing",
		Message: fmt.Sprintf("Installing version %s", ar.Spec.Version),
	})
	if err := r.updateStatus(ctx, ar); err != nil {
		return ctrl.Result{}, err
	}

	// 4. Télécharger l'artefact dans le cache local de l'avion
	chartPath, err := r.download(ctx, ar.Spec.Source)
	if err != nil {
		r.markFailed(ctx, ar, "DownloadError", err.Error())
		ReconcileTotal.WithLabelValues(ar.Spec.AppName, "download_error").Inc()
		return ctrl.Result{}, err
	}

	// 5. Vérifier l'intégrité — si le sha256 ne matche pas, on s'arrête
	if err := r.verifyDigest(chartPath, ar.Spec.Digest); err != nil {
		_ = os.Remove(chartPath)
		r.markFailed(ctx, ar, "DigestMismatch", err.Error())
		DigestMismatchTotal.WithLabelValues(ar.Spec.AppName).Inc()
		ReconcileTotal.WithLabelValues(ar.Spec.AppName, "digest_mismatch").Inc()
		return ctrl.Result{}, nil
	}

	// 6. Installer (ou mettre à jour) via Helm — Atomic = rollback auto si échec
	start := time.Now()
	err = r.installChart(chartPath, ar.Spec.AppName, "marketplace")
	InstallDuration.WithLabelValues(ar.Spec.AppName).Observe(time.Since(start).Seconds())
	if err != nil {
		r.markFailed(ctx, ar, "InstallError", err.Error())
		ReconcileTotal.WithLabelValues(ar.Spec.AppName, "install_error").Inc()
		return ctrl.Result{}, err
	}

	// 7. La boucle se ferme : le réel rejoint le désiré
	ar.Status.InstalledVersion = ar.Spec.Version
	meta.SetStatusCondition(&ar.Status.Conditions, metav1.Condition{
		Type:    conditionReady,
		Status:  metav1.ConditionTrue,
		Reason:  "Installed",
		Message: fmt.Sprintf("Version %s installed successfully", ar.Spec.Version),
	})
	if err := r.updateStatus(ctx, ar); err != nil {
		return ctrl.Result{}, err
	}
	log.Info("Install complete", "app", ar.Spec.AppName, "version", ar.Spec.Version)
	ReconcileTotal.WithLabelValues(ar.Spec.AppName, "installed").Inc()
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *AppReleaseReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&marketplacev1alpha1.AppRelease{}).
		Named("apprelease").
		Complete(r)
}

// markFailed écrit une condition d'échec sans bloquer la boucle
func (r *AppReleaseReconciler) markFailed(ctx context.Context, ar *marketplacev1alpha1.AppRelease, reason, msg string) {
	meta.SetStatusCondition(&ar.Status.Conditions, metav1.Condition{
		Type:    conditionReady,
		Status:  metav1.ConditionFalse,
		Reason:  reason,
		Message: msg,
	})
	_ = r.updateStatus(ctx, ar)
}

// download parse s3://bucket/key, télécharge et retourne le chemin local
func (r *AppReleaseReconciler) download(ctx context.Context, source string) (string, error) {
	u, err := url.Parse(source)
	if err != nil || u.Scheme != "s3" {
		return "", fmt.Errorf("invalid source %q: must be s3://bucket/key", source)
	}
	bucket := u.Host
	key := strings.TrimPrefix(u.Path, "/")

	if err := os.MkdirAll(r.ArtifactDir, 0o755); err != nil {
		return "", err
	}
	dest := filepath.Join(r.ArtifactDir, filepath.Base(key))

	f, err := os.Create(dest)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()

	resp, err := r.S3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()

	if _, err := io.Copy(f, resp.Body); err != nil {
		return "", err
	}
	return dest, nil
}

// verifyDigest recalcule le sha256 du fichier et le compare à spec.digest
func (r *AppReleaseReconciler) verifyDigest(path, want string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	got := fmt.Sprintf("sha256:%x", sha256.Sum256(data))
	if got != strings.TrimSpace(want) {
		return fmt.Errorf("digest mismatch: got %s want %s", got, want)
	}
	return nil
}

// installChart use Helm SDK
func (r *AppReleaseReconciler) installChart(chartPath, releaseName, namespace string) error {
	settings := cli.New()
	settings.SetNamespace(namespace)
	actionConfig := new(action.Configuration)
	if err := actionConfig.Init(settings.RESTClientGetter(), namespace, os.Getenv("HELM_DRIVER"), func(format string, v ...any) {
		fmt.Printf("helm: "+format+"\n", v...)
	}); err != nil {
		return err
	}

	ch, err := loader.Load(chartPath)
	if err != nil {
		return err
	}

	// On décide install vs upgrade : l'historique dit si le release existe
	hist := action.NewHistory(actionConfig)
	hist.Max = 1
	if _, err := hist.Run(releaseName); errors.Is(err, driver.ErrReleaseNotFound) {
		inst := action.NewInstall(actionConfig)
		inst.ReleaseName = releaseName
		inst.Namespace = namespace
		inst.Atomic = true
		inst.Timeout = 5 * time.Minute
		_, err = inst.Run(ch, nil)
		return err
	} else if err != nil {
		return err
	}

	upg := action.NewUpgrade(actionConfig)
	upg.Namespace = namespace
	upg.Atomic = true
	upg.Timeout = 5 * time.Minute
	_, err = upg.Run(releaseName, ch, nil)
	return err
}

func (r *AppReleaseReconciler) uninstallChart(releaseName, namespace string) error {
	settings := cli.New()
	settings.SetNamespace(namespace)
	actionConfig := new(action.Configuration)
	if err := actionConfig.Init(settings.RESTClientGetter(), namespace, os.Getenv("HELM_DRIVER"), func(format string, v ...any) {
		fmt.Printf("helm: "+format+"\n", v...)
	}); err != nil {
		return err
	}

	uninstall := action.NewUninstall(actionConfig)
	uninstall.Wait = true
	uninstall.Timeout = 5 * time.Minute
	_, err := uninstall.Run(releaseName)
	if err != nil && !errors.Is(err, driver.ErrReleaseNotFound) {
		return err
	}
	return nil
}

func (r *AppReleaseReconciler) updateStatus(ctx context.Context, ar *marketplacev1alpha1.AppRelease) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &marketplacev1alpha1.AppRelease{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(ar), latest); err != nil {
			return err
		}
		latest.Status = ar.Status
		return r.Status().Update(ctx, latest)
	})
}
