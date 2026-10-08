package controller

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	// ReconcileTotal compte les réconciliations par app et résultat.
	ReconcileTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "marketplace_reconcile_total",
			Help: "Total number of reconciliations by application and result",
		},
		[]string{"app", "result"},
	)

	// InstallDuration mesure la durée d'une installation (Helm + attente).
	InstallDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "marketplace_install_duration_seconds",
			Help:    "Duration of marketplace installs in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"app"},
	)

	// DigestMismatchTotal compte les artefacts refusés pour corruption.
	DigestMismatchTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "marketplace_digest_mismatch_total",
			Help: "Total number of artifacts rejected because of a digest mismatch",
		},
		[]string{"app"},
	)
)

func init() {
	// Le registre controller-runtime alimente automatiquement le /metrics du manager.
	metrics.Registry.MustRegister(ReconcileTotal, InstallDuration, DigestMismatchTotal)
}
