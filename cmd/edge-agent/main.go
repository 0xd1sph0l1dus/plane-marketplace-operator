package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	marketplacev1alpha1 "github.com/0xd1sph0l1dus/airbus-marketplace-operator/api/v1alpha1"
)

type desiredMessage struct {
	APIVersion string `json:"apiVersion"`
	Aircraft   string `json:"aircraft"`
	AppName    string `json:"appName"`
	Version    string `json:"version"`
	Source     string `json:"source"`
	Digest     string `json:"digest"`
}

type statusMessage struct {
	APIVersion       string `json:"apiVersion"`
	Aircraft         string `json:"aircraft"`
	AppName          string `json:"appName"`
	Version          string `json:"version"`
	InstalledVersion string `json:"installedVersion"`
	Ready            bool   `json:"ready"`
	Reason           string `json:"reason,omitempty"`
}

// receiveLoop : poll le désiré, traduit en CR, ack après succès
func receiveLoop(ctx context.Context, c *sqs.Client, k8s client.Client, queueURL, aircraft string) {
	attempt := 0
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		out, err := c.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(queueURL),
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     20, // long-polling : latence basse sans marteler le réseau
		})
		if err != nil {
			log.Printf("receive failed (attempt %d): %v", attempt, err)
			sleepBackoff(attempt)
			attempt++
			continue
		}
		attempt = 0 // le lien est OK → reset du backoff

		for _, msg := range out.Messages {
			if err := handleDesired(ctx, k8s, aircraft, msg.Body); err != nil {
				log.Printf("handle desired failed: %v (message left in queue, will be redelivered)", err)
				continue // pas de DeleteMessage → SQS redélivrera (at-least-once)
			}
			if _, err := c.DeleteMessage(ctx, &sqs.DeleteMessageInput{
				QueueUrl:      aws.String(queueURL),
				ReceiptHandle: msg.ReceiptHandle,
			}); err != nil {
				log.Printf("delete failed: %v", err)
			}
		}
	}
}

// handleDesired : JSON → CR (create ou update de la spec uniquement)
func handleDesired(ctx context.Context, k8s client.Client, aircraft string, body *string) error {
	var dm desiredMessage
	if err := json.Unmarshal([]byte(aws.ToString(body)), &dm); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}
	if dm.APIVersion != "v1" {
		return fmt.Errorf("unsupported apiVersion %q, skipping", dm.APIVersion)
	}
	if dm.Aircraft != "" && dm.Aircraft != aircraft {
		log.Printf("message for %s, we are %s — ignoring", dm.Aircraft, aircraft)
		return nil
	}

	ar := &marketplacev1alpha1.AppRelease{}
	err := k8s.Get(ctx, types.NamespacedName{Namespace: "marketplace", Name: dm.AppName}, ar)
	if errors.IsNotFound(err) {
		ar = &marketplacev1alpha1.AppRelease{
			ObjectMeta: metav1.ObjectMeta{Name: dm.AppName, Namespace: "marketplace"},
			Spec: marketplacev1alpha1.AppReleaseSpec{
				AppName: dm.AppName, Version: dm.Version, Source: dm.Source, Digest: dm.Digest,
			},
		}
		if err := k8s.Create(ctx, ar); err != nil {
			return fmt.Errorf("create CR: %w", err)
		}
		log.Printf("created AppRelease %s v%s", dm.AppName, dm.Version)
		return nil
	}
	if err != nil {
		return fmt.Errorf("get CR: %w", err)
	}

	ar.Spec = marketplacev1alpha1.AppReleaseSpec{
		AppName: dm.AppName, Version: dm.Version, Source: dm.Source, Digest: dm.Digest,
	}
	if err := k8s.Update(ctx, ar); err != nil {
		return fmt.Errorf("update CR: %w", err)
	}
	log.Printf("updated AppRelease %s v%s", dm.AppName, dm.Version)
	return nil
}

// statusLoop : toutes les 5s, remonte l'état réel de chaque CR
func statusLoop(ctx context.Context, c *sqs.Client, k8s client.Client, queueURL, aircraft string) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := pushStatuses(ctx, c, k8s, queueURL, aircraft); err != nil {
				log.Printf("push statuses failed: %v", err)
			}
		}
	}
}

func pushStatuses(ctx context.Context, c *sqs.Client, k8s client.Client, queueURL, aircraft string) error {
	list := &marketplacev1alpha1.AppReleaseList{}
	if err := k8s.List(ctx, list, client.InNamespace("marketplace")); err != nil {
		return err
	}
	for i := range list.Items {
		ar := &list.Items[i]
		ready, reason := readyState(ar)
		body, err := json.Marshal(statusMessage{
			APIVersion:       "v1",
			Aircraft:         aircraft,
			AppName:          ar.Spec.AppName,
			Version:          ar.Spec.Version,
			InstalledVersion: ar.Status.InstalledVersion,
			Ready:            ready,
			Reason:           reason,
		})
		if err != nil {
			return err
		}
		if _, err := c.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl:       aws.String(queueURL),
			MessageBody:    aws.String(string(body)),
			MessageGroupId: aws.String(aircraft), // une file logique par avion
		}); err != nil {
			return err
		}
	}
	if len(list.Items) > 0 {
		log.Printf("pushed %d status(es)", len(list.Items))
	}
	return nil
}

func readyState(ar *marketplacev1alpha1.AppRelease) (bool, string) {
	for _, cond := range ar.Status.Conditions {
		if cond.Type == "Ready" {
			return cond.Status == metav1.ConditionTrue, cond.Reason
		}
	}
	return false, "Unknown"
}

// sleepBackoff : exponentiel (1s→60s) + jitter pour éviter la synchronisation des retries
func sleepBackoff(attempt int) {
	d := time.Duration(math.Pow(2, float64(attempt))) * time.Second
	if d > 60*time.Second {
		d = 60 * time.Second
	}
	jitter := time.Duration(rand.Int63n(int64(d/2) + 1))
	time.Sleep(d + jitter)
}

func main() {
	aircraft := os.Getenv("AIRCRAFT_ID")
	if aircraft == "" {
		aircraft = "a380-01"
	}
	desiredURL := os.Getenv("EDGE_DESIRED_QUEUE_URL")
	statusURL := os.Getenv("EDGE_STATUS_QUEUE_URL")
	if desiredURL == "" || statusURL == "" {
		log.Fatal("EDGE_DESIRED_QUEUE_URL and EDGE_STATUS_QUEUE_URL must be set")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Client Kubernetes : même API que l'opérateur, mais sans manager (un client nu suffit)
	restCfg := ctrl.GetConfigOrDie()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = marketplacev1alpha1.AddToScheme(scheme)
	k8sClient, err := client.New(restCfg, client.Options{Scheme: scheme})
	if err != nil {
		log.Fatalf("k8s client: %v", err)
	}

	// Client SQS : même chaîne de credentials que l'opérateur (clés edge)
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("aws config: %v", err)
	}
	sqsClient := sqs.NewFromConfig(awsCfg)

	log.Printf("edge-agent starting, aircraft=%s", aircraft)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); receiveLoop(ctx, sqsClient, k8sClient, desiredURL, aircraft) }()
	go func() { defer wg.Done(); statusLoop(ctx, sqsClient, k8sClient, statusURL, aircraft) }()

	<-ctx.Done()
	log.Println("shutdown signal received, waiting for loops...")
	wg.Wait()
	log.Println("edge-agent stopped")
}
