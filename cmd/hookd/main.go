// Command hookd runs the ingress API or a delivery worker.
//
//	hookd api      accept signed events
//	hookd worker   claim and send deliveries
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/qbeka/durable-webhook-delivery/internal/ingress"
	"github.com/qbeka/durable-webhook-delivery/internal/store"
	"github.com/qbeka/durable-webhook-delivery/internal/worker"
)

const defaultDatabaseURL = "postgres://hook@127.0.0.1:54330/hookd"

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: hookd api|worker [flags]")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var err error
	switch os.Args[1] {
	case "api":
		err = runAPI(ctx, os.Args[2:])
	case "worker":
		err = runWorker(ctx, os.Args[2:])
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		log.Fatal(err)
	}
}

func runAPI(ctx context.Context, arguments []string) error {
	flags := flag.NewFlagSet("api", flag.ExitOnError)
	addr := flags.String("addr", "127.0.0.1:9000", "listen address")
	databaseURL := flags.String("db", defaultDatabaseURL, "engine database URL")
	maxConnections := flags.Int("max-connections", 64, "database connections")
	_ = flags.Parse(arguments)

	database, err := store.Open(ctx, *databaseURL, int32(*maxConnections))
	if err != nil {
		return err
	}
	defer database.Close()
	if err := database.Migrate(ctx); err != nil {
		return err
	}

	handler := &ingress.Handler{Store: database, Now: time.Now}
	server := &http.Server{Addr: *addr, Handler: handler.Routes(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	log.Printf("ingress listening on %s", *addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func runWorker(ctx context.Context, arguments []string) error {
	hostname, _ := os.Hostname()
	flags := flag.NewFlagSet("worker", flag.ExitOnError)
	databaseURL := flags.String("db", defaultDatabaseURL, "engine database URL")
	workerID := flags.String("id", fmt.Sprintf("%s-%d", hostname, os.Getpid()), "worker ID")
	concurrency := flags.Int("concurrency", 64, "most requests in flight")
	maxConnections := flags.Int("max-connections", 32, "database connections")
	lease := flags.Duration("lease", 30*time.Second, "how long a claim stays valid")
	leaseMargin := flags.Duration("lease-margin", 2*time.Second, "a request must end this long before its lease")
	requestTimeout := flags.Duration("request-timeout", 10*time.Second, "timeout for one request")
	pollInterval := flags.Duration("poll-interval", 10*time.Millisecond, "wait after finding no work")
	reapInterval := flags.Duration("reap-interval", time.Second, "how often to look for expired leases")
	maxAttempts := flags.Int("max-attempts", 12, "attempts before a delivery goes dead")
	maxAge := flags.Duration("max-age", 72*time.Hour, "age at which a delivery goes dead")
	retryBase := flags.Duration("retry-base", time.Second, "ceiling of the first retry delay")
	retryCap := flags.Duration("retry-cap", 5*time.Minute, "largest retry delay")
	breakerThreshold := flags.Int("breaker-threshold", 5, "consecutive failures before the breaker opens")
	breakerBase := flags.Duration("breaker-base", time.Second, "first breaker open period")
	breakerCap := flags.Duration("breaker-cap", 30*time.Second, "longest breaker open period")
	_ = flags.Parse(arguments)

	database, err := store.Open(ctx, *databaseURL, int32(*maxConnections))
	if err != nil {
		return err
	}
	defer database.Close()

	engine, err := worker.New(worker.Config{
		WorkerID: *workerID, Concurrency: *concurrency,
		Lease: *lease, LeaseMargin: *leaseMargin, RequestTimeout: *requestTimeout,
		PollInterval: *pollInterval, ReapInterval: *reapInterval, ReapBatch: 500,
		Policy:  worker.Policy{MaxAttempts: *maxAttempts, MaxAge: *maxAge, RetryBase: *retryBase, RetryCap: *retryCap},
		Breaker: store.BreakerConfig{FailureThreshold: *breakerThreshold, BaseDelay: *breakerBase, MaxDelay: *breakerCap},
	}, database)
	if err != nil {
		return err
	}
	log.Printf("worker %s running with concurrency %d", *workerID, *concurrency)
	engine.Run(ctx)
	return nil
}
