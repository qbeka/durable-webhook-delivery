// Command receiver runs the reference receipt receiver.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/qbeka/durable-webhook-delivery/internal/receiver"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9101", "delivery listen address")
	adminAddr := flag.String("admin-addr", "127.0.0.1:9201", "fault control listen address")
	databaseURL := flag.String("db", "postgres://hook@127.0.0.1:54331/receipts", "receiver database URL")
	secret := flag.String("secret", "whsec_local_demo", "destination signing secret")
	mode := flag.String("mode", "idempotent", "idempotent or naive")
	effectLogPath := flag.String("effect-log", "", "CSV path for per-request rows, empty to disable")
	maxConnections := flag.Int("max-connections", 64, "database connections")
	flag.Parse()

	if *mode != string(receiver.ModeIdempotent) && *mode != string(receiver.ModeNaive) {
		log.Fatalf("unknown mode %q", *mode)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := receiver.Connect(ctx, *databaseURL, int32(*maxConnections))
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	faults := &receiver.Faults{}
	handler := &receiver.Receiver{Mode: receiver.Mode(*mode), Secret: *secret, Pool: pool, Faults: faults}
	if *effectLogPath != "" {
		effectLog, err := receiver.OpenEffectLog(*effectLogPath)
		if err != nil {
			log.Fatal(err)
		}
		defer effectLog.Close()
		handler.Log = effectLog
	}

	server := &http.Server{Addr: *addr, Handler: handler.Routes()}
	admin := &http.Server{Addr: *adminAddr, Handler: faults.AdminRoutes()}
	go func() {
		if err := admin.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = admin.Shutdown(shutdownCtx)
		_ = server.Shutdown(shutdownCtx)
	}()
	log.Printf("receiver mode=%s listening on %s", *mode, *addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
