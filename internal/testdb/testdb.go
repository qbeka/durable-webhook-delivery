// Package testdb gives each test its own empty database on the local clusters.
//
// Tests fail, rather than skip, when the clusters are down. A correctness suite
// that silently skips is worse than one that stops you. Run `make up` first.
package testdb

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	defaultEngineAdminURL   = "postgres://hook@127.0.0.1:54330/postgres"
	defaultReceiverAdminURL = "postgres://hook@127.0.0.1:54331/postgres"
)

// Engine creates a database on the engine cluster and returns its URL.
func Engine(t *testing.T) string {
	t.Helper()
	return create(t, envOr("HOOK_TEST_ENGINE_ADMIN_URL", defaultEngineAdminURL))
}

// Receiver creates a database on the receiver cluster and returns its URL.
func Receiver(t *testing.T) string {
	t.Helper()
	return create(t, envOr("HOOK_TEST_RECEIVER_ADMIN_URL", defaultReceiverAdminURL))
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func create(t *testing.T, adminURL string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		t.Fatalf("cannot reach the local database, run `make up` first: %v", err)
	}
	defer admin.Close(ctx)

	name := fmt.Sprintf("test_%d_%08x", os.Getpid(), rand.Uint32())
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create test database: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cleanup, err := pgx.Connect(ctx, adminURL)
		if err != nil {
			t.Errorf("drop test database: %v", err)
			return
		}
		defer cleanup.Close(ctx)
		if _, err := cleanup.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)"); err != nil {
			t.Errorf("drop test database: %v", err)
		}
	})

	parsed, err := url.Parse(adminURL)
	if err != nil {
		t.Fatal(err)
	}
	parsed.Path = "/" + name
	return parsed.String()
}
