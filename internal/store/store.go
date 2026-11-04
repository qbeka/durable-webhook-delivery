// Package store holds every SQL statement the engine runs.
package store

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schemaSQL string

// Store wraps the engine's connection pool.
type Store struct {
	pool *pgxpool.Pool
}

// Open connects to the engine database.
func Open(ctx context.Context, databaseURL string, maxConnections int32) (*Store, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database URL: %w", err)
	}
	config.MaxConns = maxConnections
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Close releases all connections.
func (s *Store) Close() {
	s.pool.Close()
}

// Ping reports whether the database answers.
func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// QueryRowForTest runs one query. Tests in other packages use it to read state.
func (s *Store) QueryRowForTest(ctx context.Context, query string, arguments ...any) pgx.Row {
	return s.pool.QueryRow(ctx, query, arguments...)
}

// Migrate creates any missing tables and indexes. It is safe to run repeatedly.
func (s *Store) Migrate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, schemaSQL); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	return nil
}
