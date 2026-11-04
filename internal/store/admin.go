package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// PutSource registers a source or replaces its secret.
func (s *Store) PutSource(ctx context.Context, tenantID, sourceID, secret string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO sources (tenant_id, source_id, signing_secret) VALUES ($1, $2, $3)
		ON CONFLICT (tenant_id, source_id) DO UPDATE SET signing_secret = EXCLUDED.signing_secret`,
		tenantID, sourceID, secret)
	return err
}

// Destination is a registered receiver endpoint.
type Destination struct {
	ID            int64
	TenantID      string
	Name          string
	URL           string
	Secret        string
	MaxInFlight   int
	RatePerSecond float64
}

// PutDestination registers a destination or updates it, and returns its ID.
func (s *Store) PutDestination(ctx context.Context, destination Destination) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx, `
		INSERT INTO destinations (tenant_id, name, url, signing_secret, max_in_flight, rate_per_second)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (tenant_id, name) DO UPDATE SET
			url = EXCLUDED.url, signing_secret = EXCLUDED.signing_secret,
			max_in_flight = EXCLUDED.max_in_flight, rate_per_second = EXCLUDED.rate_per_second
		RETURNING id`,
		destination.TenantID, destination.Name, destination.URL, destination.Secret,
		destination.MaxInFlight, destination.RatePerSecond).Scan(&id)
	return id, err
}

// SetDestinationPaused pauses or resumes a destination. Paused deliveries wait.
func (s *Store) SetDestinationPaused(ctx context.Context, destinationID int64, paused bool) error {
	_, err := s.pool.Exec(ctx, `UPDATE destinations SET paused = $2 WHERE id = $1`, destinationID, paused)
	return err
}

// Subscribe routes an event type from a source to a destination. It returns
// after commit, so any event accepted later fans out to this destination.
func (s *Store) Subscribe(ctx context.Context, tenantID, sourceID, eventType string, destinationID int64) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := lockSourceExclusive(ctx, tx, tenantID, sourceID); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO subscriptions (tenant_id, source_id, event_type, destination_id)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (tenant_id, source_id, event_type, destination_id)
				WHERE ended_at IS NULL DO NOTHING`,
			tenantID, sourceID, eventType, destinationID)
		return err
	})
}

// Unsubscribe stops future fan-out. Deliveries that already exist still run.
func (s *Store) Unsubscribe(ctx context.Context, tenantID, sourceID, eventType string, destinationID int64) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := lockSourceExclusive(ctx, tx, tenantID, sourceID); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `
			UPDATE subscriptions SET ended_at = clock_timestamp()
			WHERE tenant_id = $1 AND source_id = $2 AND event_type = $3
			  AND destination_id = $4 AND ended_at IS NULL`,
			tenantID, sourceID, eventType, destinationID)
		return err
	})
}

// lockSourceExclusive waits for every in-progress ingress on this source and
// blocks new ones until commit. See the note in Ingest.
func lockSourceExclusive(ctx context.Context, tx pgx.Tx, tenantID, sourceID string) error {
	var found int
	err := tx.QueryRow(ctx,
		`SELECT 1 FROM sources WHERE tenant_id = $1 AND source_id = $2 FOR UPDATE`,
		tenantID, sourceID).Scan(&found)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrUnknownSource
	}
	if err != nil {
		return fmt.Errorf("lock source: %w", err)
	}
	return nil
}
