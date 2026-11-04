package store

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// IngestStatus is the outcome of storing an event.
type IngestStatus string

const (
	IngestAccepted  IngestStatus = "accepted"
	IngestDuplicate IngestStatus = "duplicate"
	IngestConflict  IngestStatus = "conflict"
)

// ErrUnknownSource means the tenant and source pair is not registered.
var ErrUnknownSource = errors.New("unknown source")

// Event is one inbound event, identified by tenant, source and event ID.
type Event struct {
	TenantID      string
	SourceID      string
	EventID       string
	EventType     string
	Payload       []byte
	PayloadSHA256 []byte
}

// IngestResult reports what Ingest did.
type IngestResult struct {
	Status       IngestStatus
	Deliveries   int
	StoredSHA256 []byte
}

// SourceSecret returns the signing secret for a source.
func (s *Store) SourceSecret(ctx context.Context, tenantID, sourceID string) (string, error) {
	var secret string
	err := s.pool.QueryRow(ctx,
		`SELECT signing_secret FROM sources WHERE tenant_id = $1 AND source_id = $2`,
		tenantID, sourceID).Scan(&secret)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrUnknownSource
	}
	return secret, err
}

// Ingest commits an event and its deliveries in one transaction.
//
// Invariants:
//   - An event with its deliveries commits as one unit, or not at all.
//   - A repeated event with the same type and digest changes nothing.
//   - A repeated event with a different type or digest changes nothing except
//     the conflict record.
//   - Every event is ordered wholly before or after each subscription change.
func (s *Store) Ingest(ctx context.Context, event Event) (IngestResult, error) {
	var result IngestResult
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// The share lock orders this event against subscription changes, which
		// take the matching exclusive lock. It must be held before the fan-out
		// statement starts, because at READ COMMITTED that statement's snapshot
		// is taken when it begins.
		if err := lockSourceShared(ctx, tx, event.TenantID, event.SourceID); err != nil {
			return err
		}

		// ON CONFLICT DO NOTHING waits for a concurrent insert of the same key,
		// so two identical requests cannot both see themselves as first.
		var eventRowID *int64
		err := tx.QueryRow(ctx, `
			WITH new_event AS (
				INSERT INTO events (tenant_id, source_id, event_id, event_type, payload, payload_sha256)
				VALUES ($1, $2, $3, $4, $5, $6)
				ON CONFLICT (tenant_id, source_id, event_id) DO NOTHING
				RETURNING id
			), fan_out AS (
				INSERT INTO deliveries (event_row_id, destination_id, delivery_key)
				SELECT new_event.id, subscriptions.destination_id,
				       'dk_' || left(encode(sha256(convert_to(
				           $1 || E'\n' || $2 || E'\n' || $3 || E'\n' || subscriptions.destination_id::text,
				           'UTF8')), 'hex'), 32)
				FROM new_event
				JOIN (
					SELECT DISTINCT destination_id FROM subscriptions
					WHERE tenant_id = $1 AND source_id = $2 AND ended_at IS NULL
					  AND event_type IN ($4, '*')
				) AS subscriptions ON true
				RETURNING 1
			)
			SELECT (SELECT id FROM new_event), (SELECT count(*) FROM fan_out)`,
			event.TenantID, event.SourceID, event.EventID, event.EventType,
			event.Payload, event.PayloadSHA256,
		).Scan(&eventRowID, &result.Deliveries)
		if err != nil {
			return fmt.Errorf("insert event: %w", err)
		}
		if eventRowID != nil {
			result.Status = IngestAccepted
			result.StoredSHA256 = event.PayloadSHA256
			return nil
		}
		return compareWithStoredEvent(ctx, tx, event, &result)
	})
	if err != nil {
		return IngestResult{}, err
	}
	return result, nil
}

func lockSourceShared(ctx context.Context, tx pgx.Tx, tenantID, sourceID string) error {
	var found int
	err := tx.QueryRow(ctx,
		`SELECT 1 FROM sources WHERE tenant_id = $1 AND source_id = $2 FOR SHARE`,
		tenantID, sourceID).Scan(&found)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrUnknownSource
	}
	if err != nil {
		return fmt.Errorf("lock source: %w", err)
	}
	return nil
}

func compareWithStoredEvent(ctx context.Context, tx pgx.Tx, event Event, result *IngestResult) error {
	var storedType string
	err := tx.QueryRow(ctx,
		`SELECT event_type, payload_sha256 FROM events
		 WHERE tenant_id = $1 AND source_id = $2 AND event_id = $3`,
		event.TenantID, event.SourceID, event.EventID,
	).Scan(&storedType, &result.StoredSHA256)
	if err != nil {
		return fmt.Errorf("read stored event: %w", err)
	}
	if storedType == event.EventType && bytes.Equal(result.StoredSHA256, event.PayloadSHA256) {
		result.Status = IngestDuplicate
		return nil
	}
	result.Status = IngestConflict
	_, err = tx.Exec(ctx, `
		INSERT INTO event_conflicts (tenant_id, source_id, event_id, event_type, received_sha256, stored_sha256)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (tenant_id, source_id, event_id, event_type, received_sha256)
		DO UPDATE SET seen_count = event_conflicts.seen_count + 1, last_seen_at = clock_timestamp()`,
		event.TenantID, event.SourceID, event.EventID, event.EventType,
		event.PayloadSHA256, result.StoredSHA256)
	if err != nil {
		return fmt.Errorf("record conflict %s: %w", hex.EncodeToString(event.PayloadSHA256), err)
	}
	return nil
}
