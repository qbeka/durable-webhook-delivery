package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// DestinationStatus is one row of the operator status view.
type DestinationStatus struct {
	DestinationID       int64   `json:"destination_id"`
	Name                string  `json:"name"`
	Paused              bool    `json:"paused"`
	ConsecutiveFailures int     `json:"consecutive_failures"`
	BreakerOpenSeconds  float64 `json:"breaker_open_seconds"`
	Pending             int64   `json:"pending"`
	Leased              int64   `json:"leased"`
	Succeeded           int64   `json:"succeeded"`
	Dead                int64   `json:"dead"`
	OldestPendingSecond float64 `json:"oldest_pending_seconds"`
}

// Status returns delivery counts per destination.
func (s *Store) Status(ctx context.Context) ([]DestinationStatus, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT d.id, d.name, d.paused, d.consecutive_failures,
		       GREATEST(extract(epoch FROM d.not_before - now()), 0)::double precision,
		       count(*) FILTER (WHERE q.state = 'pending'),
		       count(*) FILTER (WHERE q.state = 'leased'),
		       count(*) FILTER (WHERE q.state = 'succeeded'),
		       count(*) FILTER (WHERE q.state = 'dead'),
		       COALESCE(extract(epoch FROM now() - min(q.created_at) FILTER (WHERE q.state = 'pending')), 0)::double precision
		FROM destinations d
		LEFT JOIN deliveries q ON q.destination_id = d.id
		GROUP BY d.id ORDER BY d.id`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (DestinationStatus, error) {
		var status DestinationStatus
		err := row.Scan(&status.DestinationID, &status.Name, &status.Paused, &status.ConsecutiveFailures,
			&status.BreakerOpenSeconds, &status.Pending, &status.Leased, &status.Succeeded, &status.Dead,
			&status.OldestPendingSecond)
		return status, err
	})
}

// QueueDepth is the cheap sample taken once a second during a run.
type QueueDepth struct {
	DestinationID int64 `json:"destination_id"`
	Pending       int64 `json:"pending"`
	Leased        int64 `json:"leased"`
}

// QueueDepths counts unfinished deliveries using the partial indexes.
func (s *Store) QueueDepths(ctx context.Context) ([]QueueDepth, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT destination_id,
		       count(*) FILTER (WHERE state = 'pending'),
		       count(*) FILTER (WHERE state = 'leased')
		FROM deliveries WHERE state IN ('pending', 'leased')
		GROUP BY destination_id`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[QueueDepth])
}

// AttemptRecord is one attempt in a delivery's history.
type AttemptRecord struct {
	AttemptNumber  int        `json:"attempt_number"`
	LeaseToken     string     `json:"lease_token"`
	WorkerID       string     `json:"worker_id"`
	ClaimedAt      time.Time  `json:"claimed_at"`
	FinishedAt     *time.Time `json:"finished_at,omitempty"`
	Outcome        *string    `json:"outcome,omitempty"`
	HTTPStatus     *int       `json:"http_status,omitempty"`
	Error          *string    `json:"error,omitempty"`
	Stale          bool       `json:"stale"`
	LeaseExpiredAt *time.Time `json:"lease_expired_at,omitempty"`
}

// DeliveryRecord is a delivery with its event identity and full attempt history.
type DeliveryRecord struct {
	DeliveryKey   string          `json:"delivery_key"`
	State         string          `json:"state"`
	AttemptCount  int             `json:"attempt_count"`
	Destination   string          `json:"destination"`
	TenantID      string          `json:"tenant_id"`
	SourceID      string          `json:"source_id"`
	EventID       string          `json:"event_id"`
	EventType     string          `json:"event_type"`
	PayloadSHA256 string          `json:"payload_sha256"`
	ReceivedAt    time.Time       `json:"received_at"`
	FinishedAt    *time.Time      `json:"finished_at,omitempty"`
	Attempts      []AttemptRecord `json:"attempts"`
}

// ErrUnknownDelivery means no delivery has that key.
var ErrUnknownDelivery = errors.New("unknown delivery key")

// DeliveryHistory returns one delivery with every attempt in order.
func (s *Store) DeliveryHistory(ctx context.Context, deliveryKey string) (DeliveryRecord, error) {
	var record DeliveryRecord
	var deliveryID int64
	err := s.pool.QueryRow(ctx, `
		SELECT q.id, q.delivery_key, q.state, q.attempt_count, d.name, e.tenant_id, e.source_id,
		       e.event_id, e.event_type, encode(e.payload_sha256, 'hex'), e.received_at, q.finished_at
		FROM deliveries q
		JOIN events e ON e.id = q.event_row_id
		JOIN destinations d ON d.id = q.destination_id
		WHERE q.delivery_key = $1`, deliveryKey,
	).Scan(&deliveryID, &record.DeliveryKey, &record.State, &record.AttemptCount, &record.Destination,
		&record.TenantID, &record.SourceID, &record.EventID, &record.EventType, &record.PayloadSHA256,
		&record.ReceivedAt, &record.FinishedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return DeliveryRecord{}, ErrUnknownDelivery
	}
	if err != nil {
		return DeliveryRecord{}, err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT attempt_number, lease_token::text, worker_id, claimed_at, finished_at, outcome,
		       http_status, error, stale, lease_expired_at
		FROM delivery_attempts WHERE delivery_id = $1 ORDER BY id`, deliveryID)
	if err != nil {
		return DeliveryRecord{}, err
	}
	record.Attempts, err = pgx.CollectRows(rows, pgx.RowToStructByPos[AttemptRecord])
	return record, err
}

// MaxReplayLimit is the most deliveries one replay can requeue.
const MaxReplayLimit = 10_000

// ReplayRequest selects finished deliveries to send again under the same keys.
// Exactly one of DeliveryKey or DestinationID must be set.
type ReplayRequest struct {
	Operator      string
	Reason        string
	DeliveryKey   string
	DestinationID int64
	State         string // StateDead or StateSucceeded, with DestinationID
	Limit         int
	DryRun        bool
}

// ReplayResult reports what a replay did or, in a dry run, would do.
type ReplayResult struct {
	Matched        int `json:"matched"`
	Requeued       int `json:"requeued"`
	PayloadExpired int `json:"payload_expired"` // finished, but the payload was pruned
}

func (r ReplayRequest) validate() error {
	switch {
	case r.Operator == "" || r.Reason == "":
		return errors.New("replay needs an operator and a reason")
	case r.Limit < 1 || r.Limit > MaxReplayLimit:
		return fmt.Errorf("replay limit must be between 1 and %d", MaxReplayLimit)
	case (r.DeliveryKey == "") == (r.DestinationID == 0):
		return errors.New("replay needs either one delivery key or one destination")
	case r.DestinationID != 0 && r.State != StateDead && r.State != StateSucceeded:
		return errors.New("replay by destination needs a state of dead or succeeded")
	}
	return nil
}

// Replay requeues finished deliveries with their existing delivery keys, so a
// compliant receiver answers duplicate and records nothing. It resets the
// attempt budget, keeps the history, and writes an audit row in the same
// transaction. Deliveries still in progress are never touched.
func (s *Store) Replay(ctx context.Context, request ReplayRequest) (ReplayResult, error) {
	if err := request.validate(); err != nil {
		return ReplayResult{}, err
	}
	selector := fmt.Sprintf("destination=%d state=%s", request.DestinationID, request.State)
	if request.DeliveryKey != "" {
		selector = "key=" + request.DeliveryKey
	}
	var result ReplayResult
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := takeOperatorLock(ctx, tx); err != nil {
			return err
		}
		const matchSQL = `
			SELECT d.id, e.payload IS NULL AS payload_gone
			FROM deliveries d JOIN events e ON e.id = d.event_row_id
			WHERE d.state IN ('succeeded', 'dead')
			  AND (($1 <> '' AND d.delivery_key = $1)
			    OR ($2 <> 0 AND d.destination_id = $2 AND d.state = $3))
			ORDER BY d.id LIMIT $4`
		err := tx.QueryRow(ctx, `
			WITH matched AS (`+matchSQL+`)
			SELECT count(*), count(*) FILTER (WHERE payload_gone) FROM matched`,
			request.DeliveryKey, request.DestinationID, request.State, request.Limit,
		).Scan(&result.Matched, &result.PayloadExpired)
		if err != nil {
			return fmt.Errorf("match deliveries: %w", err)
		}
		if request.DryRun {
			return nil
		}
		err = tx.QueryRow(ctx, `
			WITH matched AS (`+matchSQL+` FOR UPDATE OF d SKIP LOCKED),
			requeued AS (
				UPDATE deliveries d SET
					state = 'pending', budget_used = 0, next_attempt_at = clock_timestamp(),
					finished_at = NULL, last_error = NULL
				FROM matched WHERE d.id = matched.id AND NOT matched.payload_gone
				RETURNING 1
			)
			SELECT count(*) FROM requeued`,
			request.DeliveryKey, request.DestinationID, request.State, request.Limit,
		).Scan(&result.Requeued)
		if err != nil {
			return fmt.Errorf("requeue deliveries: %w", err)
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO replays (operator, reason, selector, row_limit, requeued)
			VALUES ($1, $2, $3, $4, $5)`,
			request.Operator, request.Reason, selector, request.Limit, result.Requeued)
		return err
	})
	return result, err
}

// PruneResult reports what retention removed.
type PruneResult struct {
	PayloadsCleared int64 `json:"payloads_cleared"`
	EventsDeleted   int64 `json:"events_deleted"`
}

// operatorLockID serializes Replay and Prune. Both are rare, and without this a
// replay could requeue a delivery whose payload a prune is clearing.
const operatorLockID = 727_001

func takeOperatorLock(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, operatorLockID)
	return err
}

// Prune applies retention. An event with an unfinished delivery is never
// touched, however old it is.
func (s *Store) Prune(ctx context.Context, payloadRetention, identityRetention time.Duration) (PruneResult, error) {
	var result PruneResult
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := takeOperatorLock(ctx, tx); err != nil {
			return err
		}
		const allFinished = `NOT EXISTS (SELECT 1 FROM deliveries d
			WHERE d.event_row_id = e.id AND d.state IN ('pending', 'leased'))`
		tag, err := tx.Exec(ctx, `
			UPDATE events e SET payload = NULL
			WHERE e.payload IS NOT NULL
			  AND e.received_at < clock_timestamp() - make_interval(secs => $1)
			  AND `+allFinished, payloadRetention.Seconds())
		if err != nil {
			return fmt.Errorf("clear payloads: %w", err)
		}
		result.PayloadsCleared = tag.RowsAffected()

		rows, err := tx.Query(ctx, `
			SELECT e.id FROM events e
			WHERE e.received_at < clock_timestamp() - make_interval(secs => $1)
			  AND `+allFinished, identityRetention.Seconds())
		if err != nil {
			return fmt.Errorf("find expired events: %w", err)
		}
		expired, err := pgx.CollectRows(rows, pgx.RowTo[int64])
		if err != nil {
			return fmt.Errorf("read expired events: %w", err)
		}
		for _, statement := range []string{
			`DELETE FROM delivery_attempts WHERE delivery_id IN
				(SELECT id FROM deliveries WHERE event_row_id = ANY($1))`,
			`DELETE FROM deliveries WHERE event_row_id = ANY($1)`,
			`DELETE FROM events WHERE id = ANY($1)`,
		} {
			if _, err := tx.Exec(ctx, statement, expired); err != nil {
				return fmt.Errorf("delete expired events: %w", err)
			}
		}
		result.EventsDeleted = int64(len(expired))
		return nil
	})
	if err != nil {
		return PruneResult{}, err
	}
	return result, nil
}
