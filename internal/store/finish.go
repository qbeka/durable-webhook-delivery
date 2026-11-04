package store

import (
	"context"
	"fmt"
	"time"
)

// Next states a worker can move a delivery to.
const (
	StatePending   = "pending"
	StateLeased    = "leased"
	StateSucceeded = "succeeded"
	StateDead      = "dead"
)

// Finish is what a worker reports about one attempt.
type Finish struct {
	NextState     string        // StateSucceeded, StatePending or StateDead
	Outcome       string        // short label stored in the attempt history
	HTTPStatus    int           // 0 when no response arrived
	Error         string        // empty on success
	RetryDelay    time.Duration // used when NextState is StatePending
	RetryAfter    time.Duration // the receiver's Retry-After, zero if none
	CountsFailure bool          // true when this attempt should feed the breaker
}

// FinishAttempt applies a worker's report if its lease token is still current.
// It returns false when the lease was lost. The attempt history is updated
// either way, and a stale report changes neither the delivery nor the breaker.
func (s *Store) FinishAttempt(ctx context.Context, job Job, report Finish, breaker BreakerConfig) (bool, error) {
	var applied bool
	err := s.pool.QueryRow(ctx, `
		WITH finished AS (
			UPDATE deliveries SET
				state = $3,
				next_attempt_at = CASE WHEN $3 = 'pending' THEN clock_timestamp() + make_interval(secs => $4)
				                       ELSE next_attempt_at END,
				finished_at = CASE WHEN $3 = 'pending' THEN NULL ELSE clock_timestamp() END,
				last_error = NULLIF($5, ''),
				lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL
			WHERE id = $1 AND lease_token = $2::uuid AND state = 'leased'
			RETURNING destination_id
		), history AS (
			UPDATE delivery_attempts SET
				finished_at = clock_timestamp(),
				outcome = $6,
				http_status = NULLIF($7::int, 0),
				error = NULLIF($5, ''),
				stale = NOT EXISTS (SELECT 1 FROM finished)
			WHERE delivery_id = $1 AND lease_token = $2::uuid
		), breaker_failure AS (
			UPDATE destinations d SET
				consecutive_failures = d.consecutive_failures + 1,
				not_before = GREATEST(
					d.not_before,
					clock_timestamp() + make_interval(secs => $9),
					CASE WHEN d.consecutive_failures + 1 >= $10::int
					     THEN clock_timestamp() + make_interval(secs => LEAST(
					         $12::double precision,
					         $11::double precision * power(2, LEAST(d.consecutive_failures + 1 - $10::int, 20))))
					     ELSE '-infinity'::timestamptz END)
			FROM finished WHERE d.id = finished.destination_id AND $8::boolean
		), breaker_success AS (
			-- The condition keeps a healthy destination's row from being rewritten
			-- on every delivery, which would contend with claims.
			UPDATE destinations d SET consecutive_failures = 0
			FROM finished
			WHERE d.id = finished.destination_id AND $3 = 'succeeded' AND d.consecutive_failures > 0
		)
		SELECT EXISTS (SELECT 1 FROM finished)`,
		job.DeliveryID, job.LeaseToken, report.NextState, report.RetryDelay.Seconds(),
		report.Error, report.Outcome, report.HTTPStatus, report.CountsFailure,
		report.RetryAfter.Seconds(), breaker.FailureThreshold,
		breaker.BaseDelay.Seconds(), breaker.MaxDelay.Seconds(),
	).Scan(&applied)
	if err != nil {
		return false, fmt.Errorf("finish attempt: %w", err)
	}
	return applied, nil
}

// ExpireLeases returns deliveries with expired leases to the queue, or moves them
// to dead when their attempt budget is used up. The attempt counts, so a delivery
// that keeps killing workers still ends.
//
// The retry delay uses full jitter, computed per row in SQL. Without jitter,
// everything held by one dead worker would retry at the same instant.
func (s *Store) ExpireLeases(ctx context.Context, maxRows, maxAttempts int, retryBase, retryCap time.Duration) (int, error) {
	var expired int
	err := s.pool.QueryRow(ctx, `
		WITH overdue AS (
			SELECT id FROM deliveries
			WHERE state = 'leased' AND lease_expires_at <= clock_timestamp()
			ORDER BY lease_expires_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		), attempts AS (
			UPDATE delivery_attempts a SET
				lease_expired_at = clock_timestamp(),
				outcome = COALESCE(a.outcome, 'lease_expired'),
				finished_at = COALESCE(a.finished_at, clock_timestamp())
			FROM deliveries d, overdue
			WHERE d.id = overdue.id AND a.delivery_id = d.id AND a.lease_token = d.lease_token
		), released AS (
			UPDATE deliveries d SET
				state = CASE WHEN d.budget_used >= $2 THEN 'dead' ELSE 'pending' END,
				finished_at = CASE WHEN d.budget_used >= $2 THEN clock_timestamp() END,
				next_attempt_at = clock_timestamp() + make_interval(secs =>
					random() * LEAST($4::double precision,
					                 $3::double precision * power(2, LEAST(d.budget_used - 1, 30)))),
				last_error = 'lease expired',
				lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL
			FROM overdue WHERE d.id = overdue.id
			RETURNING 1
		)
		SELECT count(*) FROM released`,
		maxRows, maxAttempts, retryBase.Seconds(), retryCap.Seconds()).Scan(&expired)
	if err != nil {
		return 0, fmt.Errorf("expire leases: %w", err)
	}
	return expired, nil
}
