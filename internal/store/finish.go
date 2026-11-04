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

// Completion pairs a claimed job with what happened to it.
type Completion struct {
	Job    Job
	Report Finish
}

// FinishAttempt applies one report. See FinishAttempts.
func (s *Store) FinishAttempt(ctx context.Context, job Job, report Finish, breaker BreakerConfig) (bool, error) {
	applied, err := s.FinishAttempts(ctx, []Completion{{job, report}}, breaker)
	return applied == 1, err
}

// FinishAttempts applies reports in one commit, and returns how many still held
// their lease. The attempt history is updated either way, and a stale report
// changes neither the delivery nor the breaker.
//
// One commit for many reports matters because a durable commit is the scarcest
// resource here: see docs/experiments.md.
//
// Breaker rule within a batch: the order of reports inside one commit is unknown,
// so a success resets the count, and the batch's failures then start it again.
// A batch with no success adds to the existing count.
func (s *Store) FinishAttempts(ctx context.Context, completions []Completion, breaker BreakerConfig) (int, error) {
	count := len(completions)
	ids := make([]int64, count)
	tokens := make([]string, count)
	states := make([]string, count)
	retrySeconds := make([]float64, count)
	errorTexts := make([]string, count)
	outcomes := make([]string, count)
	statuses := make([]int32, count)
	countsFailure := make([]bool, count)
	retryAfterSeconds := make([]float64, count)
	for index, completion := range completions {
		ids[index] = completion.Job.DeliveryID
		tokens[index] = completion.Job.LeaseToken
		states[index] = completion.Report.NextState
		retrySeconds[index] = completion.Report.RetryDelay.Seconds()
		errorTexts[index] = completion.Report.Error
		outcomes[index] = completion.Report.Outcome
		statuses[index] = int32(completion.Report.HTTPStatus)
		countsFailure[index] = completion.Report.CountsFailure
		retryAfterSeconds[index] = completion.Report.RetryAfter.Seconds()
	}

	var applied int
	err := s.pool.QueryRow(ctx, `
		WITH reports AS (
			SELECT * FROM unnest($1::bigint[], $2::uuid[], $3::text[], $4::double precision[], $5::text[],
			                     $6::text[], $7::int[], $8::boolean[], $9::double precision[])
				AS r (id, token, next_state, retry_seconds, error_text, outcome, http_status, counts_failure, retry_after_seconds)
		), finished AS (
			UPDATE deliveries d SET
				state = r.next_state,
				next_attempt_at = CASE WHEN r.next_state = 'pending'
				                       THEN clock_timestamp() + make_interval(secs => r.retry_seconds)
				                       ELSE d.next_attempt_at END,
				finished_at = CASE WHEN r.next_state = 'pending' THEN NULL ELSE clock_timestamp() END,
				last_error = NULLIF(r.error_text, ''),
				lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL
			FROM reports r
			WHERE d.id = r.id AND d.lease_token = r.token AND d.state = 'leased'
			RETURNING d.id, d.destination_id
		), history AS (
			UPDATE delivery_attempts a SET
				finished_at = clock_timestamp(),
				outcome = r.outcome,
				http_status = NULLIF(r.http_status, 0),
				error = NULLIF(r.error_text, ''),
				stale = NOT EXISTS (SELECT 1 FROM finished f WHERE f.id = r.id)
			FROM reports r
			-- A report already recorded stays as it is. If the commit's answer was
			-- lost, the worker sends the same report again, and that second copy
			-- must not relabel an applied attempt as stale.
			WHERE a.delivery_id = r.id AND a.lease_token = r.token
			  AND (a.outcome IS NULL OR a.outcome = 'lease_expired')
		), per_destination AS (
			SELECT f.destination_id,
			       count(*) FILTER (WHERE r.counts_failure) AS failures,
			       bool_or(r.next_state = 'succeeded') AS any_success,
			       max(r.retry_after_seconds) AS retry_after_seconds
			FROM finished f JOIN reports r ON r.id = f.id
			GROUP BY f.destination_id
		), counted AS (
			SELECT p.*, CASE WHEN p.any_success THEN p.failures
			                 ELSE d.consecutive_failures + p.failures END AS new_failures
			FROM per_destination p JOIN destinations d ON d.id = p.destination_id
		), breaker AS (
			UPDATE destinations d SET
				consecutive_failures = c.new_failures,
				not_before = GREATEST(
					d.not_before,
					clock_timestamp() + make_interval(secs => c.retry_after_seconds),
					CASE WHEN c.failures > 0 AND c.new_failures >= $10::int
					     THEN clock_timestamp() + make_interval(secs => LEAST(
					         $12::double precision,
					         $11::double precision * power(2, LEAST(c.new_failures - $10::int, 20))))
					     ELSE '-infinity'::timestamptz END)
			FROM counted c
			-- The condition keeps a healthy destination's row from being rewritten
			-- on every batch, which would contend with claims.
			WHERE d.id = c.destination_id AND d.consecutive_failures IS DISTINCT FROM c.new_failures
		)
		SELECT count(*) FROM finished`,
		ids, tokens, states, retrySeconds, errorTexts, outcomes, statuses, countsFailure, retryAfterSeconds,
		breaker.FailureThreshold, breaker.BaseDelay.Seconds(), breaker.MaxDelay.Seconds(),
	).Scan(&applied)
	if err != nil {
		return 0, fmt.Errorf("finish attempts: %w", err)
	}
	return applied, nil
}

// ExpireLeases returns deliveries with expired leases to the queue, or moves them
// to dead when their attempts or age limit run out. The attempt counts, so a
// delivery that keeps killing workers still ends.
//
// The retry delay uses full jitter, computed per row in SQL. Without jitter,
// everything held by one dead worker would retry at the same instant.
func (s *Store) ExpireLeases(ctx context.Context, maxRows, maxAttempts int, maxAge, retryBase, retryCap time.Duration) (int, error) {
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
				state = CASE WHEN d.budget_used >= $2 OR clock_timestamp() - d.budget_started_at >= make_interval(secs => $5)
				             THEN 'dead' ELSE 'pending' END,
				finished_at = CASE WHEN d.budget_used >= $2 OR clock_timestamp() - d.budget_started_at >= make_interval(secs => $5)
				                   THEN clock_timestamp() END,
				next_attempt_at = clock_timestamp() + make_interval(secs =>
					random() * LEAST($4::double precision,
					                 $3::double precision * power(2, LEAST(GREATEST(d.budget_used, 1) - 1, 30)))),
				last_error = 'lease expired',
				lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL
			FROM overdue WHERE d.id = overdue.id
			RETURNING 1
		)
		SELECT count(*) FROM released`,
		maxRows, maxAttempts, retryBase.Seconds(), retryCap.Seconds(), maxAge.Seconds()).Scan(&expired)
	if err != nil {
		return 0, fmt.Errorf("expire leases: %w", err)
	}
	return expired, nil
}
