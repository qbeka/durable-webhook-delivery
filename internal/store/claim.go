package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
)

// BreakerConfig controls when repeated failures stop attempts to a destination.
type BreakerConfig struct {
	FailureThreshold int           // consecutive failures before the breaker opens
	BaseDelay        time.Duration // first open period
	MaxDelay         time.Duration // longest open period
}

// Job is one claimed delivery, with everything needed to send it.
type Job struct {
	DeliveryID    int64
	DeliveryKey   string
	LeaseToken    string
	Attempt       int // across the life of the delivery
	BudgetUsed    int // since creation or the last replay
	BudgetAge     time.Duration
	DestinationID int64
	URL           string
	Secret        string
	TenantID      string
	SourceID      string
	EventID       string
	EventType     string
	Payload       []byte
	PayloadSHA256 []byte
}

// claimedDestination is the state read while holding the destination row lock.
type claimedDestination struct {
	id                  int64
	maxInFlight         int
	ratePerSecond       float64
	rateTokens          float64
	secondsSinceRefill  float64
	consecutiveFailures int
}

// Claim leases up to maxJobs due deliveries from one destination and commits
// the leases, so the caller holds them before any request is sent.
//
// It returns no jobs when no destination has work it can take right now. Calling
// it again moves to the next destination in round-robin order. One destination
// per claim keeps the transaction short, and last_claimed_at makes a large
// backlog on one destination wait its turn behind the others.
func (s *Store) Claim(ctx context.Context, workerID string, maxJobs int, leaseDuration time.Duration, breaker BreakerConfig) ([]Job, error) {
	var jobs []Job
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		destination, found, err := lockNextDestination(ctx, tx)
		if err != nil || !found {
			return err
		}

		// A destination recovering from an open breaker gets one probe at a time.
		// A probe tests the destination, not the delivery, so it spends no attempt.
		// Otherwise a long outage would use up the budget of whichever deliveries
		// were picked as probes and send healthy work to dead letters. The age
		// limit still ends a delivery that only ever gets probes.
		maxInFlight, attemptCost := destination.maxInFlight, 1
		if destination.consecutiveFailures >= breaker.FailureThreshold {
			maxInFlight, attemptCost = 1, 0
		}

		budget, tokensLeft, waitSeconds := applyRateLimit(destination, maxJobs)
		if budget > 0 {
			jobs, err = claimDeliveries(ctx, tx, destination.id, workerID, budget, maxInFlight, attemptCost, leaseDuration)
			if err != nil {
				return err
			}
			if destination.ratePerSecond > 0 {
				tokensLeft -= float64(len(jobs))
			}
		}
		_, err = tx.Exec(ctx, `
			UPDATE destinations SET
				last_claimed_at = clock_timestamp(),
				rate_tokens = $2,
				rate_tokens_at = now(),
				not_before = GREATEST(not_before, now() + make_interval(secs => $3))
			WHERE id = $1`,
			destination.id, tokensLeft, waitSeconds)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("claim: %w", err)
	}
	return jobs, nil
}

// lockNextDestination locks the least recently claimed destination with work
// and spare capacity.
//
// FOR NO KEY UPDATE, not FOR UPDATE: fan-out inserts take FOR KEY SHARE on this
// row through their foreign key, and FOR UPDATE would block them. Ingress must
// not wait for a worker.
//
// SKIP LOCKED means two workers never serialize on one destination: the second
// moves on to other work. The capacity check here reads a snapshot from before
// the lock, so it only filters. claimDeliveries makes the real check.
func lockNextDestination(ctx context.Context, tx pgx.Tx) (claimedDestination, bool, error) {
	var destination claimedDestination
	err := tx.QueryRow(ctx, `
		SELECT d.id, d.max_in_flight, d.rate_per_second, d.rate_tokens,
		       extract(epoch FROM now() - d.rate_tokens_at)::double precision,
		       d.consecutive_failures
		FROM destinations d
		WHERE NOT d.paused
		  AND d.not_before <= now()
		  AND EXISTS (
		      SELECT 1 FROM deliveries p
		      WHERE p.destination_id = d.id AND p.state = 'pending' AND p.next_attempt_at <= now())
		  AND (SELECT count(*) FROM deliveries l
		       WHERE l.destination_id = d.id AND l.state = 'leased') < d.max_in_flight
		ORDER BY d.last_claimed_at
		LIMIT 1
		FOR NO KEY UPDATE OF d SKIP LOCKED`,
	).Scan(&destination.id, &destination.maxInFlight, &destination.ratePerSecond,
		&destination.rateTokens, &destination.secondsSinceRefill, &destination.consecutiveFailures)
	if errors.Is(err, pgx.ErrNoRows) {
		return claimedDestination{}, false, nil
	}
	if err != nil {
		return claimedDestination{}, false, fmt.Errorf("lock destination: %w", err)
	}
	return destination, true, nil
}

// applyRateLimit refills a token bucket that holds one second of traffic, and
// limits this claim to the whole tokens available. When the bucket is empty it
// returns how long to wait for the next token. With no rate limit it returns
// maxJobs and zero tokens.
func applyRateLimit(destination claimedDestination, maxJobs int) (budget int, tokens, waitSeconds float64) {
	if destination.ratePerSecond == 0 {
		return maxJobs, 0, 0
	}
	bucketSize := math.Max(1, destination.ratePerSecond)
	tokens = math.Min(bucketSize, destination.rateTokens+destination.ratePerSecond*destination.secondsSinceRefill)
	budget = min(maxJobs, int(tokens))
	if budget == 0 {
		waitSeconds = (1 - tokens) / destination.ratePerSecond
	}
	return budget, tokens, waitSeconds
}

// claimDeliveries takes the due rows of a locked destination.
//
// This is a new statement, so at READ COMMITTED it gets a new snapshot taken
// after the destination lock was granted. Its count therefore includes every
// claim that committed before this one, and in-flight never passes the limit.
//
// In-flight is counted from leased rows, never stored as a counter. A worker
// that dies cannot leave a counter too high: its rows expire and the count
// follows.
func claimDeliveries(ctx context.Context, tx pgx.Tx, destinationID int64, workerID string, budget, maxInFlight, attemptCost int, leaseDuration time.Duration) ([]Job, error) {
	rows, err := tx.Query(ctx, `
		WITH room AS (
			SELECT GREATEST($4::int - count(*), 0) AS free
			FROM deliveries WHERE destination_id = $1 AND state = 'leased'
		), picked AS (
			SELECT p.id FROM deliveries p
			WHERE p.destination_id = $1 AND p.state = 'pending' AND p.next_attempt_at <= now()
			ORDER BY p.next_attempt_at
			LIMIT LEAST($3::int, (SELECT free FROM room))
			FOR UPDATE SKIP LOCKED
		), claimed AS (
			UPDATE deliveries d SET
				state = 'leased',
				lease_token = gen_random_uuid(),
				lease_owner = $2,
				lease_expires_at = clock_timestamp() + make_interval(secs => $5),
				attempt_count = d.attempt_count + 1,
				budget_used = d.budget_used + $6
			FROM picked WHERE d.id = picked.id
			RETURNING d.id, d.delivery_key, d.lease_token, d.attempt_count, d.budget_used, d.event_row_id,
			          extract(epoch FROM clock_timestamp() - d.budget_started_at)::double precision AS budget_age_seconds
		), history AS (
			INSERT INTO delivery_attempts (delivery_id, attempt_number, lease_token, worker_id)
			SELECT id, attempt_count, lease_token, $2 FROM claimed
		)
		SELECT c.id, c.delivery_key, c.lease_token::text, c.attempt_count, c.budget_used, c.budget_age_seconds,
		       $1::bigint, dest.url, dest.signing_secret,
		       e.tenant_id, e.source_id, e.event_id, e.event_type, e.payload, e.payload_sha256
		FROM claimed c
		JOIN events e ON e.id = c.event_row_id
		JOIN destinations dest ON dest.id = $1`,
		destinationID, workerID, budget, maxInFlight, leaseDuration.Seconds(), attemptCost)
	if err != nil {
		return nil, fmt.Errorf("claim deliveries: %w", err)
	}
	jobs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Job, error) {
		var job Job
		var budgetAgeSeconds float64
		err := row.Scan(&job.DeliveryID, &job.DeliveryKey, &job.LeaseToken, &job.Attempt, &job.BudgetUsed, &budgetAgeSeconds,
			&job.DestinationID, &job.URL, &job.Secret,
			&job.TenantID, &job.SourceID, &job.EventID, &job.EventType, &job.Payload, &job.PayloadSHA256)
		job.BudgetAge = time.Duration(budgetAgeSeconds * float64(time.Second))
		return job, err
	})
	if err != nil {
		return nil, fmt.Errorf("read claimed deliveries: %w", err)
	}
	return jobs, nil
}
