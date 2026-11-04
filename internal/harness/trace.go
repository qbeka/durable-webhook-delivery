package harness

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/qbeka/durable-webhook-delivery/internal/store"
)

// TraceStep is one moment in a delivery's life.
type TraceStep struct {
	At     time.Time `json:"at"`
	Actor  string    `json:"actor"`  // source, engine, worker or receiver
	Action string    `json:"action"` // stable label, used by the website replay
	Detail string    `json:"detail"`
}

// Trace is one delivery across both databases and the receiver's log.
type Trace struct {
	Scenario     string               `json:"scenario"`
	Delivery     store.DeliveryRecord `json:"delivery"`
	LedgerRows   int                  `json:"ledger_rows"`
	LedgerAmount int64                `json:"ledger_amount_cents"`
	Steps        []TraceStep          `json:"steps"`
}

// findAmbiguousKey returns a delivery key that the receiver applied once and
// later answered as a duplicate: the signature of a lost acknowledgement.
func findAmbiguousKey(rows []EffectRow) (string, bool) {
	applied := map[string]bool{}
	for _, row := range rows {
		switch row.Outcome {
		case "applied":
			applied[row.DeliveryKey] = true
		case "duplicate":
			if applied[row.DeliveryKey] {
				return row.DeliveryKey, true
			}
		}
	}
	return "", false
}

// BuildTrace merges the engine's history, the receiver's log and the ledger.
func BuildTrace(ctx context.Context, engineURL, scenario, deliveryKey string, rows []EffectRow) (Trace, error) {
	database, err := store.Open(ctx, engineURL, 2)
	if err != nil {
		return Trace{}, err
	}
	defer database.Close()
	record, err := database.DeliveryHistory(ctx, deliveryKey)
	if err != nil {
		return Trace{}, err
	}

	ledger, err := pgx.Connect(ctx, ReceiverURL)
	if err != nil {
		return Trace{}, err
	}
	defer ledger.Close(ctx)
	trace := Trace{Scenario: scenario, Delivery: record}
	err = ledger.QueryRow(ctx, `
		SELECT count(*), COALESCE(sum(amount_cents), 0) FROM expense_ledger WHERE delivery_key = $1`,
		deliveryKey).Scan(&trace.LedgerRows, &trace.LedgerAmount)
	if err != nil {
		return Trace{}, err
	}

	add := func(at time.Time, actor, action, detail string) {
		trace.Steps = append(trace.Steps, TraceStep{at, actor, action, detail})
	}
	add(record.ReceivedAt, "engine", "event_committed",
		fmt.Sprintf("event %s and its delivery committed, then ingress answered 202", record.EventID))
	for _, attempt := range record.Attempts {
		add(attempt.ClaimedAt, "worker", "claim_committed",
			fmt.Sprintf("%s committed lease %s for attempt %d before sending", attempt.WorkerID, attempt.LeaseToken[:8], attempt.AttemptNumber))
		if attempt.LeaseExpiredAt != nil {
			add(*attempt.LeaseExpiredAt, "engine", "lease_expired",
				fmt.Sprintf("lease %s expired with no report from %s", attempt.LeaseToken[:8], attempt.WorkerID))
		}
		if attempt.FinishedAt != nil && attempt.Outcome != nil && *attempt.Outcome != "lease_expired" {
			detail := fmt.Sprintf("attempt %d recorded as %s", attempt.AttemptNumber, *attempt.Outcome)
			if attempt.HTTPStatus != nil {
				detail += fmt.Sprintf(" with status %d", *attempt.HTTPStatus)
			}
			if attempt.Error != nil {
				detail += ": " + *attempt.Error
			}
			if attempt.Stale {
				detail += " (stale, the delivery did not change)"
			}
			add(*attempt.FinishedAt, "worker", "attempt_recorded", detail)
		}
	}
	for _, row := range rows {
		if row.DeliveryKey != deliveryKey {
			continue
		}
		at := time.Unix(0, row.TimeUnixNs)
		switch row.Outcome {
		case "applied":
			add(at, "receiver", "effect_committed", fmt.Sprintf("attempt %d: key and expense committed together", row.Attempt))
		case "duplicate":
			add(at, "receiver", "duplicate_detected", fmt.Sprintf("attempt %d: key already present, no new effect", row.Attempt))
		default:
			add(at, "receiver", "request_refused", fmt.Sprintf("attempt %d: %s", row.Attempt, row.Outcome))
		}
	}
	slices.SortStableFunc(trace.Steps, func(a, b TraceStep) int { return a.At.Compare(b.At) })
	return trace, nil
}
