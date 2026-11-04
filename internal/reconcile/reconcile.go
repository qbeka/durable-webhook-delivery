// Package reconcile compares what happened with what should have happened.
//
// The expected ledger is rebuilt from the workload seed. Nothing written by the
// engine or the receiver decides what was expected, so a bug in either cannot
// hide itself.
package reconcile

import (
	"context"
	"crypto/sha256"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/qbeka/durable-webhook-delivery/internal/workload"
)

// Expectation is one stream and what should result from it.
type Expectation struct {
	Stream workload.Stream
	// UnackedIndexes lists events the generator never saw acknowledged. They may
	// or may not exist: the commit can happen and the response can be lost. Every
	// other event must exist.
	UnackedIndexes []int
	// ExpectEffects is false for a destination that was never meant to receive
	// this stream.
	ExpectEffects bool
}

// Report counts each way a run can be wrong. Every count except the volume
// fields must be zero for the run to pass. Denominators are in ExpectedEvents
// and ExpectedEffects.
type Report struct {
	ExpectedEvents    int `json:"expected_events"`
	ExpectedEffects   int `json:"expected_effects"`
	OptionalEvents    int `json:"optional_events"` // sent but never acknowledged
	StoredEvents      int `json:"stored_events"`
	LedgerRows        int `json:"ledger_rows"`
	DistinctEffects   int `json:"distinct_effects"`
	AttemptsTotal     int `json:"attempts_total"`
	StaleAttempts     int `json:"stale_attempts"`
	ExpiredLeases     int `json:"expired_leases"`
	RecordedConflicts int `json:"recorded_conflicts"`

	MissingEvents     int `json:"missing_events"`
	UnexpectedEvents  int `json:"unexpected_events"`
	DigestMismatches  int `json:"digest_mismatches"`
	MissingEffects    int `json:"missing_effects"`
	RepeatedEffects   int `json:"repeated_effects"` // ledger rows beyond the first for one event
	UnexpectedEffects int `json:"unexpected_effects"`
	AmountMismatches  int `json:"amount_mismatches"`
	DeadLetters       int `json:"dead_letters"`
	UnfinishedJobs    int `json:"unfinished_jobs"`

	Pass bool `json:"pass"`
}

type expectedEvent struct {
	digest        [sha256.Size]byte
	amountCents   int64
	receiptID     string
	optional      bool
	expectsEffect bool
}

type eventKey struct {
	tenantID string
	sourceID string
	eventID  string
}

func buildExpected(expectations []Expectation) map[eventKey]expectedEvent {
	expected := make(map[eventKey]expectedEvent)
	for _, expectation := range expectations {
		stream := expectation.Stream
		offsets := stream.ArrivalOffsets()
		optional := make(map[int]bool, len(expectation.UnackedIndexes))
		for _, index := range expectation.UnackedIndexes {
			optional[index] = true
		}
		for index, offset := range offsets {
			body := stream.Body(index, stream.StartUnixNs+offset.Nanoseconds())
			expected[eventKey{stream.TenantID, stream.SourceID, stream.EventID(index)}] = expectedEvent{
				digest:        sha256.Sum256(body),
				amountCents:   stream.AmountCents(index),
				receiptID:     stream.ReceiptID(index),
				optional:      optional[index],
				expectsEffect: expectation.ExpectEffects,
			}
		}
	}
	return expected
}

// Run compares both databases with the expectation.
func Run(ctx context.Context, engineURL, receiverURL string, expectations []Expectation) (Report, error) {
	var report Report
	expected := buildExpected(expectations)
	for _, event := range expected {
		if event.optional {
			report.OptionalEvents++
			continue
		}
		report.ExpectedEvents++
		if event.expectsEffect {
			report.ExpectedEffects++
		}
	}

	engine, err := pgx.Connect(ctx, engineURL)
	if err != nil {
		return report, fmt.Errorf("connect to engine: %w", err)
	}
	defer engine.Close(ctx)
	receiver, err := pgx.Connect(ctx, receiverURL)
	if err != nil {
		return report, fmt.Errorf("connect to receiver: %w", err)
	}
	defer receiver.Close(ctx)

	if err := compareEvents(ctx, engine, expected, &report); err != nil {
		return report, err
	}
	if err := compareLedger(ctx, receiver, expected, &report); err != nil {
		return report, err
	}
	if err := countEngineState(ctx, engine, &report); err != nil {
		return report, err
	}
	report.Pass = report.MissingEvents == 0 && report.UnexpectedEvents == 0 && report.DigestMismatches == 0 &&
		report.MissingEffects == 0 && report.RepeatedEffects == 0 && report.UnexpectedEffects == 0 &&
		report.AmountMismatches == 0 && report.DeadLetters == 0 && report.UnfinishedJobs == 0
	return report, nil
}

func compareEvents(ctx context.Context, engine *pgx.Conn, expected map[eventKey]expectedEvent, report *Report) error {
	rows, err := engine.Query(ctx, `SELECT tenant_id, source_id, event_id, payload_sha256 FROM events`)
	if err != nil {
		return fmt.Errorf("read events: %w", err)
	}
	defer rows.Close()
	stored := make(map[eventKey]bool, len(expected))
	for rows.Next() {
		var key eventKey
		var digest []byte
		if err := rows.Scan(&key.tenantID, &key.sourceID, &key.eventID, &digest); err != nil {
			return err
		}
		report.StoredEvents++
		stored[key] = true
		event, known := expected[key]
		if !known {
			report.UnexpectedEvents++
			continue
		}
		if string(digest) != string(event.digest[:]) {
			report.DigestMismatches++
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for key, event := range expected {
		if !event.optional && !stored[key] {
			report.MissingEvents++
		}
	}
	return nil
}

func compareLedger(ctx context.Context, receiver *pgx.Conn, expected map[eventKey]expectedEvent, report *Report) error {
	rows, err := receiver.Query(ctx, `
		SELECT tenant_id, source_id, event_id, receipt_id, amount_cents, payload_sha256 FROM expense_ledger`)
	if err != nil {
		return fmt.Errorf("read ledger: %w", err)
	}
	defer rows.Close()
	seen := make(map[eventKey]int, len(expected))
	for rows.Next() {
		var key eventKey
		var receiptID string
		var amountCents int64
		var digest []byte
		if err := rows.Scan(&key.tenantID, &key.sourceID, &key.eventID, &receiptID, &amountCents, &digest); err != nil {
			return err
		}
		report.LedgerRows++
		seen[key]++
		event, known := expected[key]
		if !known || !event.expectsEffect {
			report.UnexpectedEffects++
			continue
		}
		if amountCents != event.amountCents || receiptID != event.receiptID {
			report.AmountMismatches++
		}
		if string(digest) != string(event.digest[:]) {
			report.DigestMismatches++
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	report.DistinctEffects = len(seen)
	for _, count := range seen {
		report.RepeatedEffects += count - 1
	}
	for key, event := range expected {
		if event.expectsEffect && !event.optional && seen[key] == 0 {
			report.MissingEffects++
		}
	}
	return nil
}

func countEngineState(ctx context.Context, engine *pgx.Conn, report *Report) error {
	err := engine.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE state = 'dead'),
		       count(*) FILTER (WHERE state IN ('pending', 'leased'))
		FROM deliveries`).Scan(&report.DeadLetters, &report.UnfinishedJobs)
	if err != nil {
		return fmt.Errorf("read delivery states: %w", err)
	}
	err = engine.QueryRow(ctx, `
		SELECT count(*),
		       count(*) FILTER (WHERE stale),
		       count(*) FILTER (WHERE lease_expired_at IS NOT NULL)
		FROM delivery_attempts`).Scan(&report.AttemptsTotal, &report.StaleAttempts, &report.ExpiredLeases)
	if err != nil {
		return fmt.Errorf("read attempts: %w", err)
	}
	return engine.QueryRow(ctx, `SELECT count(*) FROM event_conflicts`).Scan(&report.RecordedConflicts)
}
