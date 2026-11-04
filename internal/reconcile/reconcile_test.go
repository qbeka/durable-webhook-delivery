package reconcile

import (
	"context"
	"crypto/sha256"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/qbeka/durable-webhook-delivery/internal/receiver"
	"github.com/qbeka/durable-webhook-delivery/internal/store"
	"github.com/qbeka/durable-webhook-delivery/internal/testdb"
	"github.com/qbeka/durable-webhook-delivery/internal/workload"
)

type fixture struct {
	engineURL, receiverURL string
	engine, ledger         *pgx.Conn
	stream                 workload.Stream
}

// A reconciler that never fails is as useless as a test that never fails, so
// these tests plant each kind of damage and require the report to name it.
func newFixture(t *testing.T) *fixture {
	t.Helper()
	ctx := context.Background()
	engineURL, receiverURL := testdb.Engine(t), testdb.Receiver(t)
	database, err := store.Open(ctx, engineURL, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	pool, err := receiver.Connect(ctx, receiverURL, 2)
	if err != nil {
		t.Fatal(err)
	}
	pool.Close()
	engine, err := pgx.Connect(ctx, engineURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { engine.Close(context.Background()) })
	ledger, err := pgx.Connect(ctx, receiverURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ledger.Close(context.Background()) })

	stream := workload.Stream{TenantID: "t", SourceID: "s", Seed: 3, EventCount: 20, RatePerSecond: 100, PayloadBytes: 256, StartUnixNs: 1_000}
	fixtures := &fixture{engineURL, receiverURL, engine, ledger, stream}
	fixtures.loadPerfectRun(t)
	return fixtures
}

func (f *fixture) body(index int) []byte {
	offset := f.stream.ArrivalOffsets()[index]
	return f.stream.Body(index, f.stream.StartUnixNs+offset.Nanoseconds())
}

func (f *fixture) loadPerfectRun(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	for index := range f.stream.EventCount {
		digest := sha256.Sum256(f.body(index))
		mustExec(t, f.engine, `INSERT INTO events (tenant_id, source_id, event_id, event_type, payload, payload_sha256)
			VALUES ($1, $2, $3, 'receipt.forwarded', 'x', $4)`,
			f.stream.TenantID, f.stream.SourceID, f.stream.EventID(index), digest[:])
		f.insertLedger(t, index, f.stream.AmountCents(index), digest[:])
	}
	_ = ctx
}

func (f *fixture) insertLedger(t *testing.T, index int, cents int64, digest []byte) {
	t.Helper()
	mustExec(t, f.ledger, `INSERT INTO expense_ledger (delivery_key, tenant_id, source_id, event_id, receipt_id,
		amount_cents, currency, merchant, payload_sha256) VALUES ($1, $2, $3, $4, $5, $6, 'CAD', 'm', $7)`,
		"key", f.stream.TenantID, f.stream.SourceID, f.stream.EventID(index), f.stream.ReceiptID(index), cents, digest)
}

func mustExec(t *testing.T, connection *pgx.Conn, statement string, arguments ...any) {
	t.Helper()
	if _, err := connection.Exec(context.Background(), statement, arguments...); err != nil {
		t.Fatalf("%s: %v", statement, err)
	}
}

func (f *fixture) reconcile(t *testing.T, unacked ...int) Report {
	t.Helper()
	report, err := Run(context.Background(), f.engineURL, f.receiverURL, []Expectation{
		{Stream: f.stream, ExpectEffects: true, UnackedIndexes: unacked},
	})
	if err != nil {
		t.Fatal(err)
	}
	return report
}

func TestPerfectRunPasses(t *testing.T) {
	report := newFixture(t).reconcile(t)
	if !report.Pass || report.ExpectedEvents != 20 || report.LedgerRows != 20 || report.DistinctEffects != 20 {
		t.Fatalf("%+v", report)
	}
}

func TestEachKindOfDamageIsReported(t *testing.T) {
	cases := []struct {
		name   string
		damage func(*testing.T, *fixture)
		count  func(Report) int
		want   int
	}{
		{"repeated effect", func(t *testing.T, f *fixture) {
			digest := sha256.Sum256(f.body(4))
			f.insertLedger(t, 4, f.stream.AmountCents(4), digest[:])
		}, func(r Report) int { return r.RepeatedEffects }, 1},
		{"missing effect", func(t *testing.T, f *fixture) {
			mustExec(t, f.ledger, `DELETE FROM expense_ledger WHERE event_id = $1`, f.stream.EventID(5))
		}, func(r Report) int { return r.MissingEffects }, 1},
		{"wrong amount", func(t *testing.T, f *fixture) {
			mustExec(t, f.ledger, `UPDATE expense_ledger SET amount_cents = amount_cents + 1 WHERE event_id = $1`, f.stream.EventID(6))
		}, func(r Report) int { return r.AmountMismatches }, 1},
		{"wrong ledger digest", func(t *testing.T, f *fixture) {
			mustExec(t, f.ledger, `UPDATE expense_ledger SET payload_sha256 = 'bad' WHERE event_id = $1`, f.stream.EventID(7))
		}, func(r Report) int { return r.DigestMismatches }, 1},
		{"wrong event digest", func(t *testing.T, f *fixture) {
			mustExec(t, f.engine, `UPDATE events SET payload_sha256 = 'bad' WHERE event_id = $1`, f.stream.EventID(8))
		}, func(r Report) int { return r.DigestMismatches }, 1},
		{"missing event", func(t *testing.T, f *fixture) {
			mustExec(t, f.engine, `DELETE FROM events WHERE event_id = $1`, f.stream.EventID(9))
		}, func(r Report) int { return r.MissingEvents }, 1},
		{"unexpected event", func(t *testing.T, f *fixture) {
			mustExec(t, f.engine, `INSERT INTO events (tenant_id, source_id, event_id, event_type, payload, payload_sha256)
				VALUES ('t', 's', 'stranger', 'x', 'x', 'x')`)
		}, func(r Report) int { return r.UnexpectedEvents }, 1},
		{"unexpected effect", func(t *testing.T, f *fixture) {
			mustExec(t, f.ledger, `INSERT INTO expense_ledger (delivery_key, tenant_id, source_id, event_id, receipt_id,
				amount_cents, currency, merchant, payload_sha256) VALUES ('k', 't', 's', 'stranger', 'r', 1, 'CAD', 'm', 'x')`)
		}, func(r Report) int { return r.UnexpectedEffects }, 1},
		{"dead letter", func(t *testing.T, f *fixture) { f.addDelivery(t, "dead") }, func(r Report) int { return r.DeadLetters }, 1},
		{"unfinished job", func(t *testing.T, f *fixture) { f.addDelivery(t, "pending") }, func(r Report) int { return r.UnfinishedJobs }, 1},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			run := newFixture(t)
			testCase.damage(t, run)
			report := run.reconcile(t)
			if report.Pass {
				t.Fatalf("damage was not noticed: %+v", report)
			}
			if got := testCase.count(report); got != testCase.want {
				t.Fatalf("got %d, want %d: %+v", got, testCase.want, report)
			}
		})
	}
}

func (f *fixture) addDelivery(t *testing.T, state string) {
	t.Helper()
	mustExec(t, f.engine, `INSERT INTO destinations (tenant_id, name, url, signing_secret) VALUES ('t', 'd', 'u', 's')`)
	mustExec(t, f.engine, `INSERT INTO deliveries (event_row_id, destination_id, delivery_key, state)
		SELECT (SELECT min(id) FROM events), (SELECT min(id) FROM destinations), 'k', $1`, state)
}

func TestAnUnacknowledgedEventMayExistOrNot(t *testing.T) {
	run := newFixture(t)
	if report := run.reconcile(t, 3); !report.Pass || report.OptionalEvents != 1 {
		t.Fatalf("stored but unacknowledged: %+v", report)
	}
	mustExec(t, run.engine, `DELETE FROM events WHERE event_id = $1`, run.stream.EventID(3))
	mustExec(t, run.ledger, `DELETE FROM expense_ledger WHERE event_id = $1`, run.stream.EventID(3))
	if report := run.reconcile(t, 3); !report.Pass {
		t.Fatalf("absent and unacknowledged: %+v", report)
	}
}
