// Package receiver is a reference webhook receiver with an expense ledger.
//
// It runs in one of two modes. They share all code except one branch in apply,
// so the naive mode is a fair negative control: same signatures, same ledger,
// same injected faults.
package receiver

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"syscall"
	"time"

	_ "embed"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/qbeka/durable-webhook-delivery/internal/signing"
)

//go:embed schema.sql
var schemaSQL string

// Mode selects how a delivery becomes an effect.
type Mode string

const (
	// ModeIdempotent commits the delivery key and the expense together.
	ModeIdempotent Mode = "idempotent"
	// ModeNaive records an expense for every request. It is the negative control.
	ModeNaive Mode = "naive"
)

// Outcome says what a request did to the ledger.
type Outcome string

const (
	OutcomeApplied   Outcome = "applied"
	OutcomeDuplicate Outcome = "duplicate"
	OutcomeConflict  Outcome = "conflict"
)

// Receipt is the business payload. Fields other than these are ignored.
type Receipt struct {
	ReceiptID   string `json:"receipt_id"`
	AmountCents int64  `json:"amount_cents"`
	Currency    string `json:"currency"`
	Merchant    string `json:"merchant"`
	// ArrivalUnixNanos is when the event was scheduled to arrive. It exists only
	// so the latency log can measure from arrival to durable effect.
	ArrivalUnixNanos int64 `json:"arrival_unix_ns"`
}

// Receiver handles deliveries.
type Receiver struct {
	Mode   Mode
	Secret string
	Pool   *pgxpool.Pool
	Faults *Faults
	Log    *EffectLog // optional
}

// Connect opens the receiver database and applies its schema.
func Connect(ctx context.Context, databaseURL string, maxConnections int32) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database URL: %w", err)
	}
	config.MaxConns = maxConnections
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	if _, err := pool.Exec(ctx, schemaSQL); err != nil {
		pool.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	return pool, nil
}

// Routes returns the delivery endpoint.
func (r *Receiver) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /webhooks/receipts", r.handle)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	return mux
}

type delivery struct {
	key      string
	attempt  int
	tenantID string
	sourceID string
	eventID  string
	digest   []byte
	receipt  Receipt
}

func (r *Receiver) handle(w http.ResponseWriter, request *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, request.Body, 1<<20))
	if err != nil {
		http.Error(w, "could not read body", http.StatusBadRequest)
		return
	}
	incoming, status, problem := r.authenticate(request, body)
	if status != 0 {
		http.Error(w, problem, status)
		return
	}

	if burst, retryAfterSeconds := r.Faults.takeStatusBurst(); burst != 0 {
		if retryAfterSeconds > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(retryAfterSeconds))
		}
		r.logRequest(incoming, "injected_"+strconv.Itoa(burst))
		w.WriteHeader(burst)
		return
	}
	r.Faults.waitBeforeCommit()

	outcome, err := r.apply(request.Context(), incoming)
	if err != nil {
		slog.Warn("apply failed", "delivery", incoming.key, "error", err)
		http.Error(w, "database error", http.StatusServiceUnavailable)
		return
	}

	// The transaction has committed. Whatever happens below, the effect stands.
	r.logRequest(incoming, string(outcome))
	switch r.Faults.takeAfterCommit() {
	case afterCommitDropResponse:
		// Closes the connection with no response: the ambiguous acknowledgement.
		panic(http.ErrAbortHandler)
	case afterCommitCrash:
		_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
		select {}
	}

	if outcome == OutcomeConflict {
		http.Error(w, "delivery key was used with a different body", http.StatusConflict)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": string(outcome), "delivery_key": incoming.key})
}

// authenticate verifies the signature before the body is treated as data. It
// returns a nonzero status when the request must be refused.
func (r *Receiver) authenticate(request *http.Request, body []byte) (delivery, int, string) {
	header := request.Header
	unixSeconds, err := signing.ParseFreshTimestamp(header.Get("Hook-Timestamp"), time.Now())
	if err != nil {
		return delivery{}, http.StatusUnauthorized, "bad signature"
	}
	key := header.Get("Hook-Delivery-Key")
	digest := signing.BodyDigest(body)
	expected := signing.DeliverySignature(r.Secret, unixSeconds, key, digest)
	if key == "" || !signing.Equal(expected, header.Get("Hook-Signature")) {
		return delivery{}, http.StatusUnauthorized, "bad signature"
	}
	var receipt Receipt
	if err := json.Unmarshal(body, &receipt); err != nil || receipt.ReceiptID == "" {
		return delivery{}, http.StatusBadRequest, "body is not a receipt"
	}
	attempt, _ := strconv.Atoi(header.Get("Hook-Attempt"))
	return delivery{
		key: key, attempt: attempt, digest: digest, receipt: receipt,
		tenantID: header.Get("Hook-Tenant"), sourceID: header.Get("Hook-Source"), eventID: header.Get("Hook-Event-Id"),
	}, 0, ""
}

// apply turns a delivery into an effect. This is the only place the two modes differ.
func (r *Receiver) apply(ctx context.Context, incoming delivery) (Outcome, error) {
	if r.Mode == ModeNaive {
		// Bug kept on purpose: nothing checks whether this key was already applied.
		_, err := r.Pool.Exec(ctx, insertLedgerSQL, ledgerArguments(incoming)...)
		if err != nil {
			return "", fmt.Errorf("insert ledger row: %w", err)
		}
		return OutcomeApplied, nil
	}

	// One statement is one transaction, so the key and the expense commit
	// together or not at all. A concurrent request with the same key waits on the
	// primary key until this commits, then inserts nothing.
	var ledgerID int64
	err := r.Pool.QueryRow(ctx, applyIdempotentSQL, ledgerArguments(incoming)...).Scan(&ledgerID)
	if err == nil {
		return OutcomeApplied, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("apply delivery: %w", err)
	}
	var storedDigest []byte
	err = r.Pool.QueryRow(ctx,
		`SELECT payload_sha256 FROM processed_deliveries WHERE delivery_key = $1`, incoming.key).Scan(&storedDigest)
	if err != nil {
		return "", fmt.Errorf("read processed delivery: %w", err)
	}
	if hex.EncodeToString(storedDigest) != hex.EncodeToString(incoming.digest) {
		return OutcomeConflict, nil
	}
	return OutcomeDuplicate, nil
}

const insertLedgerSQL = `
	INSERT INTO expense_ledger (delivery_key, tenant_id, source_id, event_id, receipt_id,
	                            amount_cents, currency, merchant, payload_sha256)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`

const applyIdempotentSQL = `
	WITH first_seen AS (
		INSERT INTO processed_deliveries (delivery_key, payload_sha256)
		VALUES ($1, $9)
		ON CONFLICT (delivery_key) DO NOTHING
		RETURNING delivery_key
	)
	INSERT INTO expense_ledger (delivery_key, tenant_id, source_id, event_id, receipt_id,
	                            amount_cents, currency, merchant, payload_sha256)
	SELECT first_seen.delivery_key, $2, $3, $4, $5, $6, $7, $8, $9 FROM first_seen
	RETURNING id`

func ledgerArguments(incoming delivery) []any {
	return []any{
		incoming.key, incoming.tenantID, incoming.sourceID, incoming.eventID, incoming.receipt.ReceiptID,
		incoming.receipt.AmountCents, incoming.receipt.Currency, incoming.receipt.Merchant, incoming.digest,
	}
}

func (r *Receiver) logRequest(incoming delivery, outcome string) {
	if r.Log != nil {
		r.Log.Record(incoming, outcome)
	}
}
