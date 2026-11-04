package receiver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/qbeka/durable-webhook-delivery/internal/signing"
	"github.com/qbeka/durable-webhook-delivery/internal/testdb"
)

const testSecret = "whsec_test"

func startReceiver(t *testing.T, mode Mode) (*Receiver, *httptest.Server) {
	t.Helper()
	pool, err := Connect(context.Background(), testdb.Receiver(t), 40)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	handler := &Receiver{Mode: mode, Secret: testSecret, Pool: pool, Faults: &Faults{}}
	server := httptest.NewServer(handler.Routes())
	t.Cleanup(server.Close)
	return handler, server
}

func receiptBody(id string, cents int64) []byte {
	body, _ := json.Marshal(Receipt{ReceiptID: id, AmountCents: cents, Currency: "CAD", Merchant: "Synthetic"})
	return body
}

func post(t *testing.T, server *httptest.Server, key string, body []byte, secret string) *http.Response {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, server.URL+"/webhooks/receipts", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	unixSeconds := time.Now().Unix()
	request.Header.Set("Hook-Delivery-Key", key)
	request.Header.Set("Hook-Attempt", "1")
	request.Header.Set("Hook-Tenant", "tenant")
	request.Header.Set("Hook-Source", "source")
	request.Header.Set("Hook-Event-Id", "evt-"+key)
	request.Header.Set("Hook-Timestamp", strconv.FormatInt(unixSeconds, 10))
	request.Header.Set("Hook-Signature", signing.DeliverySignature(secret, unixSeconds, key, signing.BodyDigest(body)))
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { response.Body.Close() })
	return response
}

func ledgerRows(t *testing.T, handler *Receiver, key string) int {
	t.Helper()
	var count int
	err := handler.Pool.QueryRow(context.Background(),
		`SELECT count(*) FROM expense_ledger WHERE delivery_key = $1`, key).Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	return count
}

func statusField(t *testing.T, response *http.Response) string {
	t.Helper()
	var answer map[string]string
	if err := json.NewDecoder(response.Body).Decode(&answer); err != nil {
		t.Fatal(err)
	}
	return answer["status"]
}

func TestIdempotentReceiverAppliesOnceAndAnswersDuplicate(t *testing.T) {
	handler, server := startReceiver(t, ModeIdempotent)
	body := receiptBody("r1", 1299)
	first := post(t, server, "key", body, testSecret)
	if first.StatusCode != 200 || statusField(t, first) != "applied" {
		t.Fatalf("first: %d", first.StatusCode)
	}
	second := post(t, server, "key", body, testSecret)
	if second.StatusCode != 200 || statusField(t, second) != "duplicate" {
		t.Fatalf("second: %d", second.StatusCode)
	}
	if got := ledgerRows(t, handler, "key"); got != 1 {
		t.Fatalf("got %d ledger rows", got)
	}
}

// Requests overlapping in time are the hard case: a lease expired while the old
// worker's request was still in flight, so both arrive at once.
func TestConcurrentRequestsWithOneKeyApplyOnce(t *testing.T) {
	handler, server := startReceiver(t, ModeIdempotent)
	body := receiptBody("r1", 1299)
	var group sync.WaitGroup
	for range 32 {
		group.Add(1)
		go func() {
			defer group.Done()
			if response := post(t, server, "shared-key", body, testSecret); response.StatusCode != 200 {
				t.Errorf("got %d", response.StatusCode)
			}
		}()
	}
	group.Wait()
	if got := ledgerRows(t, handler, "shared-key"); got != 1 {
		t.Fatalf("got %d ledger rows from 32 concurrent requests", got)
	}
}

func TestSameKeyWithDifferentBodyIsRefused(t *testing.T) {
	handler, server := startReceiver(t, ModeIdempotent)
	post(t, server, "key", receiptBody("r1", 100), testSecret)
	response := post(t, server, "key", receiptBody("r1", 999), testSecret)
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("got %d", response.StatusCode)
	}
	if got := ledgerRows(t, handler, "key"); got != 1 {
		t.Fatalf("got %d ledger rows", got)
	}
}

func TestBadSignatureWritesNothing(t *testing.T) {
	for _, mode := range []Mode{ModeIdempotent, ModeNaive} {
		handler, server := startReceiver(t, mode)
		response := post(t, server, "key", receiptBody("r1", 100), "wrong-secret")
		if response.StatusCode != http.StatusUnauthorized {
			t.Fatalf("%s: got %d", mode, response.StatusCode)
		}
		if got := ledgerRows(t, handler, "key"); got != 0 {
			t.Fatalf("%s: got %d ledger rows", mode, got)
		}
	}
}

func TestNonReceiptBodyIsRefusedAsTerminal(t *testing.T) {
	_, server := startReceiver(t, ModeIdempotent)
	if response := post(t, server, "key", []byte("not json"), testSecret); response.StatusCode != http.StatusBadRequest {
		t.Fatalf("got %d", response.StatusCode)
	}
}

// The negative control must be able to fail, or a clean run proves nothing.
func TestNaiveReceiverRecordsARepeatedEffect(t *testing.T) {
	handler, server := startReceiver(t, ModeNaive)
	body := receiptBody("r1", 1299)
	for range 2 {
		if response := post(t, server, "key", body, testSecret); response.StatusCode != 200 {
			t.Fatalf("got %d", response.StatusCode)
		}
	}
	if got := ledgerRows(t, handler, "key"); got != 2 {
		t.Fatalf("got %d ledger rows, the control must show the duplicate", got)
	}
}

func TestDroppedResponseLeavesTheEffectCommitted(t *testing.T) {
	handler, server := startReceiver(t, ModeIdempotent)
	handler.Faults.active = FaultPlan{DropResponseAfterCommit: 1}

	body := receiptBody("r1", 1299)
	request, err := http.NewRequest(http.MethodPost, server.URL+"/webhooks/receipts", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	unixSeconds := time.Now().Unix()
	request.Header.Set("Hook-Delivery-Key", "key")
	request.Header.Set("Hook-Timestamp", fmt.Sprint(unixSeconds))
	request.Header.Set("Hook-Signature", signing.DeliverySignature(testSecret, unixSeconds, "key", signing.BodyDigest(body)))
	if _, err := http.DefaultClient.Do(request); err == nil {
		t.Fatal("the sender should see a failed request")
	}
	if got := ledgerRows(t, handler, "key"); got != 1 {
		t.Fatalf("the effect must be committed even though the sender saw a failure: %d", got)
	}
	retry := post(t, server, "key", body, testSecret)
	if retry.StatusCode != 200 || statusField(t, retry) != "duplicate" {
		t.Fatalf("retry: %d", retry.StatusCode)
	}
	if got := ledgerRows(t, handler, "key"); got != 1 {
		t.Fatalf("got %d ledger rows after the retry", got)
	}
}

func TestStatusBurstEndsAfterItsCount(t *testing.T) {
	handler, server := startReceiver(t, ModeIdempotent)
	handler.Faults.active = FaultPlan{StatusBurstCount: 2, StatusBurstCode: 429, RetryAfterSeconds: 7}
	body := receiptBody("r1", 1)
	for range 2 {
		response := post(t, server, "key", body, testSecret)
		if response.StatusCode != 429 || response.Header.Get("Retry-After") != "7" {
			t.Fatalf("got %d %q", response.StatusCode, response.Header.Get("Retry-After"))
		}
	}
	if got := ledgerRows(t, handler, "key"); got != 0 {
		t.Fatal("a refused request must not write the ledger")
	}
	if response := post(t, server, "key", body, testSecret); response.StatusCode != 200 {
		t.Fatalf("got %d after the burst", response.StatusCode)
	}
}
