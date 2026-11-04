package worker

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/qbeka/durable-webhook-delivery/internal/signing"
	"github.com/qbeka/durable-webhook-delivery/internal/store"
	"github.com/qbeka/durable-webhook-delivery/internal/testdb"
)

func testConfig(id string) Config {
	return Config{
		WorkerID: id, Concurrency: 8,
		Lease: 2 * time.Second, LeaseMargin: 200 * time.Millisecond, RequestTimeout: time.Second,
		PollInterval: 5 * time.Millisecond, ReapInterval: 50 * time.Millisecond, ReapBatch: 100,
		Policy:  Policy{MaxAttempts: 4, MaxAge: time.Hour, RetryBase: 10 * time.Millisecond, RetryCap: 50 * time.Millisecond},
		Breaker: store.BreakerConfig{FailureThreshold: 100, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond},
	}
}

type testRoute struct {
	database    *store.Store
	destination int64
}

func setUp(t *testing.T, url string) testRoute {
	t.Helper()
	ctx := context.Background()
	database, err := store.Open(ctx, testdb.Engine(t), 16)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(database.Close)
	if err := database.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := database.PutSource(ctx, "tenant", "source", "s"); err != nil {
		t.Fatal(err)
	}
	destination, err := database.PutDestination(ctx, store.Destination{
		TenantID: "tenant", Name: "d", URL: url, Secret: "whsec", MaxInFlight: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Subscribe(ctx, "tenant", "source", "*", destination); err != nil {
		t.Fatal(err)
	}
	return testRoute{database: database, destination: destination}
}

func (r testRoute) ingest(t *testing.T, eventID string) {
	t.Helper()
	body := []byte(`{"receipt_id":"` + eventID + `"}`)
	_, err := r.database.Ingest(context.Background(), store.Event{
		TenantID: "tenant", SourceID: "source", EventID: eventID, EventType: "receipt.forwarded",
		Payload: body, PayloadSHA256: signing.BodyDigest(body),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func (r testRoute) run(t *testing.T, config Config) func() {
	t.Helper()
	engine, err := New(config, r.database)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		engine.Run(ctx)
	}()
	return func() {
		cancel()
		<-done
	}
}

// waitForState polls until the destination has no unfinished deliveries.
func (r testRoute) waitFor(t *testing.T, check func(store.DestinationStatus) bool) store.DestinationStatus {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		statuses, err := r.database.Status(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if check(statuses[0]) {
			return statuses[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	statuses, _ := r.database.Status(context.Background())
	t.Fatalf("timed out: %+v", statuses)
	return store.DestinationStatus{}
}

func finished(status store.DestinationStatus) bool {
	return status.Pending == 0 && status.Leased == 0
}

func TestDeliverySendsASignatureTheReceiverCanVerify(t *testing.T) {
	var verified atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		unixSeconds, _ := strconv.ParseInt(r.Header.Get("Hook-Timestamp"), 10, 64)
		digest := signing.BodyDigest(body)
		expected := signing.DeliverySignature("whsec", unixSeconds, r.Header.Get("Hook-Delivery-Key"), digest)
		if signing.Equal(expected, r.Header.Get("Hook-Signature")) &&
			r.Header.Get("Hook-Payload-Sha256") == hex.EncodeToString(digest) &&
			r.Header.Get("Hook-Event-Id") == "evt" && r.Header.Get("Hook-Attempt") == "1" {
			verified.Add(1)
		}
	}))
	defer server.Close()
	route := setUp(t, server.URL)
	stop := route.run(t, testConfig("w"))
	defer stop()

	route.ingest(t, "evt")
	route.waitFor(t, func(s store.DestinationStatus) bool { return s.Succeeded == 1 })
	if verified.Load() != 1 {
		t.Fatal("receiver could not verify the request")
	}
}

func TestRetriesKeepOneDeliveryKeyAndKeepHistory(t *testing.T) {
	var requests atomic.Int64
	keys := make(chan string, 16)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		keys <- r.Header.Get("Hook-Delivery-Key")
		if requests.Add(1) < 3 {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer server.Close()
	route := setUp(t, server.URL)
	stop := route.run(t, testConfig("w"))
	defer stop()

	route.ingest(t, "evt")
	route.waitFor(t, func(s store.DestinationStatus) bool { return s.Succeeded == 1 })
	close(keys)
	first := <-keys
	for key := range keys {
		if key != first {
			t.Fatalf("delivery key changed between attempts: %s then %s", first, key)
		}
	}
	record, err := route.database.DeliveryHistory(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Attempts) != 3 || *record.Attempts[0].Outcome != "retryable_status" || *record.Attempts[2].Outcome != "success" {
		t.Fatalf("history: %+v", record.Attempts)
	}
}

func TestTerminalStatusGoesDeadWithoutRetrying(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()
	route := setUp(t, server.URL)
	stop := route.run(t, testConfig("w"))
	defer stop()

	route.ingest(t, "evt")
	route.waitFor(t, func(s store.DestinationStatus) bool { return s.Dead == 1 })
	time.Sleep(100 * time.Millisecond)
	if requests.Load() != 1 {
		t.Fatalf("got %d requests", requests.Load())
	}
}

func TestRetryableFailureEndsDeadWhenAttemptsRunOut(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	route := setUp(t, server.URL)
	config := testConfig("w")
	stop := route.run(t, config)
	defer stop()

	route.ingest(t, "evt")
	route.waitFor(t, func(s store.DestinationStatus) bool { return s.Dead == 1 })
	if requests.Load() != int64(config.Policy.MaxAttempts) {
		t.Fatalf("got %d requests, want %d", requests.Load(), config.Policy.MaxAttempts)
	}
}

func TestRedirectIsNotFollowed(t *testing.T) {
	var followed atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { followed.Add(1) }))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	route := setUp(t, server.URL)
	stop := route.run(t, testConfig("w"))
	defer stop()

	route.ingest(t, "evt")
	route.waitFor(t, func(s store.DestinationStatus) bool { return s.Dead == 1 })
	if followed.Load() != 0 {
		t.Fatal("followed a redirect with a signed request")
	}
}

// A broken Location makes http.Client fail before any redirect hook runs, which
// turned a terminal answer into a retried connection error.
func TestRedirectWithABrokenLocationIsTerminal(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set("Location", "http://[broken")
		w.WriteHeader(http.StatusFound)
	}))
	defer server.Close()
	route := setUp(t, server.URL)
	stop := route.run(t, testConfig("w"))
	defer stop()

	route.ingest(t, "evt")
	route.waitFor(t, func(s store.DestinationStatus) bool { return s.Dead == 1 })
	if requests.Load() != 1 {
		t.Fatalf("got %d requests, want 1", requests.Load())
	}
}

// One continuous outage must not use up a delivery's attempts, however long it
// lasts, because the breaker's probes spend none.
func TestOutageDoesNotSendAHealthyDeliveryToDeadLetters(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	route := setUp(t, server.URL)
	config := testConfig("w")
	config.Breaker = store.BreakerConfig{FailureThreshold: 2, BaseDelay: 10 * time.Millisecond, MaxDelay: 20 * time.Millisecond}
	stop := route.run(t, config)
	defer stop()

	route.ingest(t, "evt")
	for deadline := time.Now().Add(1500 * time.Millisecond); time.Now().Before(deadline); {
		statuses, err := route.database.Status(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if statuses[0].Dead > 0 {
			t.Fatalf("went dead after %d requests: %+v", requests.Load(), statuses[0])
		}
		time.Sleep(5 * time.Millisecond)
	}
	if requests.Load() <= int64(config.Policy.MaxAttempts) {
		t.Fatalf("only %d requests, so the test proved nothing", requests.Load())
	}
}

func TestAgeLimitEndsADeliveryThatOnlyGetsProbes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	route := setUp(t, server.URL)
	config := testConfig("w")
	config.Policy.MaxAge = 300 * time.Millisecond
	config.Breaker = store.BreakerConfig{FailureThreshold: 1, BaseDelay: 10 * time.Millisecond, MaxDelay: 20 * time.Millisecond}
	stop := route.run(t, config)
	defer stop()

	route.ingest(t, "evt")
	route.waitFor(t, func(s store.DestinationStatus) bool { return s.Dead == 1 })
}

func TestSlowRequestTimesOutBeforeItsLease(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer server.Close()
	defer close(release)
	route := setUp(t, server.URL)
	config := testConfig("w")
	config.RequestTimeout = 100 * time.Millisecond
	config.Policy.MaxAttempts = 1
	stop := route.run(t, config)
	defer stop()

	route.ingest(t, "evt")
	status := route.waitFor(t, func(s store.DestinationStatus) bool { return s.Dead == 1 })
	if !finished(status) {
		t.Fatalf("%+v", status)
	}
	if status.Dead != 1 {
		t.Fatal("timeout was not recorded")
	}
	var key string
	if err := route.database.QueryRowForTest(context.Background(), `SELECT delivery_key FROM deliveries`).Scan(&key); err != nil {
		t.Fatal(err)
	}
	record, err := route.database.DeliveryHistory(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if outcome := *record.Attempts[0].Outcome; outcome != "timeout" {
		t.Fatalf("a slow request was recorded as %s", outcome)
	}
}

// A crash leaves a committed lease and an open attempt. Another worker must
// finish the delivery after the lease expires.
func TestAbandonedLeaseIsPickedUpByAnotherWorker(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	defer server.Close()
	route := setUp(t, server.URL)
	route.ingest(t, "evt")

	// This claim stands for a worker that died right after committing it.
	breaker := store.BreakerConfig{FailureThreshold: 100, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond}
	jobs, err := route.database.Claim(context.Background(), "dead-worker", 1, 100*time.Millisecond, breaker)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("claim: %v %v", jobs, err)
	}

	stop := route.run(t, testConfig("survivor"))
	defer stop()
	route.waitFor(t, func(s store.DestinationStatus) bool { return s.Succeeded == 1 })
	if requests.Load() != 1 {
		t.Fatalf("got %d requests", requests.Load())
	}
	record, err := route.database.DeliveryHistory(context.Background(), jobs[0].DeliveryKey)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Attempts) != 2 || *record.Attempts[0].Outcome != "lease_expired" || record.Attempts[1].WorkerID != "survivor" {
		t.Fatalf("history: %+v", record.Attempts)
	}
}

func TestMultipleWorkersDrainAQueueWithoutRepeatingARequest(t *testing.T) {
	var perKey [200]atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var index int
		if _, err := fmt.Sscanf(string(body), `{"receipt_id":"evt-%d"}`, &index); err != nil {
			t.Errorf("parse %q: %v", body, err)
			return
		}
		perKey[index].Add(1)
	}))
	defer server.Close()
	route := setUp(t, server.URL)
	for index := range perKey {
		route.ingest(t, fmt.Sprintf("evt-%d", index))
	}
	for number := range 3 {
		stop := route.run(t, testConfig(fmt.Sprintf("w%d", number)))
		defer stop()
	}
	route.waitFor(t, func(s store.DestinationStatus) bool { return s.Succeeded == int64(len(perKey)) })
	for index := range perKey {
		if got := perKey[index].Load(); got != 1 {
			t.Errorf("event %d sent %d times", index, got)
		}
	}
}

// A worker paused past its lease must still leave its report in the history.
func TestFinishAfterTheLeaseEndedIsRecordedAsStale(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()
	route := setUp(t, server.URL)
	route.ingest(t, "evt")
	ctx := context.Background()
	config := testConfig("paused")

	jobs, err := route.database.Claim(ctx, "paused", 1, 20*time.Millisecond, config.Breaker)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("claim: %v %v", jobs, err)
	}
	time.Sleep(40 * time.Millisecond)
	if _, err := route.database.ExpireLeases(ctx, 10, 5, time.Hour, time.Millisecond, time.Millisecond); err != nil {
		t.Fatal(err)
	}

	engine, err := New(config, route.database)
	if err != nil {
		t.Fatal(err)
	}
	report := store.Finish{NextState: store.StateSucceeded, Outcome: "success", HTTPStatus: 200}
	engine.writeBatch([]completion{{
		Completion: store.Completion{Job: jobs[0], Report: report},
		giveUpAt:   giveUpTime(time.Now().Add(-time.Minute)), // the lease ended long ago
	}})

	record, err := route.database.DeliveryHistory(ctx, jobs[0].DeliveryKey)
	if err != nil {
		t.Fatal(err)
	}
	attempt := record.Attempts[0]
	if !attempt.Stale || attempt.Outcome == nil || *attempt.Outcome != "success" {
		t.Fatalf("the report was lost: %+v", attempt)
	}
	if record.State != store.StatePending {
		t.Fatalf("a stale report changed the delivery to %s", record.State)
	}
}

func TestConfigRefusesATimeoutThatCouldOutliveTheLease(t *testing.T) {
	config := testConfig("w")
	config.RequestTimeout = config.Lease
	if err := config.Validate(); err == nil {
		t.Fatal("a request as long as the lease must be refused")
	}
}

func TestWorkerSurvivesAStoppedClaimDuringShutdown(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()
	route := setUp(t, server.URL)
	stop := route.run(t, testConfig("w"))
	stopped := make(chan struct{})
	go func() {
		stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not stop")
	}
}
