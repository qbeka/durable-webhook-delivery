package store

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/qbeka/durable-webhook-delivery/internal/signing"
	"github.com/qbeka/durable-webhook-delivery/internal/testdb"
)

var testBreaker = BreakerConfig{FailureThreshold: 3, BaseDelay: 50 * time.Millisecond, MaxDelay: 200 * time.Millisecond}

func openTestStore(t *testing.T) *Store {
	t.Helper()
	ctx := context.Background()
	database, err := Open(ctx, testdb.Engine(t), 40)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(database.Close)
	if err := database.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return database
}

func addSource(t *testing.T, database *Store, tenant, source string) {
	t.Helper()
	if err := database.PutSource(context.Background(), tenant, source, "secret"); err != nil {
		t.Fatal(err)
	}
}

func addDestination(t *testing.T, database *Store, tenant, name string, maxInFlight int) int64 {
	t.Helper()
	id, err := database.PutDestination(context.Background(), Destination{
		TenantID: tenant, Name: name, URL: "http://127.0.0.1:1/" + name, Secret: "secret", MaxInFlight: maxInFlight,
	})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// addRoute registers one source and one destination subscribed to all types.
func addRoute(t *testing.T, database *Store, maxInFlight int) int64 {
	t.Helper()
	addSource(t, database, "tenant", "source")
	destinationID := addDestination(t, database, "tenant", "destination", maxInFlight)
	if err := database.Subscribe(context.Background(), "tenant", "source", "*", destinationID); err != nil {
		t.Fatal(err)
	}
	return destinationID
}

func testEvent(id, body string) Event {
	return Event{
		TenantID: "tenant", SourceID: "source", EventID: id, EventType: "receipt.forwarded",
		Payload: []byte(body), PayloadSHA256: signing.BodyDigest([]byte(body)),
	}
}

func mustIngest(t *testing.T, database *Store, event Event) IngestResult {
	t.Helper()
	result, err := database.Ingest(context.Background(), event)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func countRows(t *testing.T, database *Store, query string, arguments ...any) int {
	t.Helper()
	var count int
	if err := database.pool.QueryRow(context.Background(), query, arguments...).Scan(&count); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return count
}

func TestConcurrentIdenticalRequestsConvergeToOneEvent(t *testing.T) {
	database := openTestStore(t)
	addRoute(t, database, 4)

	const requests = 32
	statuses := make([]IngestStatus, requests)
	var group sync.WaitGroup
	for index := range requests {
		group.Add(1)
		go func() {
			defer group.Done()
			result, err := database.Ingest(context.Background(), testEvent("same", `{"a":1}`))
			if err != nil {
				t.Error(err)
				return
			}
			statuses[index] = result.Status
		}()
	}
	group.Wait()

	accepted, duplicates := 0, 0
	for _, status := range statuses {
		switch status {
		case IngestAccepted:
			accepted++
		case IngestDuplicate:
			duplicates++
		}
	}
	if accepted != 1 || duplicates != requests-1 {
		t.Fatalf("got %d accepted and %d duplicates", accepted, duplicates)
	}
	if got := countRows(t, database, `SELECT count(*) FROM events`); got != 1 {
		t.Fatalf("got %d events", got)
	}
	if got := countRows(t, database, `SELECT count(*) FROM deliveries`); got != 1 {
		t.Fatalf("got %d deliveries", got)
	}
}

func TestConflictingBodyKeepsTheStoredEventAndIsRecorded(t *testing.T) {
	database := openTestStore(t)
	addRoute(t, database, 4)
	original := mustIngest(t, database, testEvent("evt", "first"))

	for range 3 {
		result := mustIngest(t, database, testEvent("evt", "second"))
		if result.Status != IngestConflict {
			t.Fatalf("got %s, want conflict", result.Status)
		}
		if string(result.StoredSHA256) != string(original.StoredSHA256) {
			t.Fatal("conflict did not report the stored digest")
		}
	}
	if got := countRows(t, database, `SELECT count(*) FROM events WHERE payload = 'first'`); got != 1 {
		t.Fatal("stored event changed")
	}
	if got := countRows(t, database, `SELECT seen_count FROM event_conflicts`); got != 3 {
		t.Fatalf("seen_count = %d, want 3", got)
	}
	if got := countRows(t, database, `SELECT count(*) FROM deliveries`); got != 1 {
		t.Fatalf("a conflict added deliveries: %d", got)
	}
}

func TestSameTypeIsPartOfIdentityComparison(t *testing.T) {
	database := openTestStore(t)
	addRoute(t, database, 4)
	mustIngest(t, database, testEvent("evt", "body"))
	changed := testEvent("evt", "body")
	changed.EventType = "receipt.other"
	if result := mustIngest(t, database, changed); result.Status != IngestConflict {
		t.Fatalf("got %s, want conflict", result.Status)
	}
}

func TestEventIdentityIsScopedByTenantAndSource(t *testing.T) {
	database := openTestStore(t)
	addRoute(t, database, 4)
	addSource(t, database, "tenant", "other-source")
	addSource(t, database, "other-tenant", "source")

	for _, scope := range [][2]string{{"tenant", "source"}, {"tenant", "other-source"}, {"other-tenant", "source"}} {
		event := testEvent("same-id", "body")
		event.TenantID, event.SourceID = scope[0], scope[1]
		if result := mustIngest(t, database, event); result.Status != IngestAccepted {
			t.Fatalf("%v: got %s", scope, result.Status)
		}
	}
	if got := countRows(t, database, `SELECT count(*) FROM events`); got != 3 {
		t.Fatalf("got %d events", got)
	}
}

func TestUnknownSourceStoresNothing(t *testing.T) {
	database := openTestStore(t)
	_, err := database.Ingest(context.Background(), testEvent("evt", "body"))
	if err != ErrUnknownSource {
		t.Fatalf("got %v", err)
	}
	if got := countRows(t, database, `SELECT count(*) FROM events`); got != 0 {
		t.Fatalf("got %d events", got)
	}
}

func TestFanOutGivesEachDestinationItsOwnStableKey(t *testing.T) {
	database := openTestStore(t)
	addSource(t, database, "tenant", "source")
	ctx := context.Background()
	first := addDestination(t, database, "tenant", "first", 4)
	second := addDestination(t, database, "tenant", "second", 4)
	unrelated := addDestination(t, database, "tenant", "unrelated", 4)
	for _, subscription := range []struct {
		eventType   string
		destination int64
	}{{"receipt.forwarded", first}, {"*", second}, {"other.type", unrelated}, {"receipt.forwarded", second}} {
		if err := database.Subscribe(ctx, "tenant", "source", subscription.eventType, subscription.destination); err != nil {
			t.Fatal(err)
		}
	}

	result := mustIngest(t, database, testEvent("evt", "body"))
	if result.Deliveries != 2 {
		t.Fatalf("got %d deliveries, want 2: one per destination, even with two matching subscriptions", result.Deliveries)
	}
	if got := countRows(t, database, `SELECT count(DISTINCT delivery_key) FROM deliveries`); got != 2 {
		t.Fatalf("got %d distinct keys", got)
	}
	if duplicate := mustIngest(t, database, testEvent("evt", "body")); duplicate.Deliveries != 0 {
		t.Fatalf("a duplicate added %d deliveries", duplicate.Deliveries)
	}
}

// Fan-out happens once. A later subscription does not reach old events.
func TestDuplicateAfterSubscribeAddsNoDeliveries(t *testing.T) {
	database := openTestStore(t)
	addRoute(t, database, 4)
	mustIngest(t, database, testEvent("evt", "body"))
	late := addDestination(t, database, "tenant", "late", 4)
	if err := database.Subscribe(context.Background(), "tenant", "source", "*", late); err != nil {
		t.Fatal(err)
	}
	mustIngest(t, database, testEvent("evt", "body"))
	if got := countRows(t, database, `SELECT count(*) FROM deliveries WHERE destination_id = $1`, late); got != 0 {
		t.Fatalf("late subscription received %d deliveries", got)
	}
}

// When Subscribe or Unsubscribe returns, the very next event must see the change,
// and when Unsubscribe returns, nothing still in progress may add a delivery.
func TestSubscriptionChangesAreOrderedAgainstIngress(t *testing.T) {
	database := openTestStore(t)
	addSource(t, database, "tenant", "source")
	toggled := addDestination(t, database, "tenant", "toggled", 4)
	ctx := context.Background()

	var eventNumber atomic.Int64
	nextEvent := func() Event {
		return testEvent(fmt.Sprintf("evt-%d", eventNumber.Add(1)), "body")
	}

	stop := make(chan struct{})
	var background sync.WaitGroup
	for range 12 {
		background.Add(1)
		go func() {
			defer background.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := database.Ingest(ctx, nextEvent()); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}

	for round := range 40 {
		if err := database.Subscribe(ctx, "tenant", "source", "*", toggled); err != nil {
			t.Fatal(err)
		}
		if result := mustIngest(t, database, nextEvent()); result.Deliveries != 1 {
			t.Fatalf("round %d: event after Subscribe returned got %d deliveries", round, result.Deliveries)
		}
		if err := database.Unsubscribe(ctx, "tenant", "source", "*", toggled); err != nil {
			t.Fatal(err)
		}
		// Taken immediately after Unsubscribe returned. No in-progress ingress may
		// raise this number. A later Subscribe would, so check before the next round.
		before := countRows(t, database, `SELECT count(*) FROM deliveries`)
		if result := mustIngest(t, database, nextEvent()); result.Deliveries != 0 {
			t.Fatalf("round %d: event after Unsubscribe returned got %d deliveries", round, result.Deliveries)
		}
		time.Sleep(2 * time.Millisecond)
		if after := countRows(t, database, `SELECT count(*) FROM deliveries`); after != before {
			t.Fatalf("round %d: %d deliveries appeared after Unsubscribe returned", round, after-before)
		}
	}
	close(stop)
	background.Wait()
}

func ingestMany(t *testing.T, database *Store, prefix string, count int) {
	t.Helper()
	for index := range count {
		mustIngest(t, database, testEvent(fmt.Sprintf("%s-%d", prefix, index), `{"n":1}`))
	}
}

func claim(t *testing.T, database *Store, worker string, maxJobs int, lease time.Duration) []Job {
	t.Helper()
	jobs, err := database.Claim(context.Background(), worker, maxJobs, lease, testBreaker)
	if err != nil {
		t.Fatal(err)
	}
	return jobs
}

func TestClaimCarriesEverythingNeededToSend(t *testing.T) {
	database := openTestStore(t)
	addRoute(t, database, 4)
	mustIngest(t, database, testEvent("evt", "body"))

	jobs := claim(t, database, "worker", 10, time.Minute)
	if len(jobs) != 1 {
		t.Fatalf("got %d jobs", len(jobs))
	}
	job := jobs[0]
	if job.EventID != "evt" || string(job.Payload) != "body" || job.Attempt != 1 || job.BudgetUsed != 1 ||
		job.LeaseToken == "" || job.URL == "" || job.Secret == "" {
		t.Fatalf("incomplete job: %+v", job)
	}
	if got := countRows(t, database, `SELECT count(*) FROM delivery_attempts WHERE outcome IS NULL`); got != 1 {
		t.Fatalf("claim should leave one open attempt, got %d", got)
	}
}

// Sixteen concurrent workers must never hold more than the limit between them,
// and each job must go to exactly one worker. The peak comes from the database:
// one query sees one snapshot, so its count is exact, where counters kept by the
// workers would lag the commits they describe.
func TestConcurrentClaimsNeverExceedTheLimitOrShareAJob(t *testing.T) {
	database := openTestStore(t)
	const limit, total = 5, 200
	addRoute(t, database, limit)
	ingestMany(t, database, "evt", total)

	samplerDone := make(chan int)
	stopSampling := make(chan struct{})
	go func() {
		peak := 0
		for {
			select {
			case <-stopSampling:
				samplerDone <- peak
				return
			default:
				peak = max(peak, countRows(t, database, `SELECT count(*) FROM deliveries WHERE state = 'leased'`))
			}
		}
	}()

	var holders sync.Map
	var finished atomic.Int64
	var group sync.WaitGroup
	for workerNumber := range 16 {
		group.Add(1)
		go func() {
			defer group.Done()
			ctx := context.Background()
			for finished.Load() < total {
				jobs, err := database.Claim(ctx, fmt.Sprintf("worker-%d", workerNumber), 3, time.Minute, testBreaker)
				if err != nil {
					t.Error(err)
					return
				}
				for _, job := range jobs {
					if _, taken := holders.LoadOrStore(job.DeliveryID, workerNumber); taken {
						t.Errorf("delivery %d claimed twice", job.DeliveryID)
					}
				}
				for _, job := range jobs {
					applied, err := database.FinishAttempt(ctx, job, Finish{NextState: StateSucceeded, Outcome: "success", HTTPStatus: 200}, testBreaker)
					if err != nil || !applied {
						t.Errorf("finish: applied=%v err=%v", applied, err)
					}
					finished.Add(1)
				}
			}
		}()
	}
	group.Wait()
	close(stopSampling)
	peak := <-samplerDone
	if peak > limit {
		t.Fatalf("peak in flight %d passed the limit %d", peak, limit)
	}
	if peak < 2 {
		t.Fatalf("peak in flight %d: the test did not create any contention", peak)
	}
	if got := countRows(t, database, `SELECT count(*) FROM deliveries WHERE state = 'succeeded'`); got != total {
		t.Fatalf("got %d succeeded", got)
	}
}

func TestStaleFinishChangesNothingButIsRecorded(t *testing.T) {
	database := openTestStore(t)
	addRoute(t, database, 4)
	mustIngest(t, database, testEvent("evt", "body"))
	ctx := context.Background()

	first := claim(t, database, "old-worker", 1, 10*time.Millisecond)[0]
	time.Sleep(30 * time.Millisecond)
	expired, err := database.ExpireLeases(ctx, 10, 5, time.Millisecond, time.Millisecond)
	if err != nil || expired != 1 {
		t.Fatalf("expired=%d err=%v", expired, err)
	}
	time.Sleep(5 * time.Millisecond)
	second := claim(t, database, "new-worker", 1, time.Minute)[0]
	if second.LeaseToken == first.LeaseToken || second.Attempt != 2 || second.DeliveryKey != first.DeliveryKey {
		t.Fatalf("second claim: %+v", second)
	}

	applied, err := database.FinishAttempt(ctx, first, Finish{NextState: StateSucceeded, Outcome: "success", HTTPStatus: 200}, testBreaker)
	if err != nil {
		t.Fatal(err)
	}
	if applied {
		t.Fatal("the old worker finished a delivery it no longer owns")
	}
	if got := countRows(t, database, `SELECT count(*) FROM deliveries WHERE state = 'leased' AND lease_token = $1::uuid`, second.LeaseToken); got != 1 {
		t.Fatal("a stale finish changed the delivery")
	}
	if got := countRows(t, database, `SELECT count(*) FROM delivery_attempts WHERE stale AND outcome = 'success' AND lease_expired_at IS NOT NULL`); got != 1 {
		t.Fatal("the stale attempt was not recorded")
	}

	applied, err = database.FinishAttempt(ctx, second, Finish{NextState: StateSucceeded, Outcome: "success", HTTPStatus: 200}, testBreaker)
	if err != nil || !applied {
		t.Fatalf("current owner could not finish: applied=%v err=%v", applied, err)
	}
}

func TestFinishingTwiceWithOneTokenAppliesOnce(t *testing.T) {
	database := openTestStore(t)
	addRoute(t, database, 4)
	mustIngest(t, database, testEvent("evt", "body"))
	job := claim(t, database, "worker", 1, time.Minute)[0]
	report := Finish{NextState: StateSucceeded, Outcome: "success", HTTPStatus: 200}
	for index, want := range []bool{true, false} {
		applied, err := database.FinishAttempt(context.Background(), job, report, testBreaker)
		if err != nil || applied != want {
			t.Fatalf("finish %d: applied=%v err=%v", index, applied, err)
		}
	}
}

func TestExpiredLeaseBudgetEndsInDead(t *testing.T) {
	database := openTestStore(t)
	addRoute(t, database, 4)
	mustIngest(t, database, testEvent("evt", "body"))
	ctx := context.Background()

	const maxAttempts = 3
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		var jobs []Job
		for deadline := time.Now().Add(2 * time.Second); len(jobs) == 0 && time.Now().Before(deadline); {
			jobs = claim(t, database, "doomed", 1, time.Millisecond)
		}
		if len(jobs) != 1 || jobs[0].Attempt != attempt {
			t.Fatalf("attempt %d: %+v", attempt, jobs)
		}
		time.Sleep(5 * time.Millisecond)
		if _, err := database.ExpireLeases(ctx, 10, maxAttempts, time.Millisecond, time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}
	if got := countRows(t, database, `SELECT count(*) FROM deliveries WHERE state = 'dead' AND last_error = 'lease expired'`); got != 1 {
		t.Fatal("a delivery that keeps killing workers must end in dead")
	}
	if got := countRows(t, database, `SELECT count(*) FROM delivery_attempts WHERE outcome = 'lease_expired'`); got != maxAttempts {
		t.Fatalf("got %d lease_expired attempts", got)
	}
}

func TestPausedDestinationKeepsItsDeliveries(t *testing.T) {
	database := openTestStore(t)
	destinationID := addRoute(t, database, 4)
	mustIngest(t, database, testEvent("evt", "body"))
	ctx := context.Background()
	if err := database.SetDestinationPaused(ctx, destinationID, true); err != nil {
		t.Fatal(err)
	}
	if jobs := claim(t, database, "worker", 10, time.Minute); len(jobs) != 0 {
		t.Fatal("claimed from a paused destination")
	}
	if err := database.SetDestinationPaused(ctx, destinationID, false); err != nil {
		t.Fatal(err)
	}
	if jobs := claim(t, database, "worker", 10, time.Minute); len(jobs) != 1 {
		t.Fatal("delivery was lost while paused")
	}
}

func TestBreakerStopsAttemptsThenProbesOneAtATime(t *testing.T) {
	database := openTestStore(t)
	addRoute(t, database, 8)
	ingestMany(t, database, "evt", 12)
	ctx := context.Background()
	failure := Finish{NextState: StatePending, Outcome: "connection_error", Error: "refused", CountsFailure: true}

	jobs := claim(t, database, "worker", testBreaker.FailureThreshold, time.Minute)
	if len(jobs) != testBreaker.FailureThreshold {
		t.Fatalf("got %d jobs", len(jobs))
	}
	for _, job := range jobs {
		if _, err := database.FinishAttempt(ctx, job, failure, testBreaker); err != nil {
			t.Fatal(err)
		}
	}
	if got := claim(t, database, "worker", 8, time.Minute); len(got) != 0 {
		t.Fatalf("breaker open but claimed %d", len(got))
	}
	spent := countRows(t, database, `SELECT coalesce(sum(attempt_count), 0)::int FROM deliveries`)
	time.Sleep(testBreaker.MaxDelay + 20*time.Millisecond)

	probe := claim(t, database, "worker", 8, time.Minute)
	if len(probe) != 1 {
		t.Fatalf("half-open claim took %d, want 1 probe", len(probe))
	}
	if got := claim(t, database, "worker", 8, time.Minute); len(got) != 0 {
		t.Fatalf("second claim during the probe took %d", len(got))
	}
	if got := countRows(t, database, `SELECT coalesce(sum(attempt_count), 0)::int FROM deliveries`); got != spent+1 {
		t.Fatalf("attempts spent while open: %d", got-spent)
	}
	success := Finish{NextState: StateSucceeded, Outcome: "success", HTTPStatus: 200}
	if _, err := database.FinishAttempt(ctx, probe[0], success, testBreaker); err != nil {
		t.Fatal(err)
	}
	if got := claim(t, database, "worker", 8, time.Minute); len(got) != 8 {
		t.Fatalf("after a good probe claimed %d, want the full limit of 8", len(got))
	}
}

func TestRetryAfterHoldsTheDestination(t *testing.T) {
	database := openTestStore(t)
	addRoute(t, database, 4)
	ingestMany(t, database, "evt", 3)
	job := claim(t, database, "worker", 1, time.Minute)[0]
	report := Finish{NextState: StatePending, Outcome: "retryable_status", HTTPStatus: 429, CountsFailure: true, RetryAfter: 150 * time.Millisecond, RetryDelay: 150 * time.Millisecond}
	if _, err := database.FinishAttempt(context.Background(), job, report, testBreaker); err != nil {
		t.Fatal(err)
	}
	if got := claim(t, database, "worker", 4, time.Minute); len(got) != 0 {
		t.Fatalf("ignored Retry-After and claimed %d", len(got))
	}
	time.Sleep(170 * time.Millisecond)
	if got := claim(t, database, "worker", 4, time.Minute); len(got) == 0 {
		t.Fatal("destination never came back after Retry-After")
	}
}

func TestRateLimitBoundsClaimsPerSecond(t *testing.T) {
	database := openTestStore(t)
	addSource(t, database, "tenant", "source")
	ctx := context.Background()
	const ratePerSecond = 50
	id, err := database.PutDestination(ctx, Destination{
		TenantID: "tenant", Name: "limited", URL: "http://127.0.0.1:1/", Secret: "s", MaxInFlight: 1000, RatePerSecond: ratePerSecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Subscribe(ctx, "tenant", "source", "*", id); err != nil {
		t.Fatal(err)
	}
	ingestMany(t, database, "evt", 300)
	time.Sleep(time.Second) // fill the bucket, which holds one second of traffic

	started := time.Now()
	claimed := 0
	for time.Since(started) < time.Second {
		claimed += len(claim(t, database, "worker", 100, time.Minute))
		time.Sleep(time.Millisecond)
	}
	// One full bucket plus one second of refill, with a small allowance.
	if claimed > 2*ratePerSecond+5 {
		t.Fatalf("claimed %d in one second at %d per second", claimed, ratePerSecond)
	}
	if claimed < ratePerSecond {
		t.Fatalf("claimed only %d, the limiter is too strict", claimed)
	}
}

// A destination with a deep backlog must not starve one with a single event.
func TestRoundRobinServesEachDestinationInTurn(t *testing.T) {
	database := openTestStore(t)
	addSource(t, database, "tenant", "busy")
	addSource(t, database, "tenant", "quiet")
	ctx := context.Background()
	busy := addDestination(t, database, "tenant", "busy", 4)
	quiet := addDestination(t, database, "tenant", "quiet", 4)
	if err := database.Subscribe(ctx, "tenant", "busy", "*", busy); err != nil {
		t.Fatal(err)
	}
	if err := database.Subscribe(ctx, "tenant", "quiet", "*", quiet); err != nil {
		t.Fatal(err)
	}
	for index := range 50 {
		event := testEvent(fmt.Sprintf("busy-%d", index), "body")
		event.SourceID = "busy"
		mustIngest(t, database, event)
	}
	event := testEvent("quiet-1", "body")
	event.SourceID = "quiet"
	mustIngest(t, database, event)

	seen := map[int64]bool{}
	for range 2 {
		for _, job := range claim(t, database, "worker", 1, time.Minute) {
			seen[job.DestinationID] = true
		}
	}
	if !seen[busy] || !seen[quiet] {
		t.Fatalf("two claims should reach both destinations: %v", seen)
	}
}

func TestReplayKeepsTheKeyAndRefusesUnfinishedWork(t *testing.T) {
	database := openTestStore(t)
	destinationID := addRoute(t, database, 4)
	mustIngest(t, database, testEvent("evt", "body"))
	ctx := context.Background()
	request := ReplayRequest{Operator: "qendrim", Reason: "test", DestinationID: destinationID, State: StateSucceeded, Limit: 10}

	job := claim(t, database, "worker", 1, time.Minute)[0]
	result, err := database.Replay(ctx, ReplayRequest{Operator: "qendrim", Reason: "test", DeliveryKey: job.DeliveryKey, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if result.Requeued != 0 {
		t.Fatal("replay touched a delivery in progress")
	}

	if _, err := database.FinishAttempt(ctx, job, Finish{NextState: StateSucceeded, Outcome: "success", HTTPStatus: 200}, testBreaker); err != nil {
		t.Fatal(err)
	}
	dryRun := request
	dryRun.DryRun = true
	if result, err := database.Replay(ctx, dryRun); err != nil || result.Matched != 1 || result.Requeued != 0 {
		t.Fatalf("dry run: %+v %v", result, err)
	}
	if got := countRows(t, database, `SELECT count(*) FROM deliveries WHERE state = 'succeeded'`); got != 1 {
		t.Fatal("dry run changed the delivery")
	}

	result, err = database.Replay(ctx, request)
	if err != nil || result.Requeued != 1 {
		t.Fatalf("replay: %+v %v", result, err)
	}
	again := claim(t, database, "worker", 1, time.Minute)[0]
	if again.DeliveryKey != job.DeliveryKey {
		t.Fatal("replay changed the delivery key")
	}
	if again.Attempt != 2 || again.BudgetUsed != 1 {
		t.Fatalf("replay should continue attempts and reset the budget: %+v", again)
	}
	if got := countRows(t, database, `SELECT count(*) FROM replays WHERE requeued = 1 AND reason = 'test'`); got != 1 {
		t.Fatal("replay left no audit row")
	}
}

func TestReplayRequiresBounds(t *testing.T) {
	database := openTestStore(t)
	ctx := context.Background()
	valid := ReplayRequest{Operator: "o", Reason: "r", DeliveryKey: "k", Limit: 1}
	broken := []func(*ReplayRequest){
		func(r *ReplayRequest) { r.Operator = "" },
		func(r *ReplayRequest) { r.Reason = "" },
		func(r *ReplayRequest) { r.Limit = 0 },
		func(r *ReplayRequest) { r.Limit = MaxReplayLimit + 1 },
		func(r *ReplayRequest) { r.DeliveryKey = "" },
		func(r *ReplayRequest) { r.DestinationID = 1 },
		func(r *ReplayRequest) { r.DeliveryKey, r.DestinationID, r.State = "", 1, StatePending },
	}
	for index, breakRequest := range broken {
		request := valid
		breakRequest(&request)
		if _, err := database.Replay(ctx, request); err == nil {
			t.Errorf("case %d should be refused", index)
		}
	}
}

func TestPruneKeepsUnfinishedWorkAndKeepsDigestsLonger(t *testing.T) {
	database := openTestStore(t)
	destinationID := addRoute(t, database, 4)
	mustIngest(t, database, testEvent("finished", "body"))
	mustIngest(t, database, testEvent("unfinished", "body"))
	ctx := context.Background()

	job := claim(t, database, "worker", 1, time.Minute)[0]
	if _, err := database.FinishAttempt(ctx, job, Finish{NextState: StateSucceeded, Outcome: "success", HTTPStatus: 200}, testBreaker); err != nil {
		t.Fatal(err)
	}
	finishedKey := job.DeliveryKey
	time.Sleep(10 * time.Millisecond)

	result, err := database.Prune(ctx, time.Millisecond, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if result.PayloadsCleared != 1 || result.EventsDeleted != 0 {
		t.Fatalf("payload pass: %+v", result)
	}
	if got := countRows(t, database, `SELECT count(*) FROM events WHERE payload IS NULL AND payload_sha256 IS NOT NULL`); got != 1 {
		t.Fatal("digest should outlive the payload")
	}
	if duplicate := mustIngest(t, database, testEvent(job.EventID, "body")); duplicate.Status != IngestDuplicate {
		t.Fatalf("a pruned payload must still dedupe, got %s", duplicate.Status)
	}
	if replayed, err := database.Replay(ctx, ReplayRequest{Operator: "o", Reason: "r", DeliveryKey: finishedKey, Limit: 1}); err != nil || replayed.PayloadExpired != 1 || replayed.Requeued != 0 {
		t.Fatalf("replay of a pruned payload: %+v %v", replayed, err)
	}

	result, err = database.Prune(ctx, time.Millisecond, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if result.EventsDeleted != 1 {
		t.Fatalf("identity pass: %+v", result)
	}
	if got := countRows(t, database, `SELECT count(*) FROM deliveries WHERE destination_id = $1 AND state = 'pending'`, destinationID); got != 1 {
		t.Fatal("prune removed unfinished work")
	}
}
