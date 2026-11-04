// Package worker claims deliveries and sends them to destinations.
package worker

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httptrace"
	"strconv"
	"sync"
	"time"

	"github.com/qbeka/durable-webhook-delivery/internal/failpoint"
	"github.com/qbeka/durable-webhook-delivery/internal/signing"
	"github.com/qbeka/durable-webhook-delivery/internal/store"
)

// minFinishWindow is the least time a worker gets to record an outcome.
const minFinishWindow = 3 * time.Second

// maxFinishBatch is the most outcomes written in one commit.
const maxFinishBatch = 512

// Config holds a worker's settings. Durations are explicit about their unit
// through time.Duration.
type Config struct {
	WorkerID       string
	Concurrency    int           // most requests in flight from this process
	Lease          time.Duration // how long a claim stays valid
	LeaseMargin    time.Duration // a request must end this long before its lease
	RequestTimeout time.Duration // must be less than Lease minus LeaseMargin
	PollInterval   time.Duration // wait after finding no work
	ReapInterval   time.Duration // how often to look for expired leases
	ReapBatch      int           // most leases expired in one pass
	Policy         Policy
	Breaker        store.BreakerConfig
}

// Validate rejects settings under which a healthy request could outlive its lease.
func (c Config) Validate() error {
	if c.Concurrency < 1 {
		return fmt.Errorf("concurrency must be at least 1, got %d", c.Concurrency)
	}
	if c.RequestTimeout+c.LeaseMargin >= c.Lease {
		return fmt.Errorf("request timeout %v plus margin %v must be less than lease %v",
			c.RequestTimeout, c.LeaseMargin, c.Lease)
	}
	return nil
}

// Worker runs the claim and delivery loop.
type Worker struct {
	config      Config
	store       *store.Store
	transport   *http.Transport
	completions chan completion
}

// New builds a worker.
func New(config Config, database *store.Store) (*Worker, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = config.Concurrency * 2
	transport.MaxIdleConnsPerHost = config.Concurrency
	transport.DisableCompression = true
	return &Worker{
		config:      config,
		store:       database,
		completions: make(chan completion, config.Concurrency),
		transport:   transport,
	}, nil
}

// Run claims and delivers until ctx ends, then waits for requests in flight.
func (w *Worker) Run(ctx context.Context) {
	var inFlight sync.WaitGroup
	reaperDone := make(chan struct{})
	go func() {
		defer close(reaperDone)
		w.reapLoop(ctx)
	}()
	finisherDone := make(chan struct{})
	go func() {
		defer close(finisherDone)
		w.finishLoop()
	}()

	slots := make(chan struct{}, w.config.Concurrency)
	for ctx.Err() == nil {
		free := cap(slots) - len(slots)
		if free == 0 {
			// Wait for one request to finish, then give its slot straight back.
			select {
			case slots <- struct{}{}:
				<-slots
			case <-ctx.Done():
			}
			continue
		}

		// The clock starts before the claim, so the local deadline is earlier
		// than the one in the database. No clock agreement is needed.
		claimedAt := time.Now()
		jobs, err := w.store.Claim(ctx, w.config.WorkerID, free, w.config.Lease, w.config.Breaker)
		if err != nil {
			if ctx.Err() == nil {
				slog.Warn("claim failed", "worker", w.config.WorkerID, "error", err)
				sleepWithJitter(ctx, 200*time.Millisecond)
			}
			continue
		}
		if len(jobs) == 0 {
			sleepWithJitter(ctx, w.config.PollInterval)
			continue
		}
		for _, job := range jobs {
			slots <- struct{}{}
			inFlight.Add(1)
			go func() {
				defer inFlight.Done()
				defer func() { <-slots }()
				w.deliver(job, claimedAt)
			}()
		}
	}
	inFlight.Wait()
	close(w.completions)
	<-finisherDone
	<-reaperDone
}

func (w *Worker) reapLoop(ctx context.Context) {
	ticker := time.NewTicker(w.config.ReapInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		// Keep going while a full batch came back, so a dead worker's leases
		// clear in one pass rather than one batch per tick.
		for {
			expired, err := w.store.ExpireLeases(ctx, w.config.ReapBatch, w.config.Policy.MaxAttempts,
				w.config.Policy.MaxAge, w.config.Policy.RetryBase, w.config.Policy.RetryCap)
			if err != nil {
				if ctx.Err() == nil {
					slog.Warn("lease expiry failed", "worker", w.config.WorkerID, "error", err)
				}
				break
			}
			if expired < w.config.ReapBatch {
				break
			}
		}
	}
}

// deliver sends one claimed job and records the outcome.
//
// The lease is committed before this runs. If the process dies at any line
// here, the lease expires and another worker sends the same delivery key.
func (w *Worker) deliver(job store.Job, claimedAt time.Time) {
	leaseDeadline := claimedAt.Add(w.config.Lease - w.config.LeaseMargin)
	if time.Now().After(leaseDeadline) {
		// Claiming took too long to send safely. Leave the lease to expire.
		return
	}
	failpoint.Hit("before_send")

	requestDeadline := time.Now().Add(w.config.RequestTimeout)
	if leaseDeadline.Before(requestDeadline) {
		requestDeadline = leaseDeadline
	}
	ctx, cancel := context.WithDeadline(context.Background(), requestDeadline)
	defer cancel()

	status, retryAfter, requestErr := w.send(ctx, job)
	failpoint.Hit("after_response")

	report := classify(w.config.Policy, job.BudgetUsed, job.BudgetAge, status, requestErr, retryAfter)
	w.completions <- completion{
		Completion: store.Completion{Job: job, Report: report},
		giveUpAt:   giveUpTime(leaseDeadline.Add(w.config.LeaseMargin)),
	}
}

// send makes one signed POST and returns the status and Retry-After header.
func (w *Worker) send(ctx context.Context, job store.Job) (status int, retryAfter string, err error) {
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		WroteRequest: func(httptrace.WroteRequestInfo) { failpoint.Hit("request_written") },
	})
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, job.URL, bytes.NewReader(job.Payload))
	if err != nil {
		return 0, "", err
	}
	unixSeconds := time.Now().Unix()
	header := request.Header
	header.Set("Content-Type", "application/json")
	header.Set("Hook-Delivery-Key", job.DeliveryKey)
	header.Set("Hook-Attempt", strconv.Itoa(job.Attempt))
	header.Set("Hook-Tenant", job.TenantID)
	header.Set("Hook-Source", job.SourceID)
	header.Set("Hook-Event-Id", job.EventID)
	header.Set("Hook-Event-Type", job.EventType)
	header.Set("Hook-Payload-Sha256", hex.EncodeToString(job.PayloadSHA256))
	header.Set("Hook-Timestamp", strconv.FormatInt(unixSeconds, 10))
	header.Set("Hook-Signature", signing.DeliverySignature(job.Secret, unixSeconds, job.DeliveryKey,
		job.TenantID, job.SourceID, job.EventID, job.EventType, job.PayloadSHA256))

	// RoundTrip, not http.Client: a client follows redirects, and a signed request
	// must not go to an address nobody registered. A client also treats a 3xx with
	// a broken Location as a transport error, which would make a terminal answer
	// look retryable.
	response, err := w.transport.RoundTrip(request)
	if err != nil {
		return 0, "", err
	}
	defer response.Body.Close()
	// Draining a bounded amount lets the connection be reused.
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64*1024))
	return response.StatusCode, response.Header.Get("Retry-After"), nil
}

// completion is one outcome waiting to be written.
type completion struct {
	store.Completion
	giveUpAt time.Time
}

// giveUpTime is when a worker stops trying to record an outcome. It is at least
// minFinishWindow from now even when the lease has already ended. A worker paused
// past its lease still has something true to record: the attempt history keeps its
// report, marked stale, and the delivery does not change.
func giveUpTime(leaseExpiry time.Time) time.Time {
	if earliest := time.Now().Add(minFinishWindow); leaseExpiry.Before(earliest) {
		return earliest
	}
	return leaseExpiry
}

// finishLoop writes outcomes in batches until the channel closes.
//
// There is no timer. A batch is whatever arrived while the last commit was
// running, so light traffic commits one report at once, and heavy traffic gets
// larger batches on its own.
func (w *Worker) finishLoop() {
	for first := range w.completions {
		batch := []completion{first}
	collect:
		for len(batch) < maxFinishBatch {
			select {
			case next, open := <-w.completions:
				if !open {
					break collect
				}
				batch = append(batch, next)
			default:
				break collect
			}
		}
		w.writeBatch(batch)
	}
}

// writeBatch retries through a brief database failure. Giving up is safe: the
// leases expire, the deliveries are sent again, and the receiver dedupes them.
func (w *Worker) writeBatch(batch []completion) {
	completions := make([]store.Completion, len(batch))
	giveUpAt := batch[0].giveUpAt
	for index, item := range batch {
		completions[index] = item.Completion
		if item.giveUpAt.After(giveUpAt) {
			giveUpAt = item.giveUpAt
		}
	}
	for {
		ctx, cancel := context.WithDeadline(context.Background(), giveUpAt)
		_, err := w.store.FinishAttempts(ctx, completions, w.config.Breaker)
		cancel()
		if err == nil {
			return
		}
		if time.Now().After(giveUpAt) {
			slog.Warn("could not record outcomes", "worker", w.config.WorkerID, "count", len(batch), "error", err)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func sleepWithJitter(ctx context.Context, interval time.Duration) {
	// Between 75% and 125% of the interval, so idle workers do not poll in step.
	timer := time.NewTimer(time.Duration((0.75 + rand.Float64()/2) * float64(interval)))
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
