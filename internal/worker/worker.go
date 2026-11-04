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
	config Config
	store  *store.Store
	client *http.Client
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
		config: config,
		store:  database,
		client: &http.Client{
			Transport: transport,
			// A signed request must not follow a redirect to an unregistered URL.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
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
				w.config.Policy.RetryBase, w.config.Policy.RetryCap)
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

	report := classify(w.config.Policy, job.BudgetUsed, status, requestErr, retryAfter)
	w.finish(job, report, leaseDeadline.Add(w.config.LeaseMargin))
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
	header.Set("Hook-Signature", signing.DeliverySignature(job.Secret, unixSeconds, job.DeliveryKey, job.PayloadSHA256))

	response, err := w.client.Do(request)
	if err != nil {
		return 0, "", err
	}
	defer response.Body.Close()
	// Draining a bounded amount lets the connection be reused.
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64*1024))
	return response.StatusCode, response.Header.Get("Retry-After"), nil
}

// finish reports an outcome, retrying through a brief database failure. It stops
// at the lease expiry, because after that the report would only be stale.
func (w *Worker) finish(job store.Job, report store.Finish, leaseExpiry time.Time) {
	for {
		ctx, cancel := context.WithDeadline(context.Background(), leaseExpiry)
		applied, err := w.store.FinishAttempt(ctx, job, report, w.config.Breaker)
		cancel()
		if err == nil {
			if !applied {
				slog.Info("stale finish ignored", "worker", w.config.WorkerID, "delivery", job.DeliveryKey)
			}
			return
		}
		if time.Now().After(leaseExpiry) {
			slog.Warn("could not record outcome before the lease ended",
				"worker", w.config.WorkerID, "delivery", job.DeliveryKey, "outcome", report.Outcome, "error", err)
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
