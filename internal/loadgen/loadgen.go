// Package loadgen sends a workload stream to the ingress API.
//
// It is open-loop: each event has a scheduled arrival time, and the schedule does
// not slow down when the system does. Latency counts from that scheduled time.
// A closed-loop generator waits for each response before sending more, so a stall
// hides the requests that should have arrived during it. This is called
// coordinated omission.
package loadgen

import (
	"bufio"
	"bytes"
	"cmp"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qbeka/durable-webhook-delivery/internal/signing"
	"github.com/qbeka/durable-webhook-delivery/internal/workload"
)

// Config controls one run.
type Config struct {
	APIURL          string
	Secret          string
	EventType       string
	Stream          workload.Stream
	DuplicateCount  int           // extra sends of already scheduled events
	MaxInFlight     int           // most requests open at once
	AckDeadline     time.Duration // how long one event may keep retrying
	RawOutputPath   string        // gzip CSV of one row per send, empty to skip
	RequestTimeout  time.Duration // timeout for one HTTP attempt
	RetryBackoff    time.Duration // first wait between attempts
	RetryBackoffCap time.Duration // longest wait between attempts
}

// Summary is what one run produced. Counts are events unless the name says requests.
type Summary struct {
	Stream           workload.Stream `json:"stream"`
	DuplicateSends   int             `json:"duplicate_sends"`
	Offered          int             `json:"offered_sends"`
	Accepted         int64           `json:"accepted"`   // 202
	Duplicates       int64           `json:"duplicates"` // 200
	Conflicts        int64           `json:"conflicts"`  // 409
	Rejected         int64           `json:"rejected"`   // any other final status
	TimedOut         int64           `json:"timed_out"`  // no acknowledgement before AckDeadline
	RetriedRequests  int64           `json:"retried_requests"`
	ElapsedSeconds   float64         `json:"elapsed_seconds"`
	SendLagP50Micros float64         `json:"send_lag_p50_us"` // actual send minus scheduled send
	SendLagP99Micros float64         `json:"send_lag_p99_us"`
	SendLagMaxMicros float64         `json:"send_lag_max_us"`
	UnackedIndexes   []int           `json:"unacked_indexes,omitempty"`
}

type sendResult struct {
	index      int
	duplicate  bool
	lagNanos   int64
	ackedNanos int64 // zero when never acknowledged
	status     int
	retries    int
}

// Run sends the stream and returns when every event is settled.
func Run(ctx context.Context, config Config) (Summary, error) {
	stream := config.Stream
	offsets := stream.ArrivalOffsets()
	start := time.Unix(0, stream.StartUnixNs)

	client := &http.Client{
		Timeout: config.RequestTimeout,
		Transport: &http.Transport{
			MaxIdleConns: config.MaxInFlight, MaxIdleConnsPerHost: config.MaxInFlight,
			DialContext:        (&net.Dialer{Timeout: time.Second}).DialContext,
			DisableCompression: true,
		},
	}
	sends := buildSchedule(config, offsets)

	results := make([]sendResult, len(sends))
	slots := make(chan struct{}, config.MaxInFlight)
	var group sync.WaitGroup
	var retried atomic.Int64
	for sendNumber, scheduled := range sends {
		waitUntil(start.Add(scheduled.at))
		if ctx.Err() != nil {
			break
		}
		slots <- struct{}{} // a full pool shows up as send lag, and is reported
		lagNanos := time.Since(start.Add(scheduled.at)).Nanoseconds()
		group.Add(1)
		go func() {
			defer group.Done()
			defer func() { <-slots }()
			arrival := start.Add(offsets[scheduled.index])
			result := sendWithRetries(ctx, client, config, scheduled.index, arrival, &retried)
			result.lagNanos = lagNanos
			result.duplicate = scheduled.duplicate
			results[sendNumber] = result
		}()
	}
	group.Wait()

	summary := summarize(config, results, retried.Load(), start)
	if config.RawOutputPath != "" {
		if err := writeRaw(config.RawOutputPath, results, start); err != nil {
			return summary, err
		}
	}
	return summary, nil
}

type scheduledSend struct {
	at        time.Duration
	index     int
	duplicate bool
}

// buildSchedule merges first sends with duplicate sends. A duplicate goes out
// shortly after its event, at a seeded offset.
func buildSchedule(config Config, offsets []time.Duration) []scheduledSend {
	sends := make([]scheduledSend, 0, len(offsets)+config.DuplicateCount)
	for index, offset := range offsets {
		sends = append(sends, scheduledSend{at: offset, index: index})
	}
	if config.DuplicateCount == 0 {
		return sends
	}
	final := offsets[len(offsets)-1]
	for _, index := range config.Stream.DuplicateIndexes(config.DuplicateCount) {
		// Up to 250 ms after the original, and never past the end of the schedule.
		delay := time.Duration(config.Stream.AmountCents(index)%250) * time.Millisecond
		sends = append(sends, scheduledSend{at: min(offsets[index]+delay, final), index: index, duplicate: true})
	}
	sortSends(sends)
	return sends
}

func sortSends(sends []scheduledSend) {
	// Stable, so a first send stays ahead of a duplicate with the same time.
	slices.SortStableFunc(sends, func(a, b scheduledSend) int { return cmp.Compare(a.at, b.at) })
}

// waitUntil sleeps most of the way, then spins, because a timer alone can wake
// a millisecond late and that would appear as false latency.
func waitUntil(target time.Time) {
	for {
		remaining := time.Until(target)
		if remaining <= 0 {
			return
		}
		if remaining > 2*time.Millisecond {
			time.Sleep(remaining - time.Millisecond)
			continue
		}
		if remaining > 50*time.Microsecond {
			time.Sleep(0)
		}
	}
}

// sendWithRetries keeps trying with the same event ID until it gets a final
// answer or the deadline passes. Retrying is always safe: that is the contract.
func sendWithRetries(ctx context.Context, client *http.Client, config Config, index int, arrival time.Time, retried *atomic.Int64) sendResult {
	stream := config.Stream
	body := stream.Body(index, arrival.UnixNano())
	digest := signing.BodyDigest(body)
	eventID := stream.EventID(index)
	url := fmt.Sprintf("%s/v1/sources/%s/events", config.APIURL, stream.SourceID)
	deadline := arrival.Add(config.AckDeadline)

	result := sendResult{index: index}
	backoff := config.RetryBackoff
	for {
		status := sendOnce(ctx, client, config, url, eventID, body, digest)
		if status != 0 && status != http.StatusServiceUnavailable && status < 500 {
			result.status = status
			result.ackedNanos = time.Now().UnixNano()
			return result
		}
		result.status = status
		if time.Now().Add(backoff).After(deadline) || ctx.Err() != nil {
			return result
		}
		result.retries++
		retried.Add(1)
		time.Sleep(backoff)
		backoff = min(backoff*2, config.RetryBackoffCap)
	}
}

func sendOnce(ctx context.Context, client *http.Client, config Config, url, eventID string, body, digest []byte) int {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0
	}
	stream := config.Stream
	unixSeconds := time.Now().Unix()
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Hook-Tenant", stream.TenantID)
	request.Header.Set("Hook-Event-Id", eventID)
	request.Header.Set("Hook-Event-Type", config.EventType)
	request.Header.Set("Hook-Timestamp", strconv.FormatInt(unixSeconds, 10))
	request.Header.Set("Hook-Signature", signing.IngressSignature(config.Secret, unixSeconds,
		stream.TenantID, stream.SourceID, eventID, config.EventType, digest))
	response, err := client.Do(request)
	if err != nil {
		return 0
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)
	return response.StatusCode
}

func summarize(config Config, results []sendResult, retried int64, start time.Time) Summary {
	summary := Summary{
		Stream: config.Stream, DuplicateSends: config.DuplicateCount, Offered: len(results),
		RetriedRequests: retried, ElapsedSeconds: time.Since(start).Seconds(),
	}
	acked := make(map[int]bool, len(results))
	lags := make([]float64, 0, len(results))
	for _, result := range results {
		lags = append(lags, float64(result.lagNanos)/1000)
		switch {
		case result.ackedNanos == 0:
			summary.TimedOut++
		case result.status == http.StatusAccepted:
			summary.Accepted++
			acked[result.index] = true
		case result.status == http.StatusOK:
			summary.Duplicates++
			acked[result.index] = true
		case result.status == http.StatusConflict:
			summary.Conflicts++
		default:
			summary.Rejected++
		}
	}
	for index := range config.Stream.EventCount {
		if !acked[index] {
			summary.UnackedIndexes = append(summary.UnackedIndexes, index)
		}
	}
	summary.SendLagP50Micros = Percentile(lags, 50)
	summary.SendLagP99Micros = Percentile(lags, 99)
	summary.SendLagMaxMicros = Percentile(lags, 100)
	return summary
}

func writeRaw(path string, results []sendResult, start time.Time) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	compressed := gzip.NewWriter(file)
	writer := bufio.NewWriter(compressed)
	fmt.Fprintln(writer, "index,duplicate,send_lag_ns,acked_after_start_ns,status,retries")
	for _, result := range results {
		ackedAfterStart := int64(0)
		if result.ackedNanos != 0 {
			ackedAfterStart = result.ackedNanos - start.UnixNano()
		}
		fmt.Fprintf(writer, "%d,%t,%d,%d,%d,%d\n", result.index, result.duplicate, result.lagNanos, ackedAfterStart, result.status, result.retries)
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	return compressed.Close()
}
