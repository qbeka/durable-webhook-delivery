package harness

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/qbeka/durable-webhook-delivery/internal/loadgen"
	"github.com/qbeka/durable-webhook-delivery/internal/receiver"
	"github.com/qbeka/durable-webhook-delivery/internal/reconcile"
	"github.com/qbeka/durable-webhook-delivery/internal/workload"
)

// BenchOptions are the service timings for benchmarks: the production defaults,
// not the short timings the fault suite uses.
func BenchOptions(workers, concurrency, maxInFlight int) Options {
	return Options{
		Workers: workers, WorkerConcurrency: concurrency, WorkerMaxConnections: 32,
		Lease: 30 * time.Second, LeaseMargin: 2 * time.Second, RequestTimeout: 10 * time.Second,
		PollInterval: 10 * time.Millisecond, ReapInterval: time.Second,
		MaxAttempts: 12, RetryBase: time.Second, RetryCap: 5 * time.Minute,
		BreakerThreshold: 5, BreakerBase: time.Second, BreakerCap: 30 * time.Second,
		MaxInFlight: maxInFlight, ReceiverConnections: 64,
	}
}

// BenchConfig is one published configuration.
type BenchConfig struct {
	Name          string        `json:"name"`
	Rate          float64       `json:"offered_events_per_second"`
	Duration      time.Duration `json:"duration_ns"`
	Warmup        time.Duration `json:"warmup_ns"` // excluded from rates and latency
	PayloadBytes  int           `json:"payload_bytes"`
	Fanout        int           `json:"fanout"`
	DrainDeadline time.Duration `json:"drain_deadline_ns"`
	Seed          uint64        `json:"seed"`
	Options       Options       `json:"options"`
}

// BenchResult is one repetition. The denominators are OfferedEvents for error
// rates and Latency.Count for percentiles.
type BenchResult struct {
	Config             BenchConfig      `json:"config"`
	Repetition         int              `json:"repetition"`
	OfferedEvents      int              `json:"offered_events"`
	OfferedRate        float64          `json:"offered_rate_per_second"`
	AcceptedRate       float64          `json:"accepted_rate_per_second"`
	CompletedRate      float64          `json:"completed_rate_per_second"`
	ErrorRate          float64          `json:"error_rate"`
	TimedOutEvents     int64            `json:"timed_out_events"`
	RejectedEvents     int64            `json:"rejected_events"`
	RetriedRequests    int64            `json:"retried_requests"`
	BacklogSlopePerSec float64          `json:"backlog_slope_per_second"`
	BacklogAtEnd       int64            `json:"backlog_at_end"`
	Latency            LatencySummary   `json:"arrival_to_effect_ms"`
	AckLatency         LatencySummary   `json:"arrival_to_ack_ms"`
	DrainSeconds       float64          `json:"drain_seconds"`
	Drained            bool             `json:"drained"`
	Reconcile          reconcile.Report `json:"reconcile"`
	Generator          loadgen.Summary  `json:"generator"`
	ElapsedSeconds     float64          `json:"elapsed_seconds"`
	Machine            GateResult       `json:"machine"`
}

// RunBench runs one repetition from a clean database, then keeps its raw data.
func RunBench(ctx context.Context, root, outDir string, config BenchConfig, repetition int, gate QuietGate) (BenchResult, error) {
	machine := gate.Wait(ctx)
	started := time.Now()
	runDir := filepath.Join(outDir, config.Name, fmt.Sprintf("rep-%d", repetition))
	if err := os.RemoveAll(runDir); err != nil {
		return BenchResult{}, err
	}
	route := HealthyRoute(receiver.ModeIdempotent)
	cluster, err := NewCluster(ctx, root, runDir, config.Options, []Route{route})
	if err != nil {
		return BenchResult{}, err
	}
	defer cluster.StopAll()
	route = cluster.Routes[0]

	seed := config.Seed + uint64(repetition)
	events := int(config.Rate * config.Duration.Seconds())
	startAt := time.Now().Add(1500 * time.Millisecond)
	plan := LoadPlan{
		Route: route, Seed: seed, Events: events, Rate: config.Rate, PayloadBytes: config.PayloadBytes,
		StartUnixNs: startAt.UnixNano(), AckDeadline: 2 * time.Minute,
	}
	load, err := cluster.StartLoad(plan)
	if err != nil {
		return BenchResult{}, err
	}

	samplerContext, stopSampler := context.WithCancel(ctx)
	defer stopSampler()
	type sampled struct{ samples []DepthSample }
	samplerDone := make(chan sampled, 1)
	go func() {
		samples, _ := sampleQueue(samplerContext, cluster.Store, cluster.Routes, filepath.Join(runDir, "queue-depth.csv"))
		samplerDone <- sampled{samples}
	}()

	waitContext, cancel := context.WithTimeout(ctx, config.Duration+5*time.Minute)
	defer cancel()
	summary, err := load.Wait(waitContext)
	if err != nil {
		return BenchResult{}, err
	}
	scheduleEnd := startAt.Add(config.Duration)

	drainSeconds, drained := cluster.WaitForDrain(ctx, config.DrainDeadline)
	stopSampler()
	samples := (<-samplerDone).samples

	cluster.StopAll()
	cluster.Store = nil
	stream := workload.Stream{
		TenantID: route.Tenant, SourceID: route.Source, Seed: seed, EventCount: events,
		RatePerSecond: config.Rate, PayloadBytes: config.PayloadBytes, StartUnixNs: plan.StartUnixNs,
	}
	report, err := reconcile.Run(ctx, config.Options.engineURL(), ReceiverURL, []reconcile.Expectation{
		{Stream: stream, ExpectEffects: true, UnackedIndexes: summary.UnackedIndexes},
	})
	if err != nil {
		return BenchResult{}, err
	}

	rows, err := ReadEffects(cluster.EffectLogPath(route))
	if err != nil {
		return BenchResult{}, err
	}
	windowStart := startAt.Add(config.Warmup)
	windowSeconds := scheduleEnd.Sub(windowStart).Seconds()
	result := BenchResult{
		Config: config, Repetition: repetition, OfferedEvents: summary.Offered,
		OfferedRate:    float64(summary.Offered) / config.Duration.Seconds(),
		TimedOutEvents: summary.TimedOut, RejectedEvents: summary.Rejected + summary.Conflicts,
		RetriedRequests: summary.RetriedRequests,
		Latency:         SummarizeLatency(rows, windowStart, scheduleEnd),
		DrainSeconds:    drainSeconds, Drained: drained, Reconcile: report, Generator: summary, Machine: machine,
	}
	if summary.Offered > 0 {
		result.ErrorRate = float64(summary.TimedOut+summary.Rejected+summary.Conflicts) / float64(summary.Offered)
	}
	completed := 0
	for _, row := range rows {
		if row.Outcome == "applied" && row.TimeUnixNs >= windowStart.UnixNano() && row.TimeUnixNs < scheduleEnd.UnixNano() {
			completed++
		}
	}
	result.CompletedRate = float64(completed) / windowSeconds
	result.BacklogSlopePerSec, result.BacklogAtEnd = backlogTrend(samples, windowStart, scheduleEnd)

	ackPath := filepath.Join(runDir, fmt.Sprintf("loadgen-%s-%d.sends.csv.gz", route.Tenant, seed))
	result.AckLatency, result.AcceptedRate, err = readAckLatency(ackPath, stream, startAt, config.Warmup, config.Duration)
	if err != nil {
		return BenchResult{}, err
	}
	if err := compressEffects(cluster.EffectLogPath(route)); err != nil {
		return BenchResult{}, err
	}
	result.ElapsedSeconds = time.Since(started).Seconds()
	return result, writeJSON(filepath.Join(runDir, "result.json"), result)
}

// backlogTrend fits a least-squares line to unfinished deliveries over time and
// returns its slope. A stable system has a slope near zero. The slope is what
// decides whether a rate can be sustained: a p99 can look fine for a short run
// while the queue is quietly growing.
func backlogTrend(samples []DepthSample, from, to time.Time) (slopePerSecond float64, final int64) {
	var count, sumX, sumY, sumXX, sumXY float64
	for _, sample := range samples {
		if sample.Time.Before(from) || sample.Time.After(to) {
			continue
		}
		x := sample.Time.Sub(from).Seconds()
		y := float64(sample.Pending + sample.Leased)
		count++
		sumX += x
		sumY += y
		sumXX += x * x
		sumXY += x * y
		final = sample.Pending + sample.Leased
	}
	denominator := count*sumXX - sumX*sumX
	if count < 2 || denominator == 0 {
		return 0, final
	}
	return (count*sumXY - sumX*sumY) / denominator, final
}

// readAckLatency reads the generator's raw file and measures arrival to ingress
// acknowledgement. Rates count acknowledgements inside the steady window.
func readAckLatency(path string, stream workload.Stream, start time.Time, warmup, duration time.Duration) (LatencySummary, float64, error) {
	file, err := os.Open(path)
	if err != nil {
		return LatencySummary{}, 0, err
	}
	defer file.Close()
	decompressed, err := gzip.NewReader(file)
	if err != nil {
		return LatencySummary{}, 0, err
	}
	reader := csv.NewReader(decompressed)
	if _, err := reader.Read(); err != nil {
		return LatencySummary{}, 0, err
	}
	offsets := stream.ArrivalOffsets()
	var latencies []float64
	accepted := 0
	for {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return LatencySummary{}, 0, err
		}
		index, _ := strconv.Atoi(record[0])
		ackedAfterStart, _ := strconv.ParseInt(record[3], 10, 64)
		if ackedAfterStart == 0 {
			continue
		}
		acked := time.Duration(ackedAfterStart)
		if acked >= warmup && acked < duration {
			accepted++
		}
		if offsets[index] >= warmup {
			latencies = append(latencies, (acked-offsets[index]).Seconds()*1000)
		}
	}
	summary := LatencySummary{
		Count: len(latencies),
		P50Ms: loadgen.Percentile(latencies, 50), P95Ms: loadgen.Percentile(latencies, 95),
		P99Ms: loadgen.Percentile(latencies, 99), MaxMs: loadgen.Percentile(latencies, 100),
	}
	return summary, float64(accepted) / (duration - warmup).Seconds(), nil
}

// compressEffects replaces an effect log with a gzip copy that keeps only what
// the latency calculation needs, so raw data stays small enough to commit.
func compressEffects(path string) error {
	rows, err := ReadEffects(path)
	if err != nil {
		return err
	}
	file, err := os.Create(strings.TrimSuffix(path, ".csv") + ".latency.csv.gz")
	if err != nil {
		return err
	}
	defer file.Close()
	compressed := gzip.NewWriter(file)
	writer := bufio.NewWriter(compressed)
	fmt.Fprintln(writer, "arrival_unix_ns,latency_us,attempt,outcome")
	for _, row := range rows {
		fmt.Fprintf(writer, "%d,%d,%d,%s\n", row.ArrivalUnixNs, (row.TimeUnixNs-row.ArrivalUnixNs)/1000, row.Attempt, row.Outcome)
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	if err := compressed.Close(); err != nil {
		return err
	}
	return os.Remove(path)
}
