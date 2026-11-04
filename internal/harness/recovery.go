package harness

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/qbeka/durable-webhook-delivery/internal/receiver"
)

// RecoveryOptions are service timings for outage tests. They are shorter than
// BenchOptions, so an outage of a minute spans many retry and breaker cycles.
func RecoveryOptions(maxInFlight, breakerThreshold int) Options {
	return Options{
		Workers: 2, WorkerConcurrency: 64, WorkerMaxConnections: 16,
		Lease: 10 * time.Second, LeaseMargin: time.Second, RequestTimeout: 2 * time.Second,
		PollInterval: 10 * time.Millisecond, ReapInterval: time.Second,
		MaxAttempts: 200, RetryBase: 200 * time.Millisecond, RetryCap: 5 * time.Second,
		BreakerThreshold: breakerThreshold, BreakerBase: 500 * time.Millisecond, BreakerCap: 5 * time.Second,
		MaxInFlight: maxInFlight, ReceiverConnections: 64,
	}
}

// RecoveryConfig is one outage experiment.
type RecoveryConfig struct {
	Name        string        `json:"name"`
	Mode        string        `json:"mode"` // offline: connection refused. slow: accepted, then past the timeout.
	Rate        float64       `json:"events_per_second_per_tenant"`
	Lead        time.Duration `json:"lead_ns"`   // normal traffic before the outage
	Outage      time.Duration `json:"outage_ns"` // length of the outage
	Trail       time.Duration `json:"trail_ns"`  // traffic after the outage
	MaxInFlight int           `json:"max_in_flight"`
	Breaker     int           `json:"breaker_threshold"`
	Seed        uint64        `json:"seed"`
}

// RecoveryResult compares the healthy tenant across the outage. Before, during
// and after refer to when an event arrived.
type RecoveryResult struct {
	Config               RecoveryConfig  `json:"config"`
	Repetition           int             `json:"repetition"`
	Verdict              string          `json:"verdict"`
	HealthyBefore        LatencySummary  `json:"healthy_before_ms"`
	HealthyDuring        LatencySummary  `json:"healthy_during_ms"`
	HealthyAfter         LatencySummary  `json:"healthy_after_ms"`
	FailingAfterRecovery LatencySummary  `json:"failing_destination_all_ms"`
	PeakBacklog          int64           `json:"peak_backlog_failing_destination"`
	BacklogAtOutageEnd   int64           `json:"backlog_at_outage_end"`
	DrainAfterOutageSec  float64         `json:"seconds_to_drain_after_outage"`
	Timeline             []TimelinePoint `json:"timeline"`
	Machine              GateResult      `json:"machine"`
	Scenario             Result          `json:"scenario"`
}

// TimelinePoint is one second of the outage chart.
type TimelinePoint struct {
	OffsetSeconds  float64 `json:"offset_seconds"`
	HealthyPending int64   `json:"healthy_queue"`
	FailingPending int64   `json:"failing_queue"`
	HealthyP50Ms   float64 `json:"healthy_p50_ms"`
	HealthyP99Ms   float64 `json:"healthy_p99_ms"`
	HealthyCount   int     `json:"healthy_count"`
}

// RunRecovery runs one outage and measures the healthy tenant around it.
func RunRecovery(ctx context.Context, root, outDir string, config RecoveryConfig, repetition int, gate QuietGate) (RecoveryResult, error) {
	machine := gate.Wait(ctx)
	total := config.Lead + config.Outage + config.Trail
	name := fmt.Sprintf("%s/rep-%d", config.Name, repetition)
	env := &Env{Root: root, OutDir: outDir, Scale: 1, Seed: config.Seed + uint64(repetition)}
	spec := Spec{
		Name: name, Fault: fmt.Sprintf("destination %s for %v", config.Mode, config.Outage),
		Options:        RecoveryOptions(config.MaxInFlight, config.Breaker),
		Routes:         []Route{HealthyRoute(receiver.ModeIdempotent), FlakyRoute(receiver.ModeIdempotent)},
		EventsPerRoute: int(config.Rate * total.Seconds()), Rate: config.Rate,
		DrainDeadline: 5 * time.Minute, Declared: map[string]int{"outages": 1},
		Inject: outageInjector(config),
	}
	scenario, err := env.Run(ctx, spec)
	if err != nil {
		return RecoveryResult{}, err
	}
	runDir := filepath.Join(outDir, name)
	result, err := analyzeRecovery(runDir, config, repetition, scenario)
	if err != nil {
		return RecoveryResult{}, err
	}
	result.Machine = machine
	for _, tenant := range []string{"healthy", "flaky"} {
		if err := compressEffects(filepath.Join(runDir, "effects-"+tenant+".csv")); err != nil {
			return RecoveryResult{}, err
		}
	}
	return result, writeJSON(filepath.Join(runDir, "recovery.json"), result)
}

func outageInjector(config RecoveryConfig) func(context.Context, *ScenarioRun) (map[string]int, error) {
	return func(ctx context.Context, run *ScenarioRun) (map[string]int, error) {
		cluster := run.Cluster
		flaky := cluster.Routes[1]
		if err := sleep(ctx, time.Until(run.Start.Add(config.Lead))); err != nil {
			return nil, err
		}
		outageStart := time.Now()
		switch config.Mode {
		case "offline":
			cluster.KillReceiver(flaky)
		case "slow":
			// Held past the request timeout, so every attempt ties up a worker slot.
			plan := receiver.FaultPlan{DelayCount: 1 << 40, DelayBeforeCommit: cluster.Options.RequestTimeout + time.Second}
			if err := cluster.SetFaults(flaky, plan); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unknown outage mode %q", config.Mode)
		}
		if err := sleep(ctx, config.Outage); err != nil {
			return nil, err
		}
		outageEnd := time.Now()
		if config.Mode == "offline" {
			if err := cluster.StartReceiver(ctx, flaky); err != nil {
				return nil, err
			}
		} else if err := cluster.SetFaults(flaky, receiver.FaultPlan{}); err != nil {
			return nil, err
		}
		err := writeJSON(filepath.Join(run.OutDir, "outage.json"), outageTimes{
			run.Start.UnixNano(), outageStart.UnixNano(), outageEnd.UnixNano(),
		})
		if err != nil {
			return nil, err
		}
		return map[string]int{"outages": 1}, nil
	}
}

type outageTimes struct {
	ScheduleStartUnixNs int64 `json:"schedule_start_unix_ns"`
	OutageStartUnixNs   int64 `json:"outage_start_unix_ns"`
	OutageEndUnixNs     int64 `json:"outage_end_unix_ns"`
}

func analyzeRecovery(runDir string, config RecoveryConfig, repetition int, scenario Result) (RecoveryResult, error) {
	content, err := os.ReadFile(filepath.Join(runDir, "outage.json"))
	if err != nil {
		return RecoveryResult{}, err
	}
	var times outageTimes
	if err := json.Unmarshal(content, &times); err != nil {
		return RecoveryResult{}, err
	}
	start := time.Unix(0, times.ScheduleStartUnixNs)
	outageStart, outageEnd := time.Unix(0, times.OutageStartUnixNs), time.Unix(0, times.OutageEndUnixNs)
	healthy, err := ReadEffects(filepath.Join(runDir, "effects-healthy.csv"))
	if err != nil {
		return RecoveryResult{}, err
	}
	failing, err := ReadEffects(filepath.Join(runDir, "effects-flaky.csv"))
	if err != nil {
		return RecoveryResult{}, err
	}
	result := RecoveryResult{
		Config: config, Repetition: repetition, Verdict: scenario.Verdict, Scenario: scenario,
		HealthyBefore:        SummarizeLatency(healthy, start, outageStart),
		HealthyDuring:        SummarizeLatency(healthy, outageStart, outageEnd),
		HealthyAfter:         SummarizeLatency(healthy, outageEnd, time.Time{}),
		FailingAfterRecovery: SummarizeLatency(failing, time.Time{}, time.Time{}),
	}
	points, err := readTimeline(filepath.Join(runDir, "queue-depth.csv"), start)
	if err != nil {
		return RecoveryResult{}, err
	}
	windows := LatencyOverTime(healthy, start, time.Second, len(points)+2)
	for index := range points {
		second := int(points[index].OffsetSeconds)
		if second >= 0 && second < len(windows) {
			points[index].HealthyP50Ms = windows[second].P50Ms
			points[index].HealthyP99Ms = windows[second].P99Ms
			points[index].HealthyCount = windows[second].Count
		}
		result.PeakBacklog = max(result.PeakBacklog, points[index].FailingPending)
	}
	result.Timeline = points
	outageEndOffset := outageEnd.Sub(start).Seconds()
	result.DrainAfterOutageSec = -1 // stays -1 if the backlog never reached zero
	for _, point := range points {
		if point.OffsetSeconds <= outageEndOffset {
			result.BacklogAtOutageEnd = point.FailingPending
			continue
		}
		if point.FailingPending == 0 {
			result.DrainAfterOutageSec = point.OffsetSeconds - outageEndOffset
			break
		}
	}
	return result, nil
}

// readTimeline turns the queue samples into one point per second. Destination 1
// is the healthy route, because it registers first.
func readTimeline(path string, start time.Time) ([]TimelinePoint, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	reader := csv.NewReader(file)
	if _, err := reader.Read(); err != nil {
		return nil, err
	}
	byTime := map[int64]*TimelinePoint{}
	var order []int64
	for {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		unixMs, _ := strconv.ParseInt(record[0], 10, 64)
		pending, _ := strconv.ParseInt(record[2], 10, 64)
		leased, _ := strconv.ParseInt(record[3], 10, 64)
		point := byTime[unixMs]
		if point == nil {
			point = &TimelinePoint{OffsetSeconds: time.UnixMilli(unixMs).Sub(start).Seconds()}
			byTime[unixMs] = point
			order = append(order, unixMs)
		}
		if record[1] == "1" {
			point.HealthyPending = pending + leased
		} else {
			point.FailingPending = pending + leased
		}
	}
	points := make([]TimelinePoint, 0, len(order))
	for _, unixMs := range order {
		points = append(points, *byTime[unixMs])
	}
	return points, nil
}

// GeneratorResult is the generator measured against a null sink.
type GeneratorResult struct {
	Rate         float64 `json:"rate_per_second"`
	Events       int     `json:"events"`
	Accepted     int64   `json:"accepted"`
	SendLagP50Us float64 `json:"send_lag_p50_us"`
	SendLagP99Us float64 `json:"send_lag_p99_us"`
	SendLagMaxUs float64 `json:"send_lag_max_us"`
	AchievedRate float64 `json:"achieved_rate_per_second"`
}

// MeasureGenerator finds the generator's own limit, with no service behind it.
// Without this, a service benchmark cannot tell a slow service from a slow client.
func MeasureGenerator(ctx context.Context, root, outDir string, rate float64, duration time.Duration, seed uint64) (GeneratorResult, error) {
	runDir := filepath.Join(outDir, fmt.Sprintf("r%.0f", rate))
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return GeneratorResult{}, err
	}
	cluster := &Cluster{Root: root, RunDir: runDir}
	sink, err := startProcess(runDir, "null-sink", cluster.binary("loadgen"), []string{"--null-sink", apiAddress}, nil)
	if err != nil {
		return GeneratorResult{}, err
	}
	defer sink.Kill()
	if err := sleep(ctx, 300*time.Millisecond); err != nil {
		return GeneratorResult{}, err
	}
	events := int(rate * duration.Seconds())
	load, err := cluster.StartLoad(LoadPlan{
		Route: HealthyRoute(receiver.ModeIdempotent), Seed: seed, Events: events, Rate: rate, PayloadBytes: 1024,
		StartUnixNs: time.Now().Add(time.Second).UnixNano(), AckDeadline: time.Minute,
	})
	if err != nil {
		return GeneratorResult{}, err
	}
	summary, err := load.Wait(ctx)
	if err != nil {
		return GeneratorResult{}, err
	}
	return GeneratorResult{
		Rate: rate, Events: events, Accepted: summary.Accepted,
		SendLagP50Us: summary.SendLagP50Micros, SendLagP99Us: summary.SendLagP99Micros, SendLagMaxUs: summary.SendLagMaxMicros,
		AchievedRate: float64(summary.Accepted) / summary.ElapsedSeconds,
	}, nil
}
