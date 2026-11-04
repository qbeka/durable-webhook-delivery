package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qbeka/durable-webhook-delivery/internal/loadgen"
	"github.com/qbeka/durable-webhook-delivery/internal/reconcile"
	"github.com/qbeka/durable-webhook-delivery/internal/workload"
)

// Env is shared by every scenario in a run.
type Env struct {
	Root   string  // repository root
	OutDir string  // where this run's files go
	Scale  float64 // multiplies event counts and fault counts, 1 for a full run
	Seed   uint64
}

// Result is the record of one scenario. It is written before anything reads it,
// and the verdict comes only from the rules in finish.
type Result struct {
	Scenario       string                     `json:"scenario"`
	Fault          string                     `json:"fault"`
	Options        Options                    `json:"options"`
	DeclaredFaults map[string]int             `json:"declared_faults"`
	DeliveredFault map[string]int             `json:"delivered_faults"`
	Load           []loadgen.Summary          `json:"load"`
	DrainDeadlineS float64                    `json:"drain_deadline_seconds"`
	DrainSeconds   float64                    `json:"drain_seconds"`
	Drained        bool                       `json:"drained"`
	Reconcile      reconcile.Report           `json:"reconcile"`
	Latency        map[string]LatencySummary  `json:"latency_ms"`
	Windows        map[string][]LatencyWindow `json:"latency_windows,omitempty"`
	Outcomes       map[string]map[string]int  `json:"receiver_outcomes"`
	Verdict        string                     `json:"verdict"`
	VerdictReason  string                     `json:"verdict_reason"`
	ElapsedSeconds float64                    `json:"elapsed_seconds"`
}

// Spec defines a scenario.
type Spec struct {
	Name           string
	Fault          string
	Options        Options
	Routes         []Route
	EventsPerRoute int
	Rate           float64 // per route
	Duplicates     int     // per route
	DrainDeadline  time.Duration
	Declared       map[string]int
	// Inject runs while load is flowing. It returns what it actually delivered.
	Inject func(ctx context.Context, run *ScenarioRun) (map[string]int, error)
	// ControlExpectsRepeats, when above zero, marks a negative control: the run is
	// valid only if the reconciler reports exactly this many repeated effects.
	ControlExpectsRepeats int
}

// ScenarioRun is what Inject can see.
type ScenarioRun struct {
	Cluster    *Cluster
	Start      time.Time // first scheduled arrival
	LoadDone   <-chan struct{}
	Random     *rand.Rand
	OutDir     string
	WindowData map[string][]LatencyWindow
}

// Run executes one scenario from a clean database to a verdict.
func (e *Env) Run(ctx context.Context, spec Spec) (Result, error) {
	started := time.Now()
	runDir := filepath.Join(e.OutDir, spec.Name)
	if err := os.RemoveAll(runDir); err != nil {
		return Result{}, err
	}
	cluster, err := NewCluster(ctx, e.Root, runDir, spec.Options, spec.Routes)
	if err != nil {
		return Result{}, err
	}
	defer cluster.StopAll()

	startAt := time.Now().Add(1500 * time.Millisecond)
	var runs []*LoadRun
	var streams []workload.Stream
	for index, route := range cluster.Routes {
		plan := LoadPlan{
			Route: route, Seed: e.Seed + uint64(index), Events: spec.EventsPerRoute, Rate: spec.Rate,
			Duplicates: spec.Duplicates, PayloadBytes: 1024, StartUnixNs: startAt.UnixNano(), AckDeadline: 5 * time.Minute,
		}
		load, err := cluster.StartLoad(plan)
		if err != nil {
			return Result{}, err
		}
		runs = append(runs, load)
		streams = append(streams, workload.Stream{
			TenantID: route.Tenant, SourceID: route.Source, Seed: plan.Seed, EventCount: plan.Events,
			RatePerSecond: plan.Rate, PayloadBytes: plan.PayloadBytes, StartUnixNs: plan.StartUnixNs,
		})
	}

	samplerContext, stopSampler := context.WithCancel(ctx)
	samplerDone := make(chan struct{})
	go func() {
		defer close(samplerDone)
		_, _ = sampleQueue(samplerContext, cluster.Store, cluster.Routes, filepath.Join(runDir, "queue-depth.csv"))
	}()

	loadDone := make(chan struct{})
	summaries := make([]loadgen.Summary, len(runs))
	var loadError error
	go func() {
		defer close(loadDone)
		var group sync.WaitGroup
		for index, load := range runs {
			group.Add(1)
			go func() {
				defer group.Done()
				summary, err := load.Wait(ctx)
				if err != nil {
					loadError = err
				}
				summaries[index] = summary
			}()
		}
		group.Wait()
	}()

	run := &ScenarioRun{
		Cluster: cluster, Start: startAt, LoadDone: loadDone, OutDir: runDir,
		Random: rand.New(rand.NewPCG(e.Seed, 0x1234)), WindowData: map[string][]LatencyWindow{},
	}
	delivered := map[string]int{}
	if spec.Inject != nil {
		if delivered, err = spec.Inject(ctx, run); err != nil {
			return Result{}, fmt.Errorf("%s inject: %w", spec.Name, err)
		}
	}
	<-loadDone
	if loadError != nil {
		return Result{}, loadError
	}

	drainSeconds, drained := cluster.WaitForDrain(ctx, spec.DrainDeadline)
	stopSampler()
	<-samplerDone

	expectations := make([]reconcile.Expectation, len(streams))
	for index, stream := range streams {
		expectations[index] = reconcile.Expectation{
			Stream: stream, ExpectEffects: true, UnackedIndexes: summaries[index].UnackedIndexes,
		}
	}
	// Stop the receivers first so the effect logs are complete.
	cluster.StopAll()
	cluster.Store = nil
	report, err := reconcile.Run(ctx, EngineURL, ReceiverURL, expectations)
	if err != nil {
		return Result{}, err
	}

	result := Result{
		Scenario: spec.Name, Fault: spec.Fault, Options: spec.Options, DeclaredFaults: spec.Declared,
		DeliveredFault: delivered, Load: summaries, DrainDeadlineS: spec.DrainDeadline.Seconds(),
		DrainSeconds: drainSeconds, Drained: drained, Reconcile: report,
		Latency: map[string]LatencySummary{}, Outcomes: map[string]map[string]int{}, Windows: run.WindowData,
	}
	for _, route := range cluster.Routes {
		rows, err := ReadEffects(cluster.EffectLogPath(route))
		if err != nil {
			return Result{}, err
		}
		result.Latency[route.Tenant] = SummarizeLatency(rows, time.Time{}, time.Time{})
		result.Outcomes[route.Tenant] = describeOutcomes(rows)
	}
	result.Verdict, result.VerdictReason = verdict(spec, result)
	result.ElapsedSeconds = time.Since(started).Seconds()
	return result, writeJSON(filepath.Join(runDir, "result.json"), result)
}

// verdict is the only place a scenario passes or fails.
func verdict(spec Spec, result Result) (string, string) {
	for name, declared := range spec.Declared {
		if result.DeliveredFault[name] != declared {
			return "invalid", fmt.Sprintf("fault %s delivered %d of %d declared", name, result.DeliveredFault[name], declared)
		}
	}
	if !result.Drained {
		return "fail", "queue did not drain before the deadline"
	}
	if spec.ControlExpectsRepeats > 0 {
		if result.Reconcile.RepeatedEffects != spec.ControlExpectsRepeats {
			return "invalid", fmt.Sprintf("control showed %d repeated effects, expected exactly %d",
				result.Reconcile.RepeatedEffects, spec.ControlExpectsRepeats)
		}
		return "control_detected", fmt.Sprintf("the harness detected all %d planted repeated effects", spec.ControlExpectsRepeats)
	}
	if !result.Reconcile.Pass {
		return "fail", "reconciliation found differences"
	}
	return "pass", "every expected effect exists exactly once"
}

func writeJSON(path string, value any) error {
	content, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(content, '\n'), 0o644)
}

// crashLoop arms workers so each dies at a failpoint, one crash per arming, and
// restarts each immediately. It returns the crashes that actually happened.
func crashLoop(ctx context.Context, run *ScenarioRun, site string, crashes, minHit, maxHit int) (int, error) {
	cluster := run.Cluster
	var remaining, achieved atomic.Int64
	remaining.Store(int64(crashes))
	var group sync.WaitGroup
	var firstError atomic.Value
	for slot := range cluster.Options.Workers {
		group.Add(1)
		go func() {
			defer group.Done()
			for remaining.Add(-1) >= 0 {
				hit := minHit + run.randomInt(maxHit-minHit+1)
				cluster.Worker(slot).Terminate(5 * time.Second)
				if err := cluster.StartWorker(slot, fmt.Sprintf("%s@%d", site, hit)); err != nil {
					firstError.Store(err)
					return
				}
				waitContext, cancel := context.WithTimeout(ctx, 30*time.Second)
				crashed := cluster.Worker(slot).WaitForExit(waitContext)
				cancel()
				if crashed {
					achieved.Add(1)
				}
			}
			// Replace the armed process with a plain worker for the drain.
			if process := cluster.Worker(slot); process.Exited() {
				if err := cluster.StartWorker(slot, ""); err != nil {
					firstError.Store(err)
				}
			} else {
				process.Terminate(5 * time.Second)
				if err := cluster.StartWorker(slot, ""); err != nil {
					firstError.Store(err)
				}
			}
		}()
	}
	group.Wait()
	if err, found := firstError.Load().(error); found {
		return int(achieved.Load()), err
	}
	return int(achieved.Load()), nil
}

var randomMutex sync.Mutex

func (r *ScenarioRun) randomInt(limit int) int {
	randomMutex.Lock()
	defer randomMutex.Unlock()
	return r.Random.IntN(limit)
}

func sleep(ctx context.Context, duration time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(duration):
		return nil
	}
}

func (e *Env) scaled(count int) int {
	return max(1, int(float64(count)*e.Scale))
}
