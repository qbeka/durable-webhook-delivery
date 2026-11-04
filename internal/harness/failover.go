package harness

import (
	"compress/gzip"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"time"

	"github.com/qbeka/durable-webhook-delivery/internal/receiver"
)

// highAvailabilityURL lists both servers. The driver tries each and keeps the one
// that accepts writes, so clients follow a promotion with no special code.
const highAvailabilityURL = "postgres://hook@127.0.0.1:54340,127.0.0.1:54341/hookd?target_session_attrs=read-write"

// FailoverConfig is the experiment in docs/preregistration-failover.md.
type FailoverConfig struct {
	Mode      string        `json:"mode"` // sync or async replication
	Rate      float64       `json:"events_per_second"`
	CrashAt   time.Duration `json:"crash_at_ns"`
	Duration  time.Duration `json:"duration_ns"`
	Freeze    time.Duration `json:"standby_freeze_ns"`
	Detection time.Duration `json:"declared_detection_ns"` // a constant, not a measurement
	Seed      uint64        `json:"seed"`
}

// FailoverResult is one repetition.
type FailoverResult struct {
	Config             FailoverConfig `json:"config"`
	Repetition         int            `json:"repetition"`
	Verdict            string         `json:"verdict"`
	UnavailableSeconds float64        `json:"unavailable_seconds"` // longest gap between acknowledgements
	Machine            GateResult     `json:"machine"`
	Scenario           Result         `json:"scenario"`
}

// RunFailover builds a new primary and standby, then kills the primary under load.
func RunFailover(ctx context.Context, root, outDir string, config FailoverConfig, repetition int, gate QuietGate) (FailoverResult, error) {
	machine := gate.Wait(ctx)
	ha := func(arguments ...string) error {
		output, err := exec.CommandContext(ctx, filepath.Join(root, "scripts/pg-ha.sh"), arguments...).CombinedOutput()
		if err != nil {
			return fmt.Errorf("pg-ha.sh %v: %w: %s", arguments, err, output)
		}
		return nil
	}
	if err := ha("create", config.Mode); err != nil {
		return FailoverResult{}, err
	}
	defer func() { _ = ha("destroy") }()

	options := FaultOptions()
	options.Workers = 2
	options.EngineDatabaseURL = highAvailabilityURL
	name := fmt.Sprintf("failover-%s/rep-%d", config.Mode, repetition)
	spec := Spec{
		Name: name, Fault: "primary killed under load, with the standby's WAL receiver frozen for 1 second",
		Options: options, Routes: []Route{HealthyRoute(receiver.ModeIdempotent)},
		EventsPerRoute: int(config.Rate * config.Duration.Seconds()), Rate: config.Rate,
		DrainDeadline: 2 * time.Minute, Declared: map[string]int{"primary_crashes": 1},
		Evidence:           senderRetried,
		ControlExpectsLoss: config.Mode == "async",
		Inject: func(ctx context.Context, run *ScenarioRun) (map[string]int, error) {
			if err := sleep(ctx, time.Until(run.Start.Add(config.CrashAt))); err != nil {
				return nil, err
			}
			if err := ha("freeze"); err != nil {
				return nil, err
			}
			if err := sleep(ctx, config.Freeze); err != nil {
				return nil, err
			}
			if err := ha("crash"); err != nil {
				return nil, err
			}
			if err := ha("thaw"); err != nil {
				return nil, err
			}
			if err := sleep(ctx, config.Detection); err != nil {
				return nil, err
			}
			if err := ha("promote"); err != nil {
				return nil, err
			}
			return map[string]int{"primary_crashes": 1}, nil
		},
	}
	env := &Env{Root: root, OutDir: outDir, Scale: 1, Seed: config.Seed + uint64(repetition)}
	scenario, err := env.Run(ctx, spec)
	if err != nil {
		return FailoverResult{}, err
	}
	runDir := filepath.Join(outDir, name)
	sends, err := filepath.Glob(filepath.Join(runDir, "loadgen-*.sends.csv.gz"))
	if err != nil || len(sends) != 1 {
		return FailoverResult{}, fmt.Errorf("find the generator's raw file: %v", err)
	}
	gap, err := longestAckGap(sends[0])
	if err != nil {
		return FailoverResult{}, err
	}
	result := FailoverResult{
		Config: config, Repetition: repetition, Verdict: scenario.Verdict,
		UnavailableSeconds: gap.Seconds(), Machine: machine, Scenario: scenario,
	}
	return result, writeJSON(filepath.Join(runDir, "failover.json"), result)
}

// longestAckGap returns the longest time with no acknowledgement. Under constant
// load that is how long ingress was unavailable.
func longestAckGap(path string) (time.Duration, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	decompressed, err := gzip.NewReader(file)
	if err != nil {
		return 0, err
	}
	reader := csv.NewReader(decompressed)
	if _, err := reader.Read(); err != nil {
		return 0, err
	}
	var acked []int64
	for {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return 0, err
		}
		if at, err := strconv.ParseInt(record[3], 10, 64); err == nil && at != 0 {
			acked = append(acked, at)
		}
	}
	slices.Sort(acked)
	var longest time.Duration
	for index := 1; index < len(acked); index++ {
		longest = max(longest, time.Duration(acked[index]-acked[index-1]))
	}
	return longest, nil
}
