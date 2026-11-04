// Command harness runs the fault scenarios and benchmarks.
//
//	harness faults [--scale 0.1] [--only name] [--out dir]
//	harness bench  [--rate 1000] [--duration 60s] [--workers 1] [--repetitions 3]
//	harness recovery [--mode offline] [--outage 60s] [--max-in-flight 16] [--breaker 5]
//	harness generator [--rates 1000,2000]
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/qbeka/durable-webhook-delivery/internal/harness"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: harness faults|bench|recovery|generator [flags]")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	var err error
	switch os.Args[1] {
	case "faults":
		err = runFaults(ctx, os.Args[2:])
	case "bench":
		err = runBench(ctx, os.Args[2:])
	case "recovery":
		err = runRecovery(ctx, os.Args[2:])
	case "generator":
		err = runGenerator(ctx, os.Args[2:])
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		log.Fatal(err)
	}
}

func repositoryRoot() string {
	root, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	return root
}

func runFaults(ctx context.Context, arguments []string) error {
	flags := flag.NewFlagSet("faults", flag.ExitOnError)
	outDir := flags.String("out", ".run/faults", "output directory")
	scale := flags.Float64("scale", 1, "multiplies event and fault counts")
	seed := flags.Uint64("seed", 2026, "workload seed")
	only := flags.String("only", "", "comma-separated scenario names")
	_ = flags.Parse(arguments)

	absolute, err := filepath.Abs(*outDir)
	if err != nil {
		return err
	}
	env := &harness.Env{Root: repositoryRoot(), OutDir: absolute, Scale: *scale, Seed: *seed}
	selected := map[string]bool{}
	for _, name := range strings.Split(*only, ",") {
		if name != "" {
			selected[name] = true
		}
	}

	failed := 0
	for _, spec := range env.FaultSuite() {
		if len(selected) > 0 && !selected[spec.Name] {
			continue
		}
		started := time.Now()
		result, err := env.Run(ctx, spec)
		if err != nil {
			return err
		}
		report := result.Reconcile
		fmt.Printf("%-34s %-17s %5.0fs drain=%.1fs effects=%d/%d repeated=%d missing=%d dead=%d unfinished=%d stale=%d expired=%d\n  %s\n",
			result.Scenario, result.Verdict, time.Since(started).Seconds(), result.DrainSeconds,
			report.DistinctEffects, report.ExpectedEffects, report.RepeatedEffects, report.MissingEffects,
			report.DeadLetters, report.UnfinishedJobs, report.StaleAttempts, report.ExpiredLeases, result.VerdictReason)
		if result.Verdict != "pass" && result.Verdict != "control_detected" {
			failed++
		}
	}
	if failed > 0 {
		return fmt.Errorf("%d scenarios did not pass", failed)
	}
	return nil
}

func runBench(ctx context.Context, arguments []string) error {
	flags := flag.NewFlagSet("bench", flag.ExitOnError)
	outDir := flags.String("out", ".run/bench", "output directory")
	name := flags.String("name", "", "configuration name, derived from the flags when empty")
	rate := flags.Float64("rate", 1000, "offered events per second")
	duration := flags.Duration("duration", time.Minute, "length of the arrival schedule")
	warmup := flags.Duration("warmup", 10*time.Second, "start of the schedule to leave out of the results")
	workers := flags.Int("workers", 1, "worker processes")
	concurrency := flags.Int("concurrency", 64, "requests in flight per worker")
	maxInFlight := flags.Int("max-in-flight", 256, "requests in flight per destination")
	receiverConnections := flags.Int("receiver-connections", 64, "database connections in the receiver")
	repetitions := flags.Int("repetitions", 3, "repetitions of this configuration")
	seed := flags.Uint64("seed", 7000, "workload seed, plus the repetition number")
	gate := quietGateFlags(flags)
	drainDeadline := flags.Duration("drain-deadline", 2*time.Minute, "longest wait for the queue after the schedule ends")
	_ = flags.Parse(arguments)

	options := harness.BenchOptions(*workers, *concurrency, *maxInFlight)
	options.ReceiverConnections = *receiverConnections
	config := harness.BenchConfig{
		Name: *name, Rate: *rate, Duration: *duration, Warmup: *warmup, PayloadBytes: 1024, Fanout: 1,
		DrainDeadline: *drainDeadline, Seed: *seed,
		Options: options,
	}
	if config.Name == "" {
		config.Name = fmt.Sprintf("r%.0f-w%d", *rate, *workers)
	}
	absolute, err := filepath.Abs(*outDir)
	if err != nil {
		return err
	}
	root := repositoryRoot()
	manifest, err := harness.NewManifest(ctx, root, config.Name, *seed, config)
	if err != nil {
		return err
	}
	for repetition := 1; repetition <= *repetitions; repetition++ {
		result, err := harness.RunBench(ctx, root, absolute, config, repetition, *gate)
		if err != nil {
			return err
		}
		fmt.Printf("%s rep %d: offered %.0f/s accepted %.0f/s completed %.0f/s errors %.4f%% slope %+.1f/s backlog %d effect %s ack p99 %.1fms drain %.1fs reconcile=%v genlag p99 %.0fus background cpu %.0f%% contended=%v\n",
			config.Name, repetition, result.OfferedRate, result.AcceptedRate, result.CompletedRate, 100*result.ErrorRate,
			result.BacklogSlopePerSec, result.BacklogAtEnd, result.Latency, result.AckLatency.P99Ms,
			result.DrainSeconds, result.Reconcile.Pass, result.Generator.SendLagP99Micros,
			result.Machine.BackgroundCPUPercent, result.Machine.Contended)
	}
	return manifest.Finish(filepath.Join(absolute, config.Name))
}

func runRecovery(ctx context.Context, arguments []string) error {
	flags := flag.NewFlagSet("recovery", flag.ExitOnError)
	outDir := flags.String("out", ".run/recovery", "output directory")
	name := flags.String("name", "", "configuration name")
	mode := flags.String("mode", "offline", "offline or slow")
	rate := flags.Float64("rate", 300, "events per second per tenant")
	lead := flags.Duration("lead", 20*time.Second, "normal traffic before the outage")
	outage := flags.Duration("outage", time.Minute, "length of the outage")
	trail := flags.Duration("trail", 20*time.Second, "traffic after the outage")
	maxInFlight := flags.Int("max-in-flight", 64, "requests in flight per destination")
	breaker := flags.Int("breaker", 5, "consecutive failures that open the breaker")
	repetitions := flags.Int("repetitions", 3, "repetitions")
	seed := flags.Uint64("seed", 8000, "workload seed, plus the repetition number")
	gate := quietGateFlags(flags)
	_ = flags.Parse(arguments)

	config := harness.RecoveryConfig{
		Name: *name, Mode: *mode, Rate: *rate, Lead: *lead, Outage: *outage, Trail: *trail,
		MaxInFlight: *maxInFlight, Breaker: *breaker, Seed: *seed,
	}
	if config.Name == "" {
		config.Name = fmt.Sprintf("%s-limit%d-breaker%d", *mode, *maxInFlight, *breaker)
	}
	absolute, err := filepath.Abs(*outDir)
	if err != nil {
		return err
	}
	root := repositoryRoot()
	manifest, err := harness.NewManifest(ctx, root, config.Name, *seed, config)
	if err != nil {
		return err
	}
	for repetition := 1; repetition <= *repetitions; repetition++ {
		result, err := harness.RunRecovery(ctx, root, absolute, config, repetition, *gate)
		if err != nil {
			return err
		}
		fmt.Printf("%s rep %d: %s healthy before p99 %.1fms during p99 %.1fms after p99 %.1fms peak backlog %d drain after outage %.1fs repeated=%d missing=%d dead=%d\n",
			config.Name, repetition, result.Verdict, result.HealthyBefore.P99Ms, result.HealthyDuring.P99Ms, result.HealthyAfter.P99Ms,
			result.PeakBacklog, result.DrainAfterOutageSec, result.Scenario.Reconcile.RepeatedEffects,
			result.Scenario.Reconcile.MissingEffects, result.Scenario.Reconcile.DeadLetters)
	}
	return manifest.Finish(filepath.Join(absolute, config.Name))
}

// quietGateFlags adds the flags for waiting on a shared machine. The default
// never waits, so a local run starts at once.
func quietGateFlags(flags *flag.FlagSet) *harness.QuietGate {
	gate := &harness.QuietGate{}
	flags.Float64Var(&gate.MaxCPUPercent, "quiet-cpu", 1e9, "wait until background CPU is below this percent, 100 is one core")
	flags.DurationVar(&gate.MaxWait, "quiet-wait", 0, "longest wait for a quiet machine")
	return gate
}

func runGenerator(ctx context.Context, arguments []string) error {
	flags := flag.NewFlagSet("generator", flag.ExitOnError)
	outDir := flags.String("out", ".run/generator", "output directory")
	rates := flags.String("rates", "1000,2000,4000,8000,16000", "offered rates to try")
	duration := flags.Duration("duration", 20*time.Second, "length of each step")
	seed := flags.Uint64("seed", 9000, "workload seed")
	_ = flags.Parse(arguments)

	absolute, err := filepath.Abs(*outDir)
	if err != nil {
		return err
	}
	var results []harness.GeneratorResult
	for _, text := range strings.Split(*rates, ",") {
		rate, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return err
		}
		result, err := harness.MeasureGenerator(ctx, repositoryRoot(), absolute, rate, *duration, *seed)
		if err != nil {
			return err
		}
		fmt.Printf("generator %.0f/s: achieved %.0f/s lag p50 %.0fus p99 %.0fus max %.0fus\n",
			rate, result.AchievedRate, result.SendLagP50Us, result.SendLagP99Us, result.SendLagMaxUs)
		results = append(results, result)
	}
	return harness.WriteJSON(filepath.Join(absolute, "generator.json"), results)
}
