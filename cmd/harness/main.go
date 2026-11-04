// Command harness runs the fault scenarios and benchmarks.
//
//	harness faults [--scale 0.1] [--only name] [--out dir]
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/qbeka/durable-webhook-delivery/internal/harness"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: harness faults [flags]")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	var err error
	switch os.Args[1] {
	case "faults":
		err = runFaults(ctx, os.Args[2:])
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
