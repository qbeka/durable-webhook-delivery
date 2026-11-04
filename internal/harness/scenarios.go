package harness

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/qbeka/durable-webhook-delivery/internal/receiver"
)

// Each fault scenario runs the same way: start clean, apply load, inject the
// fault, wait for the declared drain deadline, then reconcile.

const (
	faultEvents   = 6000 // per route, at scale 1
	faultRate     = 300  // events per second, per route
	faultDeadline = 2 * time.Minute
)

// Evidence checks. Each one names what a working fault must leave behind.

func expiredLeases(result Result) error {
	if result.Reconcile.ExpiredLeases == 0 {
		return errors.New("no lease expired")
	}
	return nil
}

// duplicateAnswers proves a resend reached a receiver that had already committed.
func duplicateAnswers(result Result) error {
	if result.Outcomes["healthy"]["duplicate"] == 0 {
		return errors.New("the receiver never answered duplicate")
	}
	return nil
}

func allOf(checks ...func(Result) error) func(Result) error {
	return func(result Result) error {
		return errors.Join(slices.Collect(func(yield func(error) bool) {
			for _, check := range checks {
				if err := check(result); err != nil && !yield(err) {
					return
				}
			}
		})...)
	}
}

func staleAttempts(result Result) error {
	if result.Reconcile.StaleAttempts == 0 {
		return errors.New("no finish arrived after its lease")
	}
	return nil
}

func senderRetried(result Result) error {
	for _, summary := range result.Load {
		if summary.RetriedRequests > 0 {
			return nil
		}
	}
	return errors.New("no ingress request needed a retry")
}

// failedAttempts proves the fault reached the workers as failed requests.
func failedAttempts(result Result) error {
	if result.Reconcile.FailedAttempts == 0 {
		return errors.New("no attempt failed")
	}
	return nil
}

// countOutcome counts rows of one outcome across receivers, for delivered faults
// that only the receiver can see.
func countOutcome(outcome, fault string) func(Result) map[string]int {
	return func(result Result) map[string]int {
		total := 0
		for _, outcomes := range result.Outcomes {
			total += outcomes[outcome]
		}
		return map[string]int{fault: total}
	}
}

func injectedStatus(status int) func(Result) error {
	return func(result Result) error {
		if result.Outcomes["healthy"][fmt.Sprintf("injected_%d", status)] == 0 {
			return fmt.Errorf("no request was answered %d", status)
		}
		return nil
	}
}

func (e *Env) baseSpec(name, fault string) Spec {
	return Spec{
		Name: name, Fault: fault, Options: FaultOptions(),
		Routes:         []Route{HealthyRoute(receiver.ModeIdempotent)},
		EventsPerRoute: e.scaled(faultEvents), Rate: faultRate, DrainDeadline: faultDeadline,
	}
}

// Control is the negative control: the naive receiver under response loss.
// It must show exactly one repeated effect per dropped response.
func (e *Env) Control() Spec {
	drops := e.scaled(50)
	spec := e.baseSpec("control_naive", "response lost after the receiver commits, naive receiver")
	spec.Routes = []Route{HealthyRoute(receiver.ModeNaive)}
	spec.Declared = map[string]int{"dropped_responses": drops}
	spec.Measured = countOutcome("dropped_response", "dropped_responses")
	spec.ControlExpectsRepeats = drops
	spec.Inject = dropResponses(drops)
	return spec
}

// ControlIdempotent applies the same fault to the reference receiver.
func (e *Env) ControlIdempotent() Spec {
	drops := e.scaled(50)
	spec := e.baseSpec("control_idempotent", "response lost after the receiver commits, reference receiver")
	spec.Declared = map[string]int{"dropped_responses": drops}
	spec.Measured = countOutcome("dropped_response", "dropped_responses")
	spec.Inject = dropResponses(drops)
	spec.Evidence = duplicateAnswers
	return spec
}

func dropResponses(drops int) func(context.Context, *ScenarioRun) (map[string]int, error) {
	return func(ctx context.Context, run *ScenarioRun) (map[string]int, error) {
		route := run.Cluster.Routes[0]
		if err := run.Cluster.SetFaults(route, receiver.FaultPlan{DropResponseAfterCommit: drops}); err != nil {
			return nil, err
		}
		return nil, nil // the count comes from the receiver's log
	}
}

// KillBeforeSend kills workers after the claim commits and before any request.
func (e *Env) KillBeforeSend() Spec {
	crashes := e.scaled(12)
	spec := e.baseSpec("kill_before_send", "SIGKILL after the claim commits, before the request is sent")
	spec.Declared = map[string]int{"worker_crashes": crashes}
	spec.Inject = workerCrashes("before_send", crashes, nil)
	spec.Evidence = expiredLeases
	return spec
}

// KillDuringRequest kills workers while a slow receiver holds their requests.
func (e *Env) KillDuringRequest() Spec {
	crashes := e.scaled(12)
	spec := e.baseSpec("kill_during_request", "SIGKILL after the request is written, while the receiver is still working")
	spec.Declared = map[string]int{"worker_crashes": crashes}
	spec.Inject = workerCrashes("request_written", crashes, &receiver.FaultPlan{
		DelayCount: 1_000_000, DelayBeforeCommit: 150 * time.Millisecond,
	})
	spec.Evidence = allOf(expiredLeases, duplicateAnswers)
	return spec
}

// KillAfterCommitBeforeAck kills workers holding a 2xx they have not recorded.
// This is the crash window the whole project is about.
func (e *Env) KillAfterCommitBeforeAck() Spec {
	crashes := e.scaled(12)
	spec := e.baseSpec("kill_after_commit_before_ack", "SIGKILL after the receiver committed and answered, before the worker records it")
	spec.Declared = map[string]int{"worker_crashes": crashes}
	spec.Inject = workerCrashes("after_response", crashes, nil)
	spec.Evidence = allOf(expiredLeases, duplicateAnswers)
	return spec
}

func workerCrashes(site string, crashes int, plan *receiver.FaultPlan) func(context.Context, *ScenarioRun) (map[string]int, error) {
	return func(ctx context.Context, run *ScenarioRun) (map[string]int, error) {
		route := run.Cluster.Routes[0]
		if plan != nil {
			if err := run.Cluster.SetFaults(route, *plan); err != nil {
				return nil, err
			}
		}
		if err := sleep(ctx, 2*time.Second); err != nil { // let traffic reach the workers
			return nil, err
		}
		achieved, err := crashLoop(ctx, run, site, crashes, 5, 40)
		if err != nil {
			return nil, err
		}
		if plan != nil {
			if err := run.Cluster.SetFaults(route, receiver.FaultPlan{}); err != nil {
				return nil, err
			}
		}
		return map[string]int{"worker_crashes": achieved}, nil
	}
}

// TimeoutThenLateCommit makes the worker give up before the receiver commits. The
// sender records a failure for a request that later succeeds.
func (e *Env) TimeoutThenLateCommit() Spec {
	requests := e.scaled(40)
	spec := e.baseSpec("timeout_then_late_commit", "request times out, then the receiver commits anyway")
	spec.Declared = map[string]int{"late_commits": requests}
	spec.Measured = countOutcome("delayed", "late_commits")
	spec.Evidence = allOf(failedAttempts, duplicateAnswers)
	spec.Inject = func(ctx context.Context, run *ScenarioRun) (map[string]int, error) {
		err := run.Cluster.SetFaults(run.Cluster.Routes[0], receiver.FaultPlan{
			DelayCount: requests, DelayBeforeCommit: run.Cluster.Options.RequestTimeout + 500*time.Millisecond,
		})
		if err != nil {
			return nil, err
		}
		return nil, nil // the count comes from the receiver's log
	}
	return spec
}

// EngineRestart crashes the engine database and every worker together.
func (e *Env) EngineRestart() Spec {
	cycles := e.scaled(3)
	spec := e.baseSpec("engine_db_and_workers_restart", "immediate database stop with SIGKILL of all workers, then restart of both")
	spec.Declared = map[string]int{"database_crashes": cycles, "worker_crashes": cycles * spec.Options.Workers}
	spec.Evidence = senderRetried
	spec.Inject = func(ctx context.Context, run *ScenarioRun) (map[string]int, error) {
		cluster := run.Cluster
		crashes := 0
		for range cycles {
			if err := sleep(ctx, 4*time.Second); err != nil {
				return nil, err
			}
			if err := cluster.PostgresCommand("crash", "engine"); err != nil {
				return nil, err
			}
			for slot := range cluster.Options.Workers {
				cluster.Worker(slot).Kill()
				crashes++
			}
			if err := sleep(ctx, 3*time.Second); err != nil {
				return nil, err
			}
			if err := cluster.PostgresCommand("start", "engine"); err != nil {
				return nil, err
			}
			for slot := range cluster.Options.Workers {
				if err := cluster.StartWorker(slot, ""); err != nil {
					return nil, err
				}
			}
		}
		return map[string]int{"database_crashes": cycles, "worker_crashes": crashes}, nil
	}
	return spec
}

// ReceiverRestart crashes the receiver's database.
func (e *Env) ReceiverRestart() Spec {
	cycles := e.scaled(3)
	spec := e.baseSpec("receiver_db_restart", "immediate stop of the receiver database, then restart")
	spec.Declared = map[string]int{"database_crashes": cycles}
	spec.Evidence = failedAttempts
	spec.Inject = func(ctx context.Context, run *ScenarioRun) (map[string]int, error) {
		for range cycles {
			if err := sleep(ctx, 4*time.Second); err != nil {
				return nil, err
			}
			if err := run.Cluster.PostgresCommand("crash", "receiver"); err != nil {
				return nil, err
			}
			if err := sleep(ctx, 2*time.Second); err != nil {
				return nil, err
			}
			if err := run.Cluster.PostgresCommand("start", "receiver"); err != nil {
				return nil, err
			}
		}
		return map[string]int{"database_crashes": cycles}, nil
	}
	return spec
}

// ReceiverCrash kills the receiver right after it commits, so the sender gets a
// broken connection and the process is gone.
func (e *Env) ReceiverCrash() Spec {
	crashes := e.scaled(6)
	spec := e.baseSpec("receiver_crash_after_commit", "receiver process dies right after its commit, before responding")
	spec.Declared = map[string]int{"receiver_crashes": crashes}
	spec.Evidence = duplicateAnswers
	spec.Inject = func(ctx context.Context, run *ScenarioRun) (map[string]int, error) {
		cluster := run.Cluster
		route := cluster.Routes[0]
		achieved := 0
		for range crashes {
			if err := sleep(ctx, time.Second); err != nil {
				return nil, err
			}
			if err := cluster.SetFaults(route, receiver.FaultPlan{CrashAfterCommit: 1}); err != nil {
				return nil, err
			}
			waitContext, cancel := context.WithTimeout(ctx, 10*time.Second)
			cluster.Receiver(route).WaitForExit(waitContext)
			cancel()
			if cluster.Receiver(route).WasKilled() {
				achieved++
			}
			if err := cluster.StartReceiver(ctx, route); err != nil {
				return nil, err
			}
		}
		return map[string]int{"receiver_crashes": achieved}, nil
	}
	return spec
}

// LeaseExpiresWhileOldWorkerRuns freezes a worker with requests in flight until
// its leases expire. Another worker takes over and sends the same keys while the
// first requests are still being handled. The old worker then wakes and reports.
func (e *Env) LeaseExpiresWhileOldWorkerRuns() Spec {
	pauses := e.scaled(3)
	spec := e.baseSpec("lease_expires_old_worker_running", "SIGSTOP a worker past its lease with requests in flight, then SIGCONT")
	spec.Declared = map[string]int{"worker_pauses": pauses}
	spec.Evidence = allOf(expiredLeases, staleAttempts, duplicateAnswers)
	// Traffic must last through every pause, or a pause freezes an idle worker.
	const secondsPerPause = 7
	spec.EventsPerRoute = int(float64(pauses*secondsPerPause+3) * spec.Rate)
	spec.Inject = func(ctx context.Context, run *ScenarioRun) (map[string]int, error) {
		cluster := run.Cluster
		route := cluster.Routes[0]
		for range pauses {
			if err := sleep(ctx, 2*time.Second); err != nil {
				return nil, err
			}
			err := cluster.SetFaults(route, receiver.FaultPlan{DelayCount: 1_000_000, DelayBeforeCommit: 400 * time.Millisecond})
			if err != nil {
				return nil, err
			}
			if err := sleep(ctx, 200*time.Millisecond); err != nil {
				return nil, err
			}
			process, err := cluster.BusiestWorker(ctx)
			if err != nil {
				return nil, err
			}
			if err := process.Pause(); err != nil {
				return nil, err
			}
			if err := sleep(ctx, cluster.Options.Lease+1500*time.Millisecond); err != nil {
				return nil, err
			}
			if err := process.Resume(); err != nil {
				return nil, err
			}
			if err := cluster.SetFaults(route, receiver.FaultPlan{}); err != nil {
				return nil, err
			}
		}
		return map[string]int{"worker_pauses": pauses}, nil
	}
	return spec
}

// StatusBurst makes the receiver answer one status for a length of time.
func (e *Env) StatusBurst(status int) Spec {
	burst := 4 * time.Second
	name := fmt.Sprintf("status_burst_%d", status)
	spec := e.baseSpec(name, fmt.Sprintf("receiver answers %d to everything for %v", status, burst))
	spec.Declared = map[string]int{"status_bursts": 1}
	spec.Evidence = injectedStatus(status)
	spec.Inject = func(ctx context.Context, run *ScenarioRun) (map[string]int, error) {
		if err := sleep(ctx, 2*time.Second); err != nil {
			return nil, err
		}
		plan := receiver.FaultPlan{StatusBurstFor: burst, StatusBurstCode: status}
		if status == 429 {
			plan.RetryAfterSeconds = 1
		}
		if err := run.Cluster.SetFaults(run.Cluster.Routes[0], plan); err != nil {
			return nil, err
		}
		return map[string]int{"status_bursts": 1}, nil
	}
	return spec
}

// DestinationOffline takes one destination offline while a healthy tenant keeps
// sending at the same rate.
func (e *Env) DestinationOffline(outage time.Duration) Spec {
	spec := e.baseSpec("destination_offline", fmt.Sprintf("one destination offline for %v, another tenant keeps sending", outage))
	spec.Routes = []Route{HealthyRoute(receiver.ModeIdempotent), FlakyRoute(receiver.ModeIdempotent)}
	spec.Declared = map[string]int{"outages": 1}
	spec.Evidence = failedAttempts
	// Traffic must outlast the outage at every scale, or the outage tests nothing.
	spec.EventsPerRoute = int((outage + 10*time.Second).Seconds() * spec.Rate)
	spec.Inject = offlineInjector(outage)
	return spec
}

func offlineInjector(outage time.Duration) func(context.Context, *ScenarioRun) (map[string]int, error) {
	return func(ctx context.Context, run *ScenarioRun) (map[string]int, error) {
		cluster := run.Cluster
		flaky := cluster.Routes[1]
		lead := 5 * time.Second
		if err := sleep(ctx, time.Until(run.Start.Add(lead))); err != nil {
			return nil, err
		}
		outageStart := time.Now()
		cluster.KillReceiver(flaky)
		if err := sleep(ctx, outage); err != nil {
			return nil, err
		}
		outageEnd := time.Now()
		if err := cluster.StartReceiver(ctx, flaky); err != nil {
			return nil, err
		}
		if err := writeJSON(run.OutDir+"/outage.json", map[string]any{
			"schedule_start_unix_ns": run.Start.UnixNano(),
			"outage_start_unix_ns":   outageStart.UnixNano(),
			"outage_end_unix_ns":     outageEnd.UnixNano(),
		}); err != nil {
			return nil, err
		}
		return map[string]int{"outages": 1}, nil
	}
}

// ReferenceParity repeats the reference benchmark's workload over HTTP with
// several workers: 100,000 distinct keys, 20,000 duplicate inputs and 30 abrupt
// worker exits, ten at each of three sites.
func (e *Env) ReferenceParity() Spec {
	const crashesPerSite = 10
	spec := e.baseSpec("reference_parity", "the reference workload over HTTP: 30 abrupt worker exits across three sites")
	spec.EventsPerRoute = e.scaled(100_000)
	spec.Duplicates = e.scaled(20_000)
	spec.Rate = 500
	spec.DrainDeadline = 5 * time.Minute
	crashes := e.scaled(crashesPerSite)
	spec.Declared = map[string]int{"before_send": crashes, "request_written": crashes, "after_response": crashes}
	spec.Evidence = allOf(expiredLeases, duplicateAnswers)
	spec.Inject = func(ctx context.Context, run *ScenarioRun) (map[string]int, error) {
		if err := sleep(ctx, 3*time.Second); err != nil {
			return nil, err
		}
		delivered := map[string]int{}
		for _, site := range []string{"before_send", "request_written", "after_response"} {
			achieved, err := crashLoop(ctx, run, site, crashes, 20, 400)
			if err != nil {
				return nil, err
			}
			delivered[site] = achieved
		}
		return delivered, nil
	}
	return spec
}

// FaultSuite lists the scenarios of a full fault run, controls first.
func (e *Env) FaultSuite() []Spec {
	return []Spec{
		e.Control(),
		e.ControlIdempotent(),
		e.KillBeforeSend(),
		e.KillDuringRequest(),
		e.KillAfterCommitBeforeAck(),
		e.TimeoutThenLateCommit(),
		e.EngineRestart(),
		e.ReceiverRestart(),
		e.ReceiverCrash(),
		e.LeaseExpiresWhileOldWorkerRuns(),
		e.StatusBurst(429),
		e.StatusBurst(500),
		e.DestinationOffline(20 * time.Second),
		e.ReferenceParity(),
	}
}
