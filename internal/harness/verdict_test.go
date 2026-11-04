package harness

import (
	"errors"
	"testing"

	"github.com/qbeka/durable-webhook-delivery/internal/loadgen"
	"github.com/qbeka/durable-webhook-delivery/internal/reconcile"
)

func passingResult() Result {
	return Result{
		Drained: true, Reconcile: reconcile.Report{Pass: true}, Load: []loadgen.Summary{{Accepted: 10}},
		DeliveredFault: map[string]int{"crashes": 3},
	}
}

func TestVerdictPassesOnlyWithEveryConditionMet(t *testing.T) {
	spec := Spec{Declared: map[string]int{"crashes": 3}}
	if got, reason := verdict(spec, passingResult()); got != "pass" {
		t.Fatalf("%s: %s", got, reason)
	}
	cases := []struct {
		name   string
		damage func(*Result, *Spec)
		want   string
	}{
		{"undelivered fault", func(r *Result, _ *Spec) { r.DeliveredFault["crashes"] = 2 }, "invalid"},
		{"no evidence", func(_ *Result, s *Spec) {
			s.Evidence = func(Result) error { return errors.New("nothing happened") }
		}, "invalid"},
		{"rejected events", func(r *Result, _ *Spec) { r.Load[0].Rejected = 10 }, "fail"},
		{"timed out events", func(r *Result, _ *Spec) { r.Load[0].TimedOut = 1 }, "fail"},
		{"conflicts", func(r *Result, _ *Spec) { r.Load[0].Conflicts = 1 }, "fail"},
		{"unacknowledged events", func(r *Result, _ *Spec) { r.Load[0].UnackedIndexes = []int{4} }, "fail"},
		{"not drained", func(r *Result, _ *Spec) { r.Drained = false }, "fail"},
		{"reconciliation", func(r *Result, _ *Spec) { r.Reconcile.Pass = false }, "fail"},
	}
	for _, testCase := range cases {
		result, testSpec := passingResult(), spec
		testCase.damage(&result, &testSpec)
		if got, reason := verdict(testSpec, result); got != testCase.want {
			t.Errorf("%s: got %s (%s), want %s", testCase.name, got, reason, testCase.want)
		}
	}
}

func TestControlIsInvalidUnlessEveryPlantedRepeatIsFound(t *testing.T) {
	spec := Spec{ControlExpectsRepeats: 5}
	result := passingResult()
	result.DeliveredFault = map[string]int{}
	result.Reconcile.RepeatedEffects = 4
	if got, _ := verdict(spec, result); got != "invalid" {
		t.Fatalf("got %s", got)
	}
	result.Reconcile.RepeatedEffects = 5
	if got, _ := verdict(spec, result); got != "control_detected" {
		t.Fatalf("got %s", got)
	}
}

func TestACleanExitIsNotACrash(t *testing.T) {
	process, err := startProcess(t.TempDir(), "exits", "/bin/sh", []string{"-c", "exit 1"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	process.wait(5e9)
	if process.WasKilled() {
		t.Fatal("exit 1 counted as a crash")
	}
	killed, err := startProcess(t.TempDir(), "sleeps", "/bin/sleep", []string{"30"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	killed.Kill()
	if !killed.WasKilled() {
		t.Fatal("SIGKILL did not count as a crash")
	}
}
