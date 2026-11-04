package worker

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/qbeka/durable-webhook-delivery/internal/store"
)

func TestFullJitterDelayStaysUnderTheExponentialCeiling(t *testing.T) {
	base, cap := 100*time.Millisecond, 3*time.Second
	almostOne := func() float64 { return 0.999999 }
	cases := []struct {
		attempt int
		ceiling time.Duration
	}{
		{1, 100 * time.Millisecond},
		{2, 200 * time.Millisecond},
		{5, 1600 * time.Millisecond},
		{6, 3 * time.Second},
		{60, 3 * time.Second},
		{10_000, 3 * time.Second},
	}
	for _, testCase := range cases {
		got := FullJitterDelay(testCase.attempt, base, cap, almostOne)
		if got > testCase.ceiling || got < testCase.ceiling*99/100 {
			t.Errorf("attempt %d: got %v, want just under %v", testCase.attempt, got, testCase.ceiling)
		}
		if zero := FullJitterDelay(testCase.attempt, base, cap, func() float64 { return 0 }); zero != 0 {
			t.Errorf("attempt %d: lowest delay should be zero, got %v", testCase.attempt, zero)
		}
	}
}

func TestClassifyMatchesTheContractTable(t *testing.T) {
	policy := Policy{MaxAttempts: 5, RetryBase: time.Millisecond, RetryCap: time.Second}
	cases := []struct {
		name      string
		status    int
		err       error
		wantState string
		wantLabel string
		wantTrips bool
	}{
		{"200", 200, nil, store.StateSucceeded, "success", false},
		{"204", 204, nil, store.StateSucceeded, "success", false},
		{"301", 301, nil, store.StateDead, "terminal_status", false},
		{"400", 400, nil, store.StateDead, "terminal_status", false},
		{"401", 401, nil, store.StateDead, "terminal_status", false},
		{"404", 404, nil, store.StateDead, "terminal_status", false},
		{"409", 409, nil, store.StateDead, "terminal_status", false},
		{"408", 408, nil, store.StatePending, "retryable_status", true},
		{"425", 425, nil, store.StatePending, "retryable_status", true},
		{"429", 429, nil, store.StatePending, "retryable_status", true},
		{"500", 500, nil, store.StatePending, "retryable_status", true},
		{"503", 503, nil, store.StatePending, "retryable_status", true},
		{"timeout", 0, fmt.Errorf("post: %w", context.DeadlineExceeded), store.StatePending, "timeout", true},
		{"refused", 0, errors.New("connection refused"), store.StatePending, "connection_error", true},
	}
	for _, testCase := range cases {
		report := classify(policy, 1, testCase.status, testCase.err, "")
		if report.NextState != testCase.wantState || report.Outcome != testCase.wantLabel || report.CountsFailure != testCase.wantTrips {
			t.Errorf("%s: got %+v", testCase.name, report)
		}
	}
}

func TestRetryableFailureGoesDeadWhenAttemptsRunOut(t *testing.T) {
	policy := Policy{MaxAttempts: 3, RetryBase: time.Millisecond, RetryCap: time.Second}
	if report := classify(policy, 2, http.StatusInternalServerError, nil, ""); report.NextState != store.StatePending {
		t.Fatalf("attempt 2 of 3 should retry, got %s", report.NextState)
	}
	if report := classify(policy, 3, http.StatusInternalServerError, nil, ""); report.NextState != store.StateDead {
		t.Fatalf("attempt 3 of 3 should go dead, got %s", report.NextState)
	}
}

func TestRetryAfterRaisesTheDelayUpToTheCap(t *testing.T) {
	policy := Policy{MaxAttempts: 9, RetryBase: time.Millisecond, RetryCap: 10 * time.Second}
	report := classify(policy, 1, http.StatusTooManyRequests, nil, "5")
	if report.RetryDelay < 5*time.Second || report.RetryAfter != 5*time.Second {
		t.Fatalf("Retry-After ignored: %+v", report)
	}
	report = classify(policy, 1, http.StatusTooManyRequests, nil, "99999")
	if report.RetryAfter != 10*time.Second {
		t.Fatalf("Retry-After not capped: %+v", report)
	}
	report = classify(policy, 1, http.StatusTooManyRequests, nil, "Wed, 21 Oct 2026 07:28:00 GMT")
	if report.RetryAfter != 0 {
		t.Fatalf("date form should be ignored: %+v", report)
	}
}
