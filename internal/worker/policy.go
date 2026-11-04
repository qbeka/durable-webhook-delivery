package worker

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"

	"github.com/qbeka/durable-webhook-delivery/internal/store"
)

// Policy holds the retry rules from docs/contract.md.
type Policy struct {
	MaxAttempts int           // attempts allowed since creation or the last replay
	RetryBase   time.Duration // ceiling of the first retry delay
	RetryCap    time.Duration // largest retry delay
}

// FullJitterDelay returns a uniform random delay in [0, min(cap, base*2^(n-1))),
// where n is the number of attempts used so far. Random delays spread out
// retries that failed together. Without them, a destination that recovers gets
// the whole backlog at the same instant.
func FullJitterDelay(attempt int, base, cap time.Duration, random func() float64) time.Duration {
	ceiling := float64(cap)
	// Past about 2^40 the product is larger than any useful cap and could overflow.
	if attempt-1 < 40 {
		ceiling = min(ceiling, float64(base)*float64(uint64(1)<<(attempt-1)))
	}
	return time.Duration(random() * ceiling)
}

// parseRetryAfter reads the seconds form of Retry-After. The date form is ignored.
func parseRetryAfter(value string, cap time.Duration) time.Duration {
	seconds, err := strconv.Atoi(value)
	if err != nil || seconds <= 0 {
		return 0
	}
	return min(time.Duration(seconds)*time.Second, cap)
}

// classify turns the result of one request into the report for the database.
// status is 0 when no response arrived.
func classify(policy Policy, budgetUsed, status int, requestErr error, retryAfterHeader string) store.Finish {
	report := classifyResult(status, requestErr)
	if report.NextState != store.StatePending {
		return report
	}
	report.RetryAfter = parseRetryAfter(retryAfterHeader, policy.RetryCap)
	if budgetUsed >= policy.MaxAttempts {
		report.NextState = store.StateDead
		report.Error = fmt.Sprintf("attempts exhausted: %s", report.Error)
		return report
	}
	jittered := FullJitterDelay(budgetUsed, policy.RetryBase, policy.RetryCap, rand.Float64)
	report.RetryDelay = max(jittered, report.RetryAfter)
	return report
}

func classifyResult(status int, requestErr error) store.Finish {
	switch {
	case requestErr != nil && errors.Is(requestErr, context.DeadlineExceeded):
		return store.Finish{NextState: store.StatePending, Outcome: "timeout", Error: "request timed out", CountsFailure: true}
	case requestErr != nil:
		return store.Finish{NextState: store.StatePending, Outcome: "connection_error", Error: requestErr.Error(), CountsFailure: true}
	case status >= 200 && status < 300:
		return store.Finish{NextState: store.StateSucceeded, Outcome: "success", HTTPStatus: status}
	case status == http.StatusRequestTimeout, status == http.StatusTooEarly,
		status == http.StatusTooManyRequests, status >= 500:
		return store.Finish{NextState: store.StatePending, Outcome: "retryable_status", HTTPStatus: status,
			Error: fmt.Sprintf("status %d", status), CountsFailure: true}
	default:
		// 3xx and other 4xx. The receiver has said this request will never succeed.
		return store.Finish{NextState: store.StateDead, Outcome: "terminal_status", HTTPStatus: status,
			Error: fmt.Sprintf("status %d", status)}
	}
}
