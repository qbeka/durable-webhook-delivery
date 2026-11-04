package receiver

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

type afterCommitAction int

const (
	afterCommitNone afterCommitAction = iota
	afterCommitDropResponse
	afterCommitCrash
)

// Faults holds faults injected by the harness. Most apply to the next N matching
// requests. A status burst lasts a length of time, as real overload does: a
// breaker slows requests down, so a burst counted in requests would last as long
// as the breaker made it. Faults are set on a separate loopback admin port, never
// on the delivery endpoint.
type Faults struct {
	mutex      sync.Mutex
	active     FaultPlan
	burstUntil time.Time
}

// Set replaces the fault plan.
func (f *Faults) Set(plan FaultPlan) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.active = plan
	f.burstUntil = time.Now().Add(plan.StatusBurstFor)
}

// FaultPlan is the JSON body of POST /faults. Setting it replaces the old plan.
type FaultPlan struct {
	DropResponseAfterCommit int           `json:"drop_response_after_commit"` // requests
	CrashAfterCommit        int           `json:"crash_after_commit"`         // requests
	StatusBurstFor          time.Duration `json:"status_burst_for_ns"`        // from when the plan is set
	StatusBurstCode         int           `json:"status_burst_code"`          // HTTP status
	RetryAfterSeconds       int           `json:"retry_after_seconds"`        // sent with the burst
	DelayCount              int           `json:"delay_count"`                // requests
	DelayBeforeCommit       time.Duration `json:"delay_before_commit_ns"`     // per request
}

func (f *Faults) takeStatusBurst() (status, retryAfterSeconds int) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	if f.active.StatusBurstFor <= 0 || time.Now().After(f.burstUntil) {
		return 0, 0
	}
	return f.active.StatusBurstCode, f.active.RetryAfterSeconds
}

// waitBeforeCommit reports whether this request was delayed.
func (f *Faults) waitBeforeCommit() bool {
	f.mutex.Lock()
	delay := time.Duration(0)
	if f.active.DelayCount > 0 {
		f.active.DelayCount--
		delay = f.active.DelayBeforeCommit
	}
	f.mutex.Unlock()
	if delay > 0 {
		time.Sleep(delay)
	}
	return delay > 0
}

func (f *Faults) takeAfterCommit() afterCommitAction {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	switch {
	case f.active.CrashAfterCommit > 0:
		f.active.CrashAfterCommit--
		return afterCommitCrash
	case f.active.DropResponseAfterCommit > 0:
		f.active.DropResponseAfterCommit--
		return afterCommitDropResponse
	}
	return afterCommitNone
}

// AdminRoutes returns the fault control endpoint.
func (f *Faults) AdminRoutes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /faults", func(w http.ResponseWriter, r *http.Request) {
		var plan FaultPlan
		if err := json.NewDecoder(r.Body).Decode(&plan); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		f.Set(plan)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	return mux
}
