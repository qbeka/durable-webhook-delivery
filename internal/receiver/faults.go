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

// Faults holds faults injected by the harness. Each fault applies to the next N
// matching requests. It is served on a separate loopback admin port, never on
// the delivery endpoint.
type Faults struct {
	mutex  sync.Mutex
	active FaultPlan
}

// FaultPlan is the JSON body of POST /faults. Setting it replaces the old plan.
type FaultPlan struct {
	DropResponseAfterCommit int           `json:"drop_response_after_commit"` // requests
	CrashAfterCommit        int           `json:"crash_after_commit"`         // requests
	StatusBurstCount        int           `json:"status_burst_count"`         // requests
	StatusBurstCode         int           `json:"status_burst_code"`          // HTTP status
	RetryAfterSeconds       int           `json:"retry_after_seconds"`        // sent with the burst
	DelayCount              int           `json:"delay_count"`                // requests
	DelayBeforeCommit       time.Duration `json:"delay_before_commit_ns"`     // per request
}

func (f *Faults) takeStatusBurst() (status, retryAfterSeconds int) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	if f.active.StatusBurstCount <= 0 {
		return 0, 0
	}
	f.active.StatusBurstCount--
	return f.active.StatusBurstCode, f.active.RetryAfterSeconds
}

func (f *Faults) waitBeforeCommit() {
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
		f.mutex.Lock()
		f.active = plan
		f.mutex.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	return mux
}
