// Package failpoint lets the fault harness stop a process at an exact line.
//
// HOOK_FAILPOINT holds one rule:
//
//	before_send@5        kill this process on the 5th hit of before_send
//	lease_stall@3=2500ms sleep for 2.5 seconds on the 3rd hit of lease_stall
//
// With the variable unset, Hit is one atomic load and does nothing.
package failpoint

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

type rule struct {
	name      string
	targetHit int64
	sleep     time.Duration // zero means kill the process
	hits      atomic.Int64
}

var active atomic.Pointer[rule]

func init() {
	spec := os.Getenv("HOOK_FAILPOINT")
	if spec == "" {
		return
	}
	parsed, err := parse(spec)
	if err != nil {
		fmt.Fprintln(os.Stderr, "HOOK_FAILPOINT:", err)
		os.Exit(2)
	}
	active.Store(parsed)
}

func parse(spec string) (*rule, error) {
	name, rest, found := strings.Cut(spec, "@")
	if !found || name == "" {
		return nil, fmt.Errorf("want name@hit or name@hit=duration, got %q", spec)
	}
	hitText, sleepText, hasSleep := strings.Cut(rest, "=")
	targetHit, err := strconv.ParseInt(hitText, 10, 64)
	if err != nil || targetHit < 1 {
		return nil, fmt.Errorf("bad hit count in %q", spec)
	}
	parsed := &rule{name: name, targetHit: targetHit}
	if hasSleep {
		if parsed.sleep, err = time.ParseDuration(sleepText); err != nil || parsed.sleep <= 0 {
			return nil, fmt.Errorf("bad duration in %q", spec)
		}
	}
	return parsed, nil
}

// Hit runs the configured action when name reaches its target hit.
func Hit(name string) {
	current := active.Load()
	if current == nil || current.name != name {
		return
	}
	if current.hits.Add(1) != current.targetHit {
		return
	}
	if current.sleep > 0 {
		time.Sleep(current.sleep)
		return
	}
	// SIGKILL, not os.Exit: no deferred function or flush may run.
	_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
	select {} // the signal is delivered before this goroutine runs again
}
