// Package harness runs the real binaries, injects faults across the HTTP
// boundary, and reconciles the result.
package harness

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"
)

// Process is one running binary with its output in a log file.
type Process struct {
	Name string
	cmd  *exec.Cmd
	log  *os.File
	done chan error
	over atomic.Bool
}

// startProcess launches a binary and captures its output.
func startProcess(runDir, name, binary string, arguments, environment []string) (*Process, error) {
	log, err := os.Create(filepath.Join(runDir, name+".log"))
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(binary, arguments...)
	cmd.Stdout, cmd.Stderr = log, log
	cmd.Env = append(os.Environ(), environment...)
	if err := cmd.Start(); err != nil {
		log.Close()
		return nil, fmt.Errorf("start %s: %w", name, err)
	}
	process := &Process{Name: name, cmd: cmd, log: log, done: make(chan error, 1)}
	go func() {
		err := cmd.Wait()
		process.over.Store(true)
		log.Close()
		process.done <- err
	}()
	return process, nil
}

// Exited reports whether the process has ended.
func (p *Process) Exited() bool {
	return p.over.Load()
}

// WasKilled reports whether the process died from SIGKILL. A clean exit or a
// failed start does not count, or a broken binary would look like a delivered crash.
func (p *Process) WasKilled() bool {
	if !p.over.Load() {
		return false
	}
	status, ok := p.cmd.ProcessState.Sys().(syscall.WaitStatus)
	return ok && status.Signaled() && status.Signal() == syscall.SIGKILL
}

// Kill sends SIGKILL and waits for the process to end.
func (p *Process) Kill() {
	_ = p.cmd.Process.Signal(syscall.SIGKILL)
	p.wait(5 * time.Second)
}

// Terminate asks the process to stop and waits, then kills it if it does not.
func (p *Process) Terminate(timeout time.Duration) {
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	if !p.wait(timeout) {
		p.Kill()
	}
}

// Pause freezes the process with SIGSTOP, as a long garbage collection pause or a
// frozen virtual machine would. Its leases keep running out in the database.
func (p *Process) Pause() error {
	return p.cmd.Process.Signal(syscall.SIGSTOP)
}

// Resume undoes Pause.
func (p *Process) Resume() error {
	return p.cmd.Process.Signal(syscall.SIGCONT)
}

// WaitForExit returns true if the process ended before ctx did.
func (p *Process) WaitForExit(ctx context.Context) bool {
	select {
	case err := <-p.done:
		p.done <- err // keep the result for any other waiter
		return true
	case <-ctx.Done():
		return false
	}
}

func (p *Process) wait(timeout time.Duration) bool {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return p.WaitForExit(ctx)
}
