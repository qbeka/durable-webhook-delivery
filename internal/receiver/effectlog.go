package receiver

import (
	"bufio"
	"fmt"
	"os"
	"sync"
	"time"
)

// EffectLog appends one CSV row per request, stamped after the commit returned.
// It is the raw latency data, and it is what the event trace reads. The ledger
// stays the source of truth for correctness: a crash can lose the last rows of
// this file but never a ledger row.
type EffectLog struct {
	mutex  sync.Mutex
	file   *os.File
	writer *bufio.Writer
	stop   chan struct{}
}

// OpenEffectLog creates the file and flushes it every 100 ms.
func OpenEffectLog(path string) (*EffectLog, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	log := &EffectLog{file: file, writer: bufio.NewWriterSize(file, 1<<20), stop: make(chan struct{})}
	if info, err := file.Stat(); err == nil && info.Size() == 0 {
		fmt.Fprintln(log.writer, "time_unix_ns,arrival_unix_ns,delivery_key,event_id,attempt,outcome")
	}
	go log.flushLoop()
	return log, nil
}

// Record appends one row.
func (l *EffectLog) Record(incoming delivery, outcome string) {
	now := time.Now().UnixNano()
	l.mutex.Lock()
	defer l.mutex.Unlock()
	fmt.Fprintf(l.writer, "%d,%d,%s,%s,%d,%s\n", now, incoming.receipt.ArrivalUnixNanos,
		incoming.key, incoming.eventID, incoming.attempt, outcome)
}

func (l *EffectLog) flushLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-ticker.C:
			l.mutex.Lock()
			_ = l.writer.Flush()
			l.mutex.Unlock()
		}
	}
}

// Close flushes and closes the file.
func (l *EffectLog) Close() error {
	close(l.stop)
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if err := l.writer.Flush(); err != nil {
		return err
	}
	return l.file.Close()
}
