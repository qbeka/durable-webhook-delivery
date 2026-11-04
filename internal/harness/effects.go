package harness

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/qbeka/durable-webhook-delivery/internal/loadgen"
)

// EffectRow is one row of a receiver's effect log.
type EffectRow struct {
	TimeUnixNs    int64
	ArrivalUnixNs int64
	DeliveryKey   string
	EventID       string
	Attempt       int
	Outcome       string
}

// ReadEffects loads an effect log. A file that does not exist reads as empty. A
// final row cut short by a crash is skipped, since the ledger holds the truth.
func ReadEffects(path string) ([]EffectRow, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	reader.ReuseRecord = true
	if _, err := reader.Read(); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil
		}
		return nil, err
	}
	var rows []EffectRow
	for {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			return rows, nil
		}
		if err != nil || len(record) != 6 {
			continue
		}
		timeNs, err1 := strconv.ParseInt(record[0], 10, 64)
		arrivalNs, err2 := strconv.ParseInt(record[1], 10, 64)
		attempt, err3 := strconv.Atoi(record[4])
		if err1 != nil || err2 != nil || err3 != nil {
			continue
		}
		rows = append(rows, EffectRow{timeNs, arrivalNs, record[2], record[3], attempt, record[5]})
	}
}

// LatencySummary describes arrival-to-effect latency in milliseconds. Count is
// the denominator: rows with outcome applied.
type LatencySummary struct {
	Count int     `json:"count"`
	P50Ms float64 `json:"p50_ms"`
	P95Ms float64 `json:"p95_ms"`
	P99Ms float64 `json:"p99_ms"`
	MaxMs float64 `json:"max_ms"`
}

// SummarizeLatency measures rows whose arrival falls in [from, to). A zero time
// means no bound.
func SummarizeLatency(rows []EffectRow, from, to time.Time) LatencySummary {
	var latencies []float64
	for _, row := range rows {
		if row.Outcome != "applied" {
			continue
		}
		if !from.IsZero() && row.ArrivalUnixNs < from.UnixNano() {
			continue
		}
		if !to.IsZero() && row.ArrivalUnixNs >= to.UnixNano() {
			continue
		}
		latencies = append(latencies, float64(row.TimeUnixNs-row.ArrivalUnixNs)/1e6)
	}
	return LatencySummary{
		Count: len(latencies),
		P50Ms: loadgen.Percentile(latencies, 50), P95Ms: loadgen.Percentile(latencies, 95),
		P99Ms: loadgen.Percentile(latencies, 99), MaxMs: loadgen.Percentile(latencies, 100),
	}
}

// LatencyWindow is latency for events arriving in one window.
type LatencyWindow struct {
	OffsetSeconds float64 `json:"offset_seconds"` // from the first arrival
	LatencySummary
}

// LatencyOverTime cuts a run into windows by arrival time.
func LatencyOverTime(rows []EffectRow, start time.Time, window time.Duration, count int) []LatencyWindow {
	windows := make([]LatencyWindow, count)
	for index := range windows {
		from := start.Add(time.Duration(index) * window)
		windows[index] = LatencyWindow{
			OffsetSeconds:  from.Sub(start).Seconds(),
			LatencySummary: SummarizeLatency(rows, from, from.Add(window)),
		}
	}
	return windows
}

// describeOutcomes counts rows by outcome, for result files.
func describeOutcomes(rows []EffectRow) map[string]int {
	counts := map[string]int{}
	for _, row := range rows {
		counts[row.Outcome]++
	}
	return counts
}

func (l LatencySummary) String() string {
	return fmt.Sprintf("n=%d p50=%.1fms p95=%.1fms p99=%.1fms max=%.1fms", l.Count, l.P50Ms, l.P95Ms, l.P99Ms, l.MaxMs)
}
