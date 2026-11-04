// Package workload builds the synthetic event stream as pure functions of a seed.
//
// The expected ledger comes from these functions alone. Reconciliation never
// reads what the system wrote to decide what it should have written.
package workload

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand/v2"
	"strings"
	"time"
)

// Stream describes one source's traffic.
type Stream struct {
	TenantID      string  `json:"tenant_id"`
	SourceID      string  `json:"source_id"`
	Seed          uint64  `json:"seed"`
	EventCount    int     `json:"event_count"`
	RatePerSecond float64 `json:"rate_per_second"`
	PayloadBytes  int     `json:"payload_bytes"`
	StartUnixNs   int64   `json:"start_unix_ns"`
}

// Currency is the currency of every synthetic receipt.
const Currency = "CAD"

var merchants = []string{
	"Synthetic Coffee", "Example Grocery", "Placeholder Transit", "Sample Books", "Fixture Hardware",
}

func (s Stream) mix(index int) uint64 {
	var input [32]byte
	binary.LittleEndian.PutUint64(input[0:], s.Seed)
	binary.LittleEndian.PutUint64(input[8:], uint64(index))
	copy(input[16:], s.TenantID)
	sum := sha256.Sum256(input[:])
	return binary.LittleEndian.Uint64(sum[:8])
}

// EventID returns the event ID for an index.
func (s Stream) EventID(index int) string {
	return fmt.Sprintf("evt-%d-%d", s.Seed, index)
}

// ReceiptID returns the business ID for an index.
func (s Stream) ReceiptID(index int) string {
	return fmt.Sprintf("rcpt-%d-%d", s.Seed, index)
}

// AmountCents returns the expense amount for an index, from 1 to 100,000 cents.
func (s Stream) AmountCents(index int) int64 {
	return int64(1 + s.mix(index)%100_000)
}

// Merchant returns the merchant for an index.
func (s Stream) Merchant(index int) string {
	return merchants[s.mix(index)%uint64(len(merchants))]
}

// ArrivalOffsets returns each event's scheduled arrival as an offset from the
// start. Arrivals form a Poisson process: the gaps between them are exponential,
// which is the usual model for independent senders and creates the small bursts a
// fixed tick would hide.
func (s Stream) ArrivalOffsets() []time.Duration {
	random := rand.New(rand.NewPCG(s.Seed, 0x9e3779b97f4a7c15))
	offsets := make([]time.Duration, s.EventCount)
	var seconds float64
	for index := range offsets {
		seconds += random.ExpFloat64() / s.RatePerSecond
		offsets[index] = time.Duration(seconds * float64(time.Second))
	}
	return offsets
}

// Body returns the exact request body for an event. The arrival time is inside
// the body, so the receiver can measure from arrival to durable effect. Padding
// brings the body to exactly PayloadBytes.
func (s Stream) Body(index int, arrivalUnixNs int64) []byte {
	prefix := fmt.Sprintf(`{"receipt_id":%q,"amount_cents":%d,"currency":%q,"merchant":%q,"arrival_unix_ns":%d,"pad":"`,
		s.ReceiptID(index), s.AmountCents(index), Currency, s.Merchant(index), arrivalUnixNs)
	const suffix = `"}`
	padding := max(0, s.PayloadBytes-len(prefix)-len(suffix))
	return []byte(prefix + strings.Repeat("x", padding) + suffix)
}

// DuplicateIndexes picks which events get sent a second time. It samples without
// replacement, as the reference benchmark does.
func (s Stream) DuplicateIndexes(count int) []int {
	if count > s.EventCount {
		count = s.EventCount
	}
	random := rand.New(rand.NewPCG(s.Seed, 0xdeadbeefcafef00d))
	return random.Perm(s.EventCount)[:count]
}

// Duration returns the expected length of the arrival schedule.
func (s Stream) Duration() time.Duration {
	return time.Duration(math.Round(float64(s.EventCount) / s.RatePerSecond * float64(time.Second)))
}
