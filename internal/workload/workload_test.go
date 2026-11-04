package workload

import (
	"encoding/json"
	"math"
	"testing"
	"time"
)

func testStream() Stream {
	return Stream{TenantID: "t", SourceID: "s", Seed: 7, EventCount: 20_000, RatePerSecond: 1000, PayloadBytes: 1024}
}

func TestSameSeedGivesTheSameStream(t *testing.T) {
	first, second := testStream(), testStream()
	if string(first.Body(5, 99)) != string(second.Body(5, 99)) {
		t.Fatal("body is not deterministic")
	}
	a, b := first.ArrivalOffsets(), second.ArrivalOffsets()
	for index := range a {
		if a[index] != b[index] {
			t.Fatalf("offset %d differs", index)
		}
	}
	other := testStream()
	other.Seed = 8
	if other.AmountCents(5) == first.AmountCents(5) && other.AmountCents(6) == first.AmountCents(6) {
		t.Fatal("a different seed should give different amounts")
	}
}

func TestBodyIsExactSizeValidJSONAndHoldsTheAmount(t *testing.T) {
	stream := testStream()
	for _, index := range []int{0, 7, 19_999} {
		body := stream.Body(index, 1_793_000_000_000_000_000)
		if len(body) != stream.PayloadBytes {
			t.Fatalf("index %d: %d bytes", index, len(body))
		}
		var parsed struct {
			ReceiptID   string `json:"receipt_id"`
			AmountCents int64  `json:"amount_cents"`
		}
		if err := json.Unmarshal(body, &parsed); err != nil {
			t.Fatal(err)
		}
		if parsed.AmountCents != stream.AmountCents(index) || parsed.ReceiptID != stream.ReceiptID(index) {
			t.Fatalf("index %d: %+v", index, parsed)
		}
		if parsed.AmountCents < 1 || parsed.AmountCents > 100_000 {
			t.Fatalf("amount out of range: %d", parsed.AmountCents)
		}
	}
}

func TestArrivalsAreAPoissonProcessAtTheDeclaredRate(t *testing.T) {
	stream := testStream()
	offsets := stream.ArrivalOffsets()
	seconds := offsets[len(offsets)-1].Seconds()
	measuredRate := float64(len(offsets)) / seconds
	if math.Abs(measuredRate-stream.RatePerSecond)/stream.RatePerSecond > 0.03 {
		t.Fatalf("measured rate %.1f", measuredRate)
	}
	// For exponential gaps the standard deviation equals the mean.
	var sum, sumSquares float64
	previous := time.Duration(0)
	for _, offset := range offsets {
		gap := (offset - previous).Seconds()
		previous = offset
		sum += gap
		sumSquares += gap * gap
	}
	count := float64(len(offsets))
	mean := sum / count
	deviation := math.Sqrt(sumSquares/count - mean*mean)
	if ratio := deviation / mean; ratio < 0.95 || ratio > 1.05 {
		t.Fatalf("gap deviation over mean is %.3f, want about 1", ratio)
	}
}

func TestDuplicateIndexesAreDistinct(t *testing.T) {
	indexes := testStream().DuplicateIndexes(4000)
	seen := map[int]bool{}
	for _, index := range indexes {
		if seen[index] {
			t.Fatalf("index %d chosen twice", index)
		}
		seen[index] = true
	}
	if len(seen) != 4000 {
		t.Fatalf("got %d", len(seen))
	}
}
