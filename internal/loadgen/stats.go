package loadgen

import (
	"math"
	"slices"
)

// Percentile returns the nearest-rank percentile of values, which is always a
// value that was actually observed. It sorts a copy. It returns 0 for no values.
func Percentile(values []float64, percent float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := slices.Clone(values)
	slices.Sort(sorted)
	rank := int(math.Ceil(percent / 100 * float64(len(sorted))))
	return sorted[max(rank, 1)-1]
}
