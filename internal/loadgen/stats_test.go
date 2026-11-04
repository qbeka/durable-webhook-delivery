package loadgen

import "testing"

func TestPercentileUsesNearestRank(t *testing.T) {
	values := []float64{15, 20, 35, 40, 50}
	cases := []struct {
		percent float64
		want    float64
	}{{5, 15}, {30, 20}, {40, 20}, {50, 35}, {100, 50}}
	for _, testCase := range cases {
		if got := Percentile(values, testCase.percent); got != testCase.want {
			t.Errorf("p%v: got %v, want %v", testCase.percent, got, testCase.want)
		}
	}
	if Percentile(nil, 50) != 0 {
		t.Error("empty input should give 0")
	}
}

func TestPercentileLeavesItsInputUnsorted(t *testing.T) {
	values := []float64{3, 1, 2}
	Percentile(values, 50)
	if values[0] != 3 {
		t.Fatal("input was sorted in place")
	}
}
