package cmd

import (
	"math"
	"testing"
)

func approxEqual(a, b float64) bool { return math.Abs(a-b) < 1e-6 }

func TestCalculateGradedNDCG(t *testing.T) {
	tests := []struct {
		name     string
		ids      []int
		relevant map[int]int
		cutoff   int
		want     float64
	}{
		{"perfect graded", []int{0, 1, 2}, map[int]int{0: 2, 1: 1}, 10, 1.0},
		{"reversed", []int{1, 0}, map[int]int{0: 2, 1: 1}, 10, 2.2618595 / 2.6309298},
		{"relevant lower down", []int{7, 8, 0, 1}, map[int]int{0: 2, 1: 1}, 10, (2.0/math.Log2(4) + 1.0/math.Log2(5)) / 2.6309298},
		{"empty relevant", []int{0, 1}, map[int]int{}, 10, 0.0},
		{"grade-0 ignored", []int{0}, map[int]int{0: 0, 1: 2}, 10, 0.0},
		{"cutoff truncates", []int{5, 0}, map[int]int{0: 1, 5: 1}, 1, 1.0}, // only pos 0 (id5, rel) counted; IDCG@1=1
		{"fewer results than cutoff", []int{0}, map[int]int{0: 1, 1: 1}, 10, (1.0) / (1.0 + 1.0/math.Log2(3))},
	}
	for _, tc := range tests {
		got := calculateGradedNDCG(tc.ids, tc.relevant, tc.cutoff)
		if !approxEqual(got, tc.want) {
			t.Errorf("%s: NDCG = %.7f, want %.7f", tc.name, got, tc.want)
		}
	}
}

func TestRecallVsRelevant(t *testing.T) {
	tests := []struct {
		name     string
		ids      []int
		relevant map[int]int
		cutoff   int
		want     float64
	}{
		{"partial", []int{0, 1, 9}, map[int]int{0: 1, 1: 1, 2: 1}, 10, 2.0 / 3.0},
		{"cutoff limits found", []int{0, 1, 2}, map[int]int{0: 1, 1: 1, 2: 1}, 2, 2.0 / 3.0},
		{"grade-0 not relevant", []int{0, 1}, map[int]int{0: 1, 1: 0}, 10, 1.0},
		{"all found", []int{2, 1, 0}, map[int]int{0: 2, 1: 1, 2: 1}, 10, 1.0},
		{"empty relevant", []int{0}, map[int]int{}, 10, 0.0},
	}
	for _, tc := range tests {
		got := recallVsRelevant(tc.ids, tc.relevant, tc.cutoff)
		if !approxEqual(got, tc.want) {
			t.Errorf("%s: recall = %.7f, want %.7f", tc.name, got, tc.want)
		}
	}
}
