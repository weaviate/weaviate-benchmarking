package cmd

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestUuidFromInt(t *testing.T) {
	tests := []struct {
		input  int
		output string
	}{
		{0, "00000000-0000-0000-0000-000000000000"},
		{1, "00000000-0000-0000-0000-000000000001"},
		{255, "00000000-0000-0000-0000-0000000000ff"},
		{959797, "00000000-0000-0000-0000-0000000ea535"},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("Testing with input %d", test.input), func(t *testing.T) {
			got := uuidFromInt(test.input)
			if got != test.output {
				t.Errorf("For input %d, expected %s, but got %s", test.input, test.output, got)
			}
		})
	}
}

func TestIntFromUUID(t *testing.T) {
	tests := []struct {
		input  string
		output int
	}{
		{"00000000-0000-0000-0000-000000000000", 0},
		{"00000000-0000-0000-0000-000000000001", 1},
		{"00000000-0000-0000-0000-0000000000ff", 255},
		{"00000000-0000-0000-0000-0000000ea535", 959797},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("Testing with input %s", test.input), func(t *testing.T) {
			got := intFromUUID(test.input)
			if got != test.output {
				t.Errorf("For input %s, expected %d, but got %d", test.input, test.output, got)
			}
		})
	}
}

func TestAnalyzer(t *testing.T) {
	fmt.Println("TestAnalyzer")

	c := Config{
		Mode:         "random-vectors",
		Origin:       "http://localhost:8080",
		Queries:      10,
		QueriesFile:  "",
		Parallel:     8,
		Limit:        10,
		ClassName:    "",
		API:          "graphql",
		Dimensions:   768,
		DB:           "weaviate",
		WhereFilter:  "",
		OutputFormat: "text",
		OutputFile:   "",
	}

	durations := []time.Duration{
		time.Second * 5,
		time.Second * 3,
		time.Second * 0,
		time.Second * 1,
		time.Second * 2,
		time.Second * 4,
		time.Second * 6,
	}

	totalTime := time.Second * 21

	recall := []float64{0.7, 0.8, 0.9, 0.7, 0.8, 0.9, 0.2}

	ndcg := []float64{}

	t.Run("check analyze accuracy", func(t *testing.T) {
		results := analyze(c, durations, totalTime, recall, ndcg)

		require.Equal(t, 10, results.Total)
		require.Equal(t, 3, results.Failed)
		require.Equal(t, time.Second*6, results.Max)
		require.Equal(t, time.Second*0, results.Min)
		require.Equal(t, time.Second*3, results.Mean)
		require.Equal(t, 7.0/21.0, results.QueriesPerSecond)

		require.Equal(t, 5.0/7.0, results.Recall)

	})

}

func TestCalculateLinearNDCG(t *testing.T) {
	tests := []struct {
		name      string
		ids       []int
		neighbors []int
		k         int
		expected  float64
		tolerance float64
	}{
		{
			name:      "Perfect ranking - all neighbors at top",
			ids:       []int{1, 2, 3, 4, 5},
			neighbors: []int{1, 2, 3, 4, 5},
			k:         5,
			expected:  1.0,
			tolerance: 1e-10,
		},
		{
			name:      "Perfect ranking - partial k",
			ids:       []int{1, 2, 3, 6, 7},
			neighbors: []int{1, 2, 3, 4, 5},
			k:         3,
			expected:  1.0,
			tolerance: 1e-10,
		},
		{
			name:      "No relevant items in top-k",
			ids:       []int{6, 7, 8, 9, 10},
			neighbors: []int{1, 2, 3, 4, 5},
			k:         5,
			expected:  0.0,
			tolerance: 1e-10,
		},
		{
			name:      "Single relevant item at position 1",
			ids:       []int{1, 6, 7, 8, 9},
			neighbors: []int{1, 2, 3, 4, 5},
			k:         5,
			expected:  1.0 / (1.0 + 1.0/2.0 + 1.0/3.0 + 1.0/4.0 + 1.0/5.0),
			tolerance: 1e-10,
		},
		{
			name:      "Single relevant item at position 5",
			ids:       []int{6, 7, 8, 9, 1},
			neighbors: []int{1, 2, 3, 4, 5},
			k:         5,
			expected:  (1.0 / 5.0) / (1.0 + 1.0/2.0 + 1.0/3.0 + 1.0/4.0 + 1.0/5.0),
			tolerance: 1e-10,
		},
		{
			name:      "Missing most important neighbor (position 1)",
			ids:       []int{6, 1, 2, 3, 4},
			neighbors: []int{1, 2, 3, 4, 5},
			k:         5,
			expected:  (1.0/2.0 + 1.0/3.0 + 1.0/4.0 + 1.0/5.0) / (1.0 + 1.0/2.0 + 1.0/3.0 + 1.0/4.0 + 1.0/5.0),
			tolerance: 1e-10,
		},
		{
			name:      "High recall but poor ranking",
			ids:       []int{5, 4, 3, 2, 6},
			neighbors: []int{1, 2, 3, 4, 5},
			k:         5,
			expected:  (1.0/1.0 + 1.0/2.0 + 1.0/3.0 + 1.0/4.0) / (1.0 + 1.0/2.0 + 1.0/3.0 + 1.0/4.0 + 1.0/5.0),
			tolerance: 1e-10,
		},
		{
			name:      "Empty ids",
			ids:       []int{},
			neighbors: []int{1, 2, 3},
			k:         3,
			expected:  0.0,
			tolerance: 1e-10,
		},
		{
			name:      "k = 1",
			ids:       []int{1, 2, 3},
			neighbors: []int{1, 2, 3},
			k:         1,
			expected:  1.0,
			tolerance: 1e-10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := calculateLinearNDCG(tt.ids, tt.neighbors, tt.k)
			if math.Abs(result-tt.expected) > tt.tolerance {
				t.Errorf("calculateLinearNDCG() = %v, expected %v (diff: %v)", result, tt.expected, math.Abs(result-tt.expected))
			}
		})
	}
}
