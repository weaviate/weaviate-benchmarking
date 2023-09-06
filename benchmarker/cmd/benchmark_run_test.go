package cmd

import (
	"fmt"
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

	t.Run("check analyze accuracy", func(t *testing.T) {
		results := analyze(c, durations, totalTime, recall)

		require.Equal(t, 10, results.Total)
		require.Equal(t, 3, results.Failed)
		require.Equal(t, time.Second*6, results.Max)
		require.Equal(t, time.Second*0, results.Min)
		require.Equal(t, time.Second*3, results.Mean)
		require.Equal(t, 7.0/21.0, results.QueriesPerSecond)

		require.Equal(t, 5.0/7.0, results.Recall)

	})

}
