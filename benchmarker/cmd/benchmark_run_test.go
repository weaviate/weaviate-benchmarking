package cmd

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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

	t.Run("check analyze accuracy", func(t *testing.T) {
		results := analyze(c, durations, totalTime)

		require.Equal(t, 10, results.Total)
		require.Equal(t, 3, results.Failed)
		require.Equal(t, time.Second*6, results.Max)
		require.Equal(t, time.Second*0, results.Min)
		require.Equal(t, time.Second*3, results.Mean)
		require.Equal(t, 7.0/21.0, results.QueriesPerSecond)

	})

}
