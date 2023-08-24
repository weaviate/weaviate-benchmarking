package cmd

import (
	"fmt"
	"math/rand"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

func initRandomText() {
	rootCmd.AddCommand(randomTextCmd)
	randomTextCmd.PersistentFlags().IntVarP(&globalConfig.Queries,
		"queries-file", "f", 100, "Set the number of queries the benchmarker should run")
	randomTextCmd.PersistentFlags().IntVarP(&globalConfig.Parallel,
		"parallel", "p", 8, "Set the number of parallel threads which send queries")
	randomTextCmd.PersistentFlags().IntVarP(&globalConfig.Limit,
		"limit", "l", 10, "Set the query limit (top_k)")
	randomTextCmd.PersistentFlags().StringVarP(&globalConfig.ClassName,
		"className", "c", "", "The Weaviate class to run the benchmark against")
	randomTextCmd.PersistentFlags().StringVarP(&globalConfig.API,
		"api", "a", "graphql", "The API to use on benchmarks")
	randomTextCmd.PersistentFlags().StringVarP(&globalConfig.Origin,
		"origin", "u", "http://localhost:8080", "The origin that Weaviate is running at")
}

var randomTextCmd = &cobra.Command{
	Use:   "random-text",
	Short: "Benchmark nearText searches",
	Long:  `Benchmark random nearText searches`,
	Run: func(cmd *cobra.Command, args []string) {
		cfg := globalConfig

		cfg.Mode = "random-text"
		if err := cfg.Validate(); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		result := benchmarkNearText(cfg)
		result.WriteTextTo(os.Stdout)
	},
}

func randomSearchString(maxLength int) string {
	words := []string{}

	for i := 0; i < maxLength; i++ {
		words = append(words, Nouns[rand.Intn(len(Nouns))])
	}

	return strings.Join(words, " ")
}

func nearTextQueryJSON(className string, query string) []byte {
	return []byte(fmt.Sprintf(`{
"query": "{ Get { %s(limit: 10, nearText: {concepts:[\"%s\"]}) { title } } }" 
}`, className, query))
}

func benchmarkNearText(cfg Config) Results {
	return benchmark(cfg, func(className string) []byte {
		return nearTextQueryJSON(className, randomSearchString(4))
	})
}
