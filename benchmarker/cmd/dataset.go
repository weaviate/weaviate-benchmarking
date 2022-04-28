package cmd

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var datasetCmd = &cobra.Command{
	Use:   "dataset",
	Short: "Benchmark vectors from an existing dataset",
	Long:  `Specify an existing dataset as a list of query vectors in a .json file to parse the query vectors and then query them with the specified parallelism`,
	Run: func(cmd *cobra.Command, args []string) {
		cfg := globalConfig
		cfg.Mode = "random-vectors"

		if err := cfg.Validate(); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		q, err := parseVectorsFromFile(cfg)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

		benchmarkDataset(cfg, q)

		fmt.Println("not implemented yet")
		os.Exit(1)
	},
}

func initDataset() {
	rootCmd.AddCommand(datasetCmd)
	datasetCmd.PersistentFlags().StringVarP(&globalConfig.QueriesFile,
		"queries", "q", "", "Point to the queries file, (.json)")
	datasetCmd.PersistentFlags().IntVarP(&globalConfig.Parallel,
		"parallel", "p", 8, "Set the number of parallel threads which send queries")
	datasetCmd.PersistentFlags().IntVarP(&globalConfig.Limit,
		"limit", "l", 10, "Set the query limit (top_k)")
	datasetCmd.PersistentFlags().StringVarP(&globalConfig.ClassName,
		"className", "c", "", "The Weaviate class to run the benchmark against")
	datasetCmd.PersistentFlags().StringVarP(&globalConfig.WhereFilter,
		"where", "w", "", "An entire where filter as a string")
	datasetCmd.PersistentFlags().StringVarP(&globalConfig.API,
		"api", "a", "graphql", "The API to use on benchmarks")
	datasetCmd.PersistentFlags().StringVarP(&globalConfig.Origin,
		"origin", "u", "http://localhost:8080", "The origin that Weaviate is running at")
	datasetCmd.PersistentFlags().StringVarP(&globalConfig.OutputFormat,
		"format", "f", "text", "Output format, one of [text, json]")
	datasetCmd.PersistentFlags().StringVarP(&globalConfig.OutputFile,
		"output", "o", "output", "Filename for an output file. If none provided, output to stdout only")
}

type Queries [][]float32

func parseVectorsFromFile(cfg Config) (Queries, error) {
	var q Queries
	f, err := os.Open(cfg.QueriesFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if err := json.NewDecoder(f).Decode(&q); err != nil {
		return nil, err
	}

	return q, nil
}

func benchmarkDataset(cfg Config, queries Queries) {
	cfg.Queries = len(queries)

	i := 0
	benchmark(cfg, func(className string) []byte {
		defer func() { i++ }()

		if cfg.API == "graphql" {
			return nearVectorQueryJSONGraphQL(cfg.ClassName, queries[i], cfg.Limit)
		}

		if cfg.API == "rest" {
			return nearVectorQueryJSONRest(cfg.ClassName, queries[i], cfg.Limit)
		}

		return nil
	})
}
