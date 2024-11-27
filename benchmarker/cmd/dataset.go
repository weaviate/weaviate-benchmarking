package cmd

import (
	"encoding/json"
	"io"
	"os"

	"github.com/spf13/cobra"
)

var datasetCmd = &cobra.Command{
	Use:   "dataset",
	Short: "Benchmark vectors from an existing dataset",
	Long:  `Specify an existing dataset as a list of query vectors in a .json file to parse the query vectors and then query them with the specified parallelism`,
	Run: func(cmd *cobra.Command, args []string) {
		cfg := globalConfig
		cfg.Mode = "dataset"

		if err := cfg.Validate(); err != nil {
			fatal(err)
		}

		q, err := parseVectorsFromFile(cfg)
		if err != nil {
			fatal(err)
		}

		var w io.Writer
		if cfg.OutputFile == "" {
			w = os.Stdout
		} else {
			f, err := os.Create(cfg.OutputFile)
			if err != nil {
				fatal(err)
			}

			defer f.Close()
			w = f

		}

		result := benchmarkDataset(cfg, q)
		if cfg.OutputFormat == "json" {
			result.WriteJSONTo(w)
		} else if cfg.OutputFormat == "text" {
			result.WriteTextTo(w)
		}

		if cfg.OutputFile != "" {
			infof("results succesfully written to %q", cfg.OutputFile)
		}
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
		"output", "o", "", "Filename for an output file. If none provided, output to stdout only")
}

type Queries [][]float32
type Neighbors [][]int

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

func benchmarkDataset(cfg Config, queries Queries) Results {
	cfg.Queries = len(queries)

	i := 0
	return benchmark(cfg, func(className string) QueryWithNeighbors {
		defer func() { i++ }()

		if cfg.API == "graphql" {
			return QueryWithNeighbors{
				Query: nearVectorQueryJSONGraphQL(cfg.ClassName, queries[i], cfg.Limit, cfg.WhereFilter),
			}

		}

		if cfg.API == "graphql-raw" {
			return QueryWithNeighbors{
				Query: nearVectorQueryJSONGraphQL(cfg.ClassName, queries[i], cfg.Limit, cfg.WhereFilter),
			}
		}

		if cfg.API == "rest" {
			return QueryWithNeighbors{
				Query: nearVectorQueryJSONRest(cfg.ClassName, queries[i], cfg.Limit),
			}
		}

		if cfg.API == "grpc" {
			return QueryWithNeighbors{
				Query: nearVectorQueryGrpc(cfg, queries[i], cfg.Limit, cfg.Tenant, 0),
			}
		}

		return QueryWithNeighbors{}
	})
}
