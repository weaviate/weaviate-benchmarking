package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var globalConfig Config

func init() {
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

	rootCmd.AddCommand(randomVectorsCmd)
	randomVectorsCmd.PersistentFlags().IntVarP(&globalConfig.Queries,
		"queries", "q", 100, "Set the number of queries the benchmarker should run")
	randomVectorsCmd.PersistentFlags().IntVarP(&globalConfig.Parallel,
		"parallel", "p", 8, "Set the number of parallel threads which send queries")
	randomVectorsCmd.PersistentFlags().IntVarP(&globalConfig.Limit,
		"limit", "l", 10, "Set the query limit (top_k)")
	randomVectorsCmd.PersistentFlags().IntVarP(&globalConfig.Dimensions,
		"dimensions", "d", 768, "Set the vector dimensions (must match your data)")
	randomVectorsCmd.PersistentFlags().StringVarP(&globalConfig.ClassName,
		"className", "c", "", "The Weaviate class to run the benchmark against")
	randomVectorsCmd.PersistentFlags().StringVar(&globalConfig.DB,
		"db", "weaviate", "The tool you're benchmarking")
	randomVectorsCmd.PersistentFlags().StringVarP(&globalConfig.API,
		"api", "a", "graphql", "The API to use on benchmarks")
	randomVectorsCmd.PersistentFlags().StringVarP(&globalConfig.Origin,
		"origin", "u", "http://localhost:8080", "The origin that Weaviate is running at")

	rootCmd.AddCommand(datasetCmd)
	initDataset()
}

var rootCmd = &cobra.Command{
	Use:   "benchmarker",
	Short: "Weaviate Benchmarker",
	Long:  `A Weaviate Benchmarker`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("running the root command, see help or -h for available commands\n")
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
