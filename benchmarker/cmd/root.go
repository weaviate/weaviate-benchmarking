package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	queries    int
	limit      int
	parallel   int
	className  string
	dimensions int
	db         string
	api        string
)

func init() {
	rootCmd.AddCommand(randomTextCmd)
	randomTextCmd.PersistentFlags().IntVarP(&queries,
		"queries", "q", 100, "Set the number of queries the benchmarker should run")
	randomTextCmd.PersistentFlags().IntVarP(&parallel,
		"parallel", "p", 8, "Set the number of parallel threads which send queries")
	randomTextCmd.PersistentFlags().IntVarP(&limit,
		"limit", "l", 10, "Set the query limit (top_k)")
	randomTextCmd.PersistentFlags().StringVarP(&className,
		"className", "c", "", "The Weaviate class to run the benchmark against")
	randomTextCmd.PersistentFlags().StringVarP(&api,
		"api", "a", "graphql", "The API to use on benchmarks")

	rootCmd.AddCommand(randomVectorsCmd)
	randomVectorsCmd.PersistentFlags().IntVarP(&queries,
		"queries", "q", 100, "Set the number of queries the benchmarker should run")
	randomVectorsCmd.PersistentFlags().IntVarP(&parallel,
		"parallel", "p", 8, "Set the number of parallel threads which send queries")
	randomVectorsCmd.PersistentFlags().IntVarP(&limit,
		"limit", "l", 10, "Set the query limit (top_k)")
	randomVectorsCmd.PersistentFlags().IntVarP(&dimensions,
		"dimensions", "d", 768, "Set the vector dimensions (must match your data)")
	randomVectorsCmd.PersistentFlags().StringVarP(&className,
		"className", "c", "", "The Weaviate class to run the benchmark against")
	randomVectorsCmd.PersistentFlags().StringVar(&db,
		"db", "weaviate", "The tool you're benchmarking")
	randomVectorsCmd.PersistentFlags().StringVarP(&api,
		"api", "a", "graphql", "The API to use on benchmarks")
}

var rootCmd = &cobra.Command{
	Use:   "benchmarker",
	Short: "Weaviate Benchmarker",
	Long:  `A Weaviate Benchmarker`,
	Run: func(cmd *cobra.Command, args []string) {
		// Do Stuff Here
		fmt.Printf("running the root command\n")
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
