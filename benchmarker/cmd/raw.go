package cmd

import (
	"bufio"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

var rawCmd = &cobra.Command{
	Use:   "raw",
	Short: "Benchmark raw GraphQL queries",
	Long:  `Specify an existing dataset as a list of GraphQL queries`,
	Run: func(cmd *cobra.Command, args []string) {
		cfg := globalConfig
		cfg.Mode = "dataset"

		if err := cfg.Validate(); err != nil {
			fatal(err)
		}

		q, err := parseQueriesFromFile(cfg)
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

		result := benchmarkRaw(cfg, q)
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

func initRaw() {
	rootCmd.AddCommand(rawCmd)
	rawCmd.PersistentFlags().StringVarP(&globalConfig.QueriesFile,
		"queries", "q", "", "Point to the queries file, (.txt)")
	rawCmd.PersistentFlags().IntVarP(&globalConfig.Parallel,
		"parallel", "p", 8, "Set the number of parallel threads which send queries")
	rawCmd.PersistentFlags().StringVarP(&globalConfig.API,
		"api", "a", "graphql", "The API to use on benchmarks")
	rawCmd.PersistentFlags().StringVarP(&globalConfig.Origin,
		"origin", "u", "http://localhost:8080", "The origin that Weaviate is running at")
	rawCmd.PersistentFlags().StringVarP(&globalConfig.OutputFormat,
		"format", "f", "text", "Output format, one of [text, json]")
	rawCmd.PersistentFlags().StringVarP(&globalConfig.OutputFile,
		"output", "o", "", "Filename for an output file. If none provided, output to stdout only")
}

func parseQueriesFromFile(cfg Config) ([]string, error) {
	f, err := os.Open(cfg.QueriesFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var lines []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		line = strings.Replace(line, "\"", "\\\"", -1)
		lines = append(lines, line)

	}
	return lines, scanner.Err()
}

func benchmarkRaw(cfg Config, queries []string) Results {
	cfg.Queries = len(queries)

	i := 0
	return benchmark(cfg, func(className string) []byte {
		defer func() { i++ }()
		return nearVectorQueryJSONGraphQLRaw(queries[i])
	})
}
