package cmd

import (
	"fmt"
	"math/rand"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

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
		benchmarkNearText(cfg)
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

func benchmarkNearText(cfg Config) {
	benchmark(cfg, func(className string) []byte {
		return nearTextQueryJSON(className, randomSearchString(4))
	})
}
