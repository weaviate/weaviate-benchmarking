package cmd

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"

	"github.com/spf13/cobra"
)

var randomVectorsCmd = &cobra.Command{
	Use:   "random-vectors",
	Short: "Benchmark nearVector searches",
	Long:  `Benchmark random nearVector searches`,
	Run: func(cmd *cobra.Command, args []string) {
		if className == "" {
			fmt.Printf("className must be set\n")
			os.Exit(1)
		}

		if dimensions == 0 {
			fmt.Printf("dimenstions must be set and larger than 0\n")
			os.Exit(1)
		}

		if db == "weaviate" {
			benchmarkNearVector()
			return
		}

		if db == "opendistro" {
			benchmarkOpendistroVector()
			return
		}

		fmt.Printf("unrecognized db\n")
		os.Exit(1)
	},
}

func randomVector() []float32 {
	vector := []float32{}

	for i := 0; i < dimensions; i++ {
		vector = append(vector, rand.Float32()*2-1)
	}

	return vector
}

func nearVectorQueryJSONGraphQL(className string, vec []float32) []byte {
	vecJSON, _ := json.Marshal(vec)
	return []byte(fmt.Sprintf(`{
"query": "{ Get { %s(limit: %d, nearVector: {vector:%s}) { _additional { id } } } }" 
}`, className, limit, string(vecJSON)))
}

func nearVectorQueryJSONRest(className string, vec []float32) []byte {
	vecJSON, _ := json.Marshal(vec)
	return []byte(fmt.Sprintf(`{
		"nearVector":{"vector":%s},
		"limit":%d
}`, string(vecJSON), limit))
}

func benchmarkNearVector() {
	benchmark(func(className string) []byte {
		if api == "graphql" {
			return nearVectorQueryJSONGraphQL(className, randomVector())
		}

		if api == "rest" {
			return nearVectorQueryJSONRest(className, randomVector())
		}

		fmt.Printf("unknown api\n")
		os.Exit(1)

		return nil
	})
}
