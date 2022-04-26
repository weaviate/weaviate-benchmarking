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
		cfg := globalConfig
		cfg.Mode = "random-vectors"

		if err := cfg.Validate(); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		if cfg.DB == "weaviate" {
			benchmarkNearVector(cfg)
			return
		}

		// 		if cfg.DB == "opendistro" {
		// 			benchmarkOpendistroVector()
		// 			return
		// 		}

		fmt.Printf("unrecognized db\n")
		os.Exit(1)
	},
}

func randomVector(dims int) []float32 {
	vector := []float32{}

	for i := 0; i < dims; i++ {
		vector = append(vector, rand.Float32()*2-1)
	}

	return vector
}

func nearVectorQueryJSONGraphQL(className string, vec []float32, limit int) []byte {
	vecJSON, _ := json.Marshal(vec)
	return []byte(fmt.Sprintf(`{
"query": "{ Get { %s(limit: %d, nearVector: {vector:%s}) { _additional { id } } } }" 
}`, className, limit, string(vecJSON)))
}

func nearVectorQueryJSONRest(className string, vec []float32, limit int) []byte {
	vecJSON, _ := json.Marshal(vec)
	return []byte(fmt.Sprintf(`{
		"nearVector":{"vector":%s},
		"limit":%d
}`, string(vecJSON), limit))
}

func benchmarkNearVector(cfg Config) {
	benchmark(cfg, func(className string) []byte {
		if cfg.API == "graphql" {
			return nearVectorQueryJSONGraphQL(cfg.ClassName, randomVector(cfg.Dimensions), cfg.Limit)
		}

		if cfg.API == "rest" {
			return nearVectorQueryJSONRest(cfg.ClassName, randomVector(cfg.Dimensions), cfg.Limit)
		}

		return nil
	})
}
