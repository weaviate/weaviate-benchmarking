package cmd

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/spf13/cobra"
	"github.com/weaviate/weaviate-go-client/v4/weaviate"
	weaviategrpc "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/usecases/byteops"
	"google.golang.org/protobuf/proto"
)

func initRandomVectors() {
	rootCmd.AddCommand(randomVectorsCmd)
	numCPU := runtime.NumCPU()
	randomVectorsCmd.PersistentFlags().IntVarP(&globalConfig.Queries,
		"queries", "q", 100, "Set the number of queries the benchmarker should run")
	randomVectorsCmd.PersistentFlags().IntVar(&globalConfig.QueryDuration,
		"queryDuration", 0, "Instead of a fixed number of queries, query for the specified duration in seconds (default 0)")
	randomVectorsCmd.PersistentFlags().IntVarP(&globalConfig.Parallel,
		"parallel", "p", numCPU, "Set the number of parallel threads which send queries")
	randomVectorsCmd.PersistentFlags().StringVarP(&globalConfig.API,
		"api", "a", "grpc", "API (graphql | grpc) default and recommended is grpc")
	randomVectorsCmd.PersistentFlags().IntVarP(&globalConfig.Limit,
		"limit", "l", 10, "Set the query limit (top_k)")
	randomVectorsCmd.PersistentFlags().IntVarP(&globalConfig.Dimensions,
		"dimensions", "d", 0, "Set the vector dimensions (will infer from class if not set)")
	randomVectorsCmd.PersistentFlags().StringVarP(&globalConfig.ClassName,
		"className", "c", "", "The Weaviate class to run the benchmark against")
	randomVectorsCmd.PersistentFlags().StringVarP(&globalConfig.Origin,
		"grpcOrigin", "u", "localhost:50051", "The gRPC origin that Weaviate is running at")
	randomVectorsCmd.PersistentFlags().StringVar(&globalConfig.HttpOrigin,
		"httpOrigin", "localhost:8080", "The http origin for Weaviate (without http scheme)")
	randomVectorsCmd.PersistentFlags().StringVar(&globalConfig.HttpScheme,
		"httpScheme", "http", "The http scheme (http or https)")
}

var randomVectorsCmd = &cobra.Command{
	Use:   "random-vectors",
	Short: "Benchmark random vector queries",
	Long:  `Benchmark random vector queries`,
	Run: func(cmd *cobra.Command, args []string) {
		cfg := globalConfig
		cfg.Mode = "random-vectors"

		if err := cfg.Validate(); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		log.WithFields(log.Fields{"queries": cfg.Queries,
			"class": cfg.ClassName}).Info("Beginning random-vectors benchmark")

		client := createClient(&cfg)
		cfg.Dimensions = getDimensions(cfg, client)

		var result Results

		if cfg.QueryDuration > 0 {
			result = benchmarkNearVectorDuration(cfg)
		} else {
			result = benchmarkNearVector(cfg)
		}

		log.WithFields(log.Fields{"mean": result.Mean, "qps": result.QueriesPerSecond,
			"parallel": cfg.Parallel, "limit": cfg.Limit,
			"api": cfg.API, "count": result.Total, "failed": result.Failed}).Info("Benchmark result")

	},
}

func getDimensions(cfg Config, client *weaviate.Client) int {
	dimensions := cfg.Dimensions
	if cfg.Dimensions == 0 {
		// Try to infer dimensions from class

		objects, err := client.Data().ObjectsGetter().WithClassName(cfg.ClassName).WithVector().WithLimit(10).Do(context.Background())
		if err != nil {
			log.Infof("Error fetching class %s, %v", cfg.ClassName, err)
		}

		for _, obj := range objects {
			if obj.Vector != nil {
				dimensions = len(obj.Vector)
				break
			}
		}

		if dimensions == 0 {
			log.Fatalf("Could not fetch dimensions from class %s", cfg.ClassName)
		}
	}
	return dimensions
}

func randomVector(dims int) []float32 {
	vector := []float32{}

	for i := 0; i < dims; i++ {
		vector = append(vector, rand.Float32()*2-1)
	}

	return vector
}

func nearVectorQueryJSONGraphQLRaw(raw string) []byte {
	return []byte(fmt.Sprintf(`{
"query": "%s"
}`, raw))
}

func nearVectorQueryJSONGraphQL(className string, vec []float32, limit int, whereFilter string) []byte {
	vecJSON, _ := json.Marshal(vec)
	return []byte(fmt.Sprintf(`{
"query": "{ Get { %s(limit: %d, nearVector: {vector:%s}%s) { _additional { id } } } }"
}`, className, limit, string(vecJSON), whereFilter))
}

func encodeVector(fs []float32) []byte {
	buf := make([]byte, len(fs)*4)
	for i, f := range fs {
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(f))
	}
	return buf
}

func nearVectorQueryGrpc(cfg *Config, vec []float32, tenant string, filter int) []byte {

	var searchRequest *weaviategrpc.SearchRequest
	if cfg.MultiVectorDimensions > 0 {

		rows := len(vec) / cfg.MultiVectorDimensions
		doc := make([][]float32, rows)
		for i := 0; i < rows; i++ {
			start := i * cfg.MultiVectorDimensions
			end := start + cfg.MultiVectorDimensions
			doc[i] = vec[start:end]
		}
		multiVec := []*weaviategrpc.Vectors{{
			Name:        "multivector",
			VectorBytes: byteops.Fp32SliceOfSlicesToBytes(doc),
			Type:        weaviategrpc.VectorType_VECTOR_TYPE_MULTI_FP32,
		}}

		searchRequest = &weaviategrpc.SearchRequest{
			Collection: cfg.ClassName,
			Limit:      uint32(cfg.Limit),
			NearVector: &weaviategrpc.NearVector{
				Vectors: multiVec,
			},
			Metadata: &weaviategrpc.MetadataRequest{
				Certainty: false,
				Distance:  false,
				Uuid:      true,
			},
		}

	} else {
		searchRequest = &weaviategrpc.SearchRequest{
			Collection: cfg.ClassName,
			Limit:      uint32(cfg.Limit),
			NearVector: &weaviategrpc.NearVector{
				VectorBytes: encodeVector(vec),
			},
			Metadata: &weaviategrpc.MetadataRequest{
				Certainty: false,
				Distance:  false,
				Uuid:      true,
			},
		}
	}

	if tenant != "" {
		searchRequest.Tenant = tenant
	}

	if cfg.NamedVector != "" {
		searchRequest.NearVector = &weaviategrpc.NearVector{
			Targets: &weaviategrpc.Targets{
				TargetVectors: []string{cfg.NamedVector},
			},
			VectorPerTarget: map[string][]byte{
				cfg.NamedVector: encodeVector(vec),
			},
		}
	}

	if filter >= 0 {
		searchRequest.Filters = &weaviategrpc.Filters{
			TestValue: &weaviategrpc.Filters_ValueText{
				ValueText: strconv.Itoa(filter),
			},
			On:       []string{"category"},
			Operator: weaviategrpc.Filters_OPERATOR_EQUAL,
		}

	}

	data, err := proto.Marshal(searchRequest)
	if err != nil {
		fmt.Printf("grpc marshal err: %v\n", err)
	}

	return data
}

func benchmarkNearVector(cfg Config) Results {
	return benchmark(cfg, func(className string) QueryWithNeighbors {
		if cfg.API == "graphql" {
			return QueryWithNeighbors{
				Query: nearVectorQueryJSONGraphQL(cfg.ClassName, randomVector(cfg.Dimensions), cfg.Limit, cfg.WhereFilter),
			}
		}
		if cfg.API == "grpc" {
			return QueryWithNeighbors{
				Query: nearVectorQueryGrpc(&cfg, randomVector(cfg.Dimensions), cfg.Tenant, -1),
			}
		}

		return QueryWithNeighbors{}
	})
}

func benchmarkNearVectorDuration(cfg Config) Results {

	var samples sampledResults

	startTime := time.Now()

	var results Results
	iterations := 0
	for time.Since(startTime) < time.Duration(cfg.QueryDuration)*time.Second {
		results = benchmarkNearVector(cfg)
		samples.Min = append(samples.Min, results.Min)
		samples.Max = append(samples.Max, results.Max)
		samples.Mean = append(samples.Mean, results.Mean)
		samples.Took = append(samples.Took, results.Took)
		samples.QueriesPerSecond = append(samples.QueriesPerSecond, results.QueriesPerSecond)
		samples.Results = append(samples.Results, results)
		iterations += 1
	}

	var medianResult Results

	medianResult.Min = time.Duration(median(samples.Min))
	medianResult.Max = time.Duration(median(samples.Max))
	medianResult.Mean = time.Duration(median(samples.Mean))
	medianResult.Took = time.Duration(median(samples.Took))
	medianResult.QueriesPerSecond = median(samples.QueriesPerSecond)
	medianResult.Percentiles = results.Percentiles
	medianResult.PercentilesLabels = results.PercentilesLabels
	medianResult.Total = results.Total
	medianResult.Successful = results.Successful
	medianResult.Failed = results.Failed
	medianResult.Parallelization = cfg.Parallel

	log.WithFields(log.Fields{"iterations": iterations}).Infof("Queried for %d seconds", cfg.QueryDuration)

	return medianResult
}
