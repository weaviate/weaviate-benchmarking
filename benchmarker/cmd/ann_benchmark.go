package cmd

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate/entities/models"
	weaviategrpc "github.com/weaviate/weaviate/grpc"
	"gonum.org/v1/hdf5"
	"google.golang.org/grpc"
)

// Batch of vectors and offset for writing to Weaviate
type Batch struct {
	Vectors [][]float32
	Offset  int
}

// Weaviate https://github.com/weaviate/weaviate-chaos-engineering/tree/main/apps/ann-benchmarks style format
type ResultsJSONBenchmark struct {
	Api              string  `json:"api"`
	Ef               int     `json:"ef"`
	EfConstruction   int     `json:"efConstruction"`
	MaxConnections   int     `json:"maxConnections"`
	Mean             float64 `json:"mean"`
	QueriesPerSecond float64 `json:"qps"`
	Shards           int     `json:"shards"`
	Parallelization  int     `json:"parallelization"`
	RunID            string  `json:"run_id"`
	Dataset          string  `json:"dataset_file"`
	Recall           float64 `json:"recall"`
}

// Convert an int to a uuid formatted string
func uuidFromInt(val int) string {
	bytes := make([]byte, 16)
	binary.BigEndian.PutUint64(bytes[8:], uint64(val))
	id, err := uuid.FromBytes(bytes)
	if err != nil {
		panic(err)
	}

	return id.String()
}

// Convert a uuid formatted string to an int32
func int32FromUUID(uuidStr string) int32 {
	id, err := uuid.Parse(uuidStr)
	if err != nil {
		panic(err)
	}
	val := binary.BigEndian.Uint64(id[8:])
	return int32(val)
}

// Writes a single batch of vectors to Weaviate using gRPC
func writeChunk(chunk *Batch, client *weaviategrpc.WeaviateClient, cfg *Config) {

	objects := make([]*weaviategrpc.BatchObject, len(chunk.Vectors))

	for i, vector := range chunk.Vectors {
		objects[i] = &weaviategrpc.BatchObject{
			Uuid:      uuidFromInt(i + chunk.Offset),
			Vector:    vector,
			ClassName: cfg.ClassName,
		}
	}

	batchRequest := &weaviategrpc.BatchObjectsRequest{
		Objects: objects,
	}

	// 4. Use the BatchObjects RPC method to send the batch of vectors.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	response, err := (*client).BatchObjects(ctx, batchRequest)
	if err != nil {
		log.Fatalf("could not send batch: %v", err)
	}

	for _, result := range response.GetResults() {
		if result.Error != "" {
			log.Printf("Error for index %d: %s", result.Index, result.Error)
		} else {
			log.Printf("Successfully processed object at index %d", result.Index)
		}
	}

}

// Re/create Weaviate schema
func createSchema(cfg *Config) {
	wcfg := weaviate.Config{
		Host:   strings.Replace(cfg.Origin, "50051", "8080", 1),
		Scheme: "http",
	}
	client, err := weaviate.NewClient(wcfg)
	if err != nil {
		panic(err)
	}

	err = client.Schema().ClassDeleter().WithClassName(cfg.ClassName).Do(context.Background())
	if err != nil {
		panic(err)
	}

	classObj := &models.Class{
		Class:       cfg.ClassName,
		Description: fmt.Sprintf("Created by the Weaviate Benchmarker at %s", time.Now().String()),
		VectorIndexConfig: map[string]interface{}{
			"distance":       cfg.DistanceMetric,
			"efConstruction": float64(cfg.EfConstruction),
			"maxConnections": float64(cfg.MaxConnections),
		},
	}

	err = client.Schema().ClassCreator().WithClass(classObj).Do(context.Background())
	if err != nil {
		panic(err)
	}
	log.Printf("Created class %s", cfg.ClassName)
}

// Update ef parameter on the Weaviate schema
func updateEf(ef int, cfg *Config) {
	wcfg := weaviate.Config{
		Host:   strings.Replace(cfg.Origin, "50051", "8080", 1),
		Scheme: "http",
	}
	client, err := weaviate.NewClient(wcfg)
	if err != nil {
		panic(err)
	}

	classConfig, err := client.Schema().ClassGetter().WithClassName(cfg.ClassName).Do(context.Background())
	if err != nil {
		panic(err)
	}

	vectorIndexConfig := classConfig.VectorIndexConfig.(map[string]interface{})
	vectorIndexConfig["ef"] = ef
	classConfig.VectorIndexConfig = vectorIndexConfig

	err = client.Schema().ClassUpdater().WithClass(classConfig).Do(context.Background())

	if err != nil {
		panic(err)
	}

	// log.Printf("Updated ef to %f\n", ef)
}

// Load a large dataset from an hdf5 file and stream it to Weaviate
func loadHdf5Streaming(dataset *hdf5.Dataset, chunks chan<- Batch, cfg * Config) {
	dataspace := dataset.Space()
	dims, _, _ := dataspace.SimpleExtentDims()

	if len(dims) != 2 {
		log.Fatal("expected 2 dimensions")
	}

	rows := dims[0]
	dimensions := dims[1]

	batchSize := uint(cfg.BatchSize)

	log.WithFields(log.Fields{"rows": rows, "dimensions": dimensions}).Printf(
		"Reading HDF5 dataset")

	memspace, err := hdf5.CreateSimpleDataspace([]uint{batchSize, dimensions}, []uint{batchSize, dimensions})
	if err != nil {
		log.Fatalf("Error creating memspace: %v", err)
	}
	defer memspace.Close()

	for i := uint(0); i < rows; i += batchSize {

		offset := []uint{i, 0}
		count := []uint{batchSize, dimensions}

		if err := dataspace.SelectHyperslab(offset, nil, count, nil); err != nil {
			log.Fatalf("Error selecting hyperslab: %v", err)
		}

		chunkData1D := make([]float32, batchSize*dimensions)

		if err := dataset.ReadSubset(&chunkData1D, memspace, dataspace); err != nil {
			log.Fatalf("Error reading subset: %v", err)
		}

		chunkData := make([][]float32, batchSize)
		for i := range chunkData {
			chunkData[i] = chunkData1D[i*int(dimensions) : (i+1)*int(dimensions)]
		}

		if (i+batchSize)%10000 == 0 {
			log.Printf("Imported %d/%d rows", i+batchSize, rows)
		}

		chunks <- Batch{Vectors: chunkData, Offset: int(i)}
	}
}

// Read an entire dataset from an hdf5 file at once
func loadHdf5Float32(file *hdf5.File, name string) [][]float32 {
	dataset, err := file.OpenDataset(name)
	if err != nil {
		log.Fatalf("Error opening loadHdf5Float32 dataset: %v", err)
	}
	defer dataset.Close()
	dataspace := dataset.Space()
	dims, _, _ := dataspace.SimpleExtentDims()

	if len(dims) != 2 {
		log.Fatal("expected 2 dimensions")
	}

	rows := dims[0]
	dimensions := dims[1]

	chunkData1D := make([]float32, rows*dimensions)

	dataset.Read(&chunkData1D)

	chunkData := make([][]float32, rows)
	for i := range chunkData {
		chunkData[i] = chunkData1D[i*int(dimensions) : (i+1)*int(dimensions)]
	}
	return chunkData
}

// Read an entire dataset from an hdf5 file at once (neighbours)
func loadHdf5Neighbors(file *hdf5.File, name string) [][]int32 {
	dataset, err := file.OpenDataset(name)
	if err != nil {
		log.Fatalf("Error opening neighbors dataset: %v", err)
	}
	defer dataset.Close()
	dataspace := dataset.Space()
	dims, _, _ := dataspace.SimpleExtentDims()

	if len(dims) != 2 {
		log.Fatal("expected 2 dimensions")
	}

	rows := dims[0]
	dimensions := dims[1]

	chunkData1D := make([]int32, rows*dimensions)

	dataset.Read(&chunkData1D)

	chunkData := make([][]int32, rows)
	for i := range chunkData {
		chunkData[i] = chunkData1D[i*int(dimensions) : (i+1)*int(dimensions)]
	}
	return chunkData
}

// Load an hdf5 file in the format of ann-benchmarks.com
func loadANNBenchmarksFile(file *hdf5.File, cfg *Config) {

	createSchema(cfg)

	startTime := time.Now()
	dataset, err := file.OpenDataset("train")
	if err != nil {
		log.Fatalf("Error opening dataset: %v", err)
	}
	defer dataset.Close()

	chunks := make(chan Batch, 10)

	go func() {
		loadHdf5Streaming(dataset, chunks, cfg)
		close(chunks)
	}()

	var wg sync.WaitGroup

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			grpcConn, err := grpc.Dial(cfg.Origin, grpc.WithInsecure(), grpc.WithBlock())
			if err != nil {
				log.Fatalf("Did not connect: %v", err)
			}
			defer grpcConn.Close()

			client := weaviategrpc.NewWeaviateClient(grpcConn)
			for chunk := range chunks {
				writeChunk(&chunk, &client, cfg)
			}
		}()
	}

	wg.Wait()

	endTime := time.Now()
	log.Printf("Total import time: %v\n", endTime.Sub(startTime))
	sleepDuration := 5 & time.Second
	log.Printf("Waiting for %s seconds to allow for compaction etc\n", sleepDuration)
	time.Sleep(sleepDuration)
}

var annBenchmarkCommand = &cobra.Command{
	Use:   "ann-benchmark",
	Short: "Benchmark ANN Benchmark style hdf5 files",
	Long:  `Specify an existing dataset as a list of GraphQL queries`,
	Run: func(cmd *cobra.Command, args []string) {

		cfg := globalConfig
		cfg.Mode = "ann-benchmark"

		if err := cfg.Validate(); err != nil {
			fatal(err)
		}

		cfg.parseLabels()

		runID := strconv.FormatInt(time.Now().Unix(), 10)

		file, err := hdf5.OpenFile(cfg.BenchmarkFile, hdf5.F_ACC_RDONLY)
		if err != nil {
			log.Fatalf("Error opening file: %v", err)
		}
		defer file.Close()

		if !cfg.QueryOnly {
			log.WithFields(log.Fields{"efC": cfg.EfConstruction, "m": cfg.MaxConnections, "shards": cfg.Shards,
				"distance": cfg.DistanceMetric, "dataset": cfg.BenchmarkFile}).Info("Starting import")
			loadANNBenchmarksFile(file, &cfg)
		}

		log.WithFields(log.Fields{"efC": cfg.EfConstruction, "m": cfg.MaxConnections, "shards": cfg.Shards,
			"distance": cfg.DistanceMetric, "dataset": cfg.BenchmarkFile}).Info("Starting query")

		neighbors := loadHdf5Neighbors(file, "neighbors")
		testData := loadHdf5Float32(file, "test")

		efCandidates := []int{
			16,
			24,
			32,
			48,
			64,
			96,
			128,
			256,
			512,
		}

		var benchmarkResultsMap []map[string]interface{}

		for _, ef := range efCandidates {
			updateEf(ef, &cfg)
			result := benchmarkANN(cfg, testData, neighbors)

			log.WithFields(log.Fields{"mean": result.Mean, "qps": result.QueriesPerSecond, "recall": result.Recall,
			"api": cfg.API, "ef": ef, "count": result.Total, "failed": result.Failed}).Info("Benchmark result")

			dataset := filepath.Base(cfg.BenchmarkFile)

			var resultMap map[string]interface{}

			benchResult := ResultsJSONBenchmark{
				Api:              cfg.API,
				Ef:               ef,
				EfConstruction:   cfg.EfConstruction,
				MaxConnections:   cfg.MaxConnections,
				Mean:             result.Mean.Seconds(),
				QueriesPerSecond: result.QueriesPerSecond,
				Shards:           cfg.Shards,
				Parallelization:  cfg.Parallel,
				RunID:            runID,
				Dataset:          dataset,
				Recall:           result.Recall,
			}

			jsonData, err := json.Marshal(benchResult)
			if err != nil {
				log.Fatalf("Error converting result to json")
			}

			if err := json.Unmarshal(jsonData, &resultMap); err != nil {
				log.Fatalf("Error converting json to map")
			}

			if cfg.LabelMap != nil {
				for key, value := range(cfg.LabelMap) {
					resultMap[key] = value
				}
			}

			benchmarkResultsMap = append(benchmarkResultsMap, resultMap)

		}

		data, err := json.MarshalIndent(benchmarkResultsMap, "", "    ")
		if err != nil {
			log.Fatalf("Error marshaling benchmark results: %v", err)
		}

		os.Mkdir("./results", 0755)

		err = os.WriteFile(fmt.Sprintf("./results/%s.json", runID), data, 0644)
		if err != nil {
			log.Fatalf("Error writing benchmark results to file: %v", err)
		}

	},
}

func initAnnBenchmark() {
	rootCmd.AddCommand(annBenchmarkCommand)
	annBenchmarkCommand.PersistentFlags().StringVarP(&globalConfig.Labels,
		"labels", "l", "", "Labels of format key1=value1,key2=value2,...")
	annBenchmarkCommand.PersistentFlags().StringVarP(&globalConfig.BenchmarkFile,
		"vectors", "v", "", "Path to the hdf5 file containing the vectors")
	annBenchmarkCommand.PersistentFlags().StringVarP(&globalConfig.ClassName,
		"className", "c", "Vector", "Class name for testing")
	annBenchmarkCommand.PersistentFlags().StringVarP(&globalConfig.DistanceMetric,
		"distance", "d", "", "Set distance metric (mandatory)")
	annBenchmarkCommand.PersistentFlags().BoolVarP(&globalConfig.QueryOnly,
		"query", "q", false, "Do not import data and only run query tests")
	annBenchmarkCommand.PersistentFlags().IntVar(&globalConfig.EfConstruction,
		"efConstruction", 256, "Set Weaviate efConstruction parameter (default 256)")
	annBenchmarkCommand.PersistentFlags().IntVar(&globalConfig.MaxConnections,
		"maxConnections", 16, "Set Weaviate efConstruction parameter (default 16)")
	annBenchmarkCommand.PersistentFlags().IntVar(&globalConfig.Shards,
		"shards", 1, "Set number of Weaviate shards")
	annBenchmarkCommand.PersistentFlags().IntVarP(&globalConfig.BatchSize,
		"batchSize", "b", 1000, "Batch size for insert operations")
	annBenchmarkCommand.PersistentFlags().IntVarP(&globalConfig.Parallel,
		"parallel", "p", 8, "Set the number of parallel threads which send queries")
	annBenchmarkCommand.PersistentFlags().StringVarP(&globalConfig.API,
		"api", "a", "grpc", "The API to use on benchmarks")
	annBenchmarkCommand.PersistentFlags().StringVarP(&globalConfig.Origin,
		"origin", "u", "localhost:50051", "The origin that Weaviate is running at")
	annBenchmarkCommand.PersistentFlags().StringVarP(&globalConfig.OutputFormat,
		"format", "f", "text", "Output format, one of [text, json]")
	annBenchmarkCommand.PersistentFlags().StringVarP(&globalConfig.OutputFile,
		"output", "o", "", "Filename for an output file. If none provided, output to stdout only")
}

func benchmarkANN(cfg Config, queries Queries, neighbors Neighbors) Results {
	cfg.Queries = len(queries)

	i := 0
	return benchmark(cfg, func(className string) QueryWithNeighbors {
		defer func() { i++ }()

		return QueryWithNeighbors{
			Query:     nearVectorQueryGrpc(cfg.ClassName, queries[i], cfg.Limit),
			Neighbors: neighbors[i],
		}

	})
}
