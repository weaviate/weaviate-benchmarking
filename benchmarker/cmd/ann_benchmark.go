package cmd

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/weaviate/hdf5"
	"github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/fault"
	"github.com/weaviate/weaviate/entities/models"
	weaviategrpc "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
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
	Mean             float64 `json:"meanLatency"`
	P99Latency       float64 `json:"p99Latency"`
	QueriesPerSecond float64 `json:"qps"`
	Shards           int     `json:"shards"`
	Parallelization  int     `json:"parallelization"`
	Limit            int     `json:"limit"`
	ImportTime       float64 `json:"importTime"`
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

// Convert a uuid formatted string to an int
func intFromUUID(uuidStr string) int {
	id, err := uuid.Parse(uuidStr)
	if err != nil {
		panic(err)
	}
	val := binary.BigEndian.Uint64(id[8:])
	return int(val)
}

// Writes a single batch of vectors to Weaviate using gRPC
func writeChunk(chunk *Batch, client *weaviategrpc.WeaviateClient, cfg *Config) {

	objects := make([]*weaviategrpc.BatchObject, len(chunk.Vectors))

	for i, vector := range chunk.Vectors {
		objects[i] = &weaviategrpc.BatchObject{
			Uuid:       uuidFromInt(i + chunk.Offset),
			Vector:     vector,
			Collection: cfg.ClassName,
		}
		if cfg.Tenant != "" {
			objects[i].Tenant = cfg.Tenant
		}
	}

	batchRequest := &weaviategrpc.BatchObjectsRequest{
		Objects: objects,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	response, err := (*client).BatchObjects(ctx, batchRequest)
	if err != nil {
		log.Fatalf("could not send batch: %v", err)
	}

	for _, result := range response.GetErrors() {
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
		Host:   cfg.HttpOrigin,
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

	multiTenancyEnabled := false
	if cfg.Tenant != "" {
		multiTenancyEnabled = true
	}

	classObj := &models.Class{
		Class:       cfg.ClassName,
		Description: fmt.Sprintf("Created by the Weaviate Benchmarker at %s", time.Now().String()),
		VectorIndexConfig: map[string]interface{}{
			"distance":       cfg.DistanceMetric,
			"efConstruction": float64(cfg.EfConstruction),
			"maxConnections": float64(cfg.MaxConnections),
		},
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: multiTenancyEnabled,
		},
	}

	err = client.Schema().ClassCreator().WithClass(classObj).Do(context.Background())
	if err != nil {
		panic(err)
	}
	log.Printf("Created class %s", cfg.ClassName)
}

func addTenantIfNeeded(cfg *Config) {
	if cfg.Tenant == "" {
		return
	}
	wcfg := weaviate.Config{
		Host:   cfg.HttpOrigin,
		Scheme: "http",
	}
	client, err := weaviate.NewClient(wcfg)
	if err != nil {
		panic(err)
	}
	client.Schema().TenantsCreator().
		WithClassName(cfg.ClassName).
		WithTenants(models.Tenant{Name: cfg.Tenant}).
		Do(context.Background())
}

// Update ef parameter on the Weaviate schema
func updateEf(ef int, cfg *Config) {
	wcfg := weaviate.Config{
		Host:   cfg.HttpOrigin,
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

func waitReady(cfg *Config, indexStart time.Time, maxDuration time.Duration, minQueueSize int64) time.Time {
	wcfg := weaviate.Config{
		Host:   cfg.HttpOrigin,
		Scheme: "http",
	}
	client, err := weaviate.NewClient(wcfg)
	if err != nil {
		panic(err)
	}

	start := time.Now()
	current := time.Now()

	log.Infof("Waiting for queue to be empty\n")
	for current.Sub(start) < maxDuration {
		nodesStatus, err := client.Cluster().NodesStatusGetter().Do(context.Background())
		if err != nil {
			panic(err)
		}
		totalShardQueue := int64(0)
		for _, n := range nodesStatus.Nodes {
			for _, s := range n.Shards {
				if s.Class == cfg.ClassName && s.VectorQueueLength > 0 {
					totalShardQueue += s.VectorQueueLength
				}
			}
		}
		if totalShardQueue < minQueueSize {
			log.WithFields(log.Fields{"duration": current.Sub(start)}).Printf("Queue ready\n")
			log.WithFields(log.Fields{"duration": current.Sub(indexStart)}).Printf("Total load and queue ready\n")
			return current
		}
		time.Sleep(2 * time.Second)
		current = time.Now()
	}
	log.Fatalf("Queue wasn't ready in %s\n", maxDuration)
	return current
}

// Update ef parameter on the Weaviate schema
func enablePQ(cfg *Config, dimensions uint) {
	wcfg := weaviate.Config{
		Host:   cfg.HttpOrigin,
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

	if dimensions%cfg.PQRatio != 0 {
		log.Fatalf("PQ ratio of %d and dimensions of %d incompatible", cfg.PQRatio, dimensions)
	}

	segments := dimensions / cfg.PQRatio

	vectorIndexConfig := classConfig.VectorIndexConfig.(map[string]interface{})
	vectorIndexConfig["pq"] = map[string]interface{}{
		"enabled":       true,
		"segments":      segments,
		"trainingLimit": cfg.TrainingLimit,
	}

	classConfig.VectorIndexConfig = vectorIndexConfig

	err = client.Schema().ClassUpdater().WithClass(classConfig).Do(context.Background())

	if err != nil {
		panic(err)
	}
	log.WithFields(log.Fields{"segments": segments, "dimensions": dimensions}).Printf("Enabled PQ. Waiting for shard ready.\n")

	start := time.Now()

	for {
		time.Sleep(3 * time.Second)
		diff := time.Since(start)
		if diff.Minutes() > 50 {
			log.Fatalf("Shard still not ready after 50 minutes, exiting..\n")
		}
		shards, err := client.Schema().ShardsGetter().WithClassName(cfg.ClassName).Do(context.Background())
		if err != nil || len(shards) == 0 {
			if weaviateErr, ok := err.(*fault.WeaviateClientError); ok {
				log.Warnf("Error getting schema: %v", weaviateErr.DerivedFromError)
			} else {
				log.Warnf("Error getting schema: %v", err)
			}
			continue
		}
		ready := true
		for _, shard := range shards {
			if shard.Status != "READY" {
				ready = false
			}
		}
		if ready {
			break
		}
	}

	endTime := time.Now()
	log.WithFields(log.Fields{"segments": segments, "dimensions": dimensions}).Printf("PQ Completed in %v\n", endTime.Sub(start))

}

func convert1DChunk[D float32 | float64](input []D, dimensions int, batchRows int) [][]float32 {
	chunkData := make([][]float32, batchRows)
	for i := range chunkData {
		chunkData[i] = make([]float32, dimensions)
		for j := 0; j < dimensions; j++ {
			chunkData[i][j] = float32(input[i*dimensions+j])
		}
	}
	return chunkData
}

func getHDF5ByteSize(dataset *hdf5.Dataset) uint {

	datatype, err := dataset.Datatype()
	if err != nil {
		log.Fatalf("Unabled to read datatype\n")
	}

	// log.WithFields(log.Fields{"size": datatype.Size()}).Printf("Parsing HDF5 byte format\n")
	byteSize := datatype.Size()
	if byteSize != 4 && byteSize != 8 {
		log.Fatalf("Unable to load dataset with byte size %d\n", byteSize)
	}
	return byteSize
}

// Load a large dataset from an hdf5 file and stream it to Weaviate
// startOffset and maxRecords are ignored if equal to 0
func loadHdf5Streaming(dataset *hdf5.Dataset, chunks chan<- Batch, cfg *Config, startOffset uint, maxRecords uint) {
	dataspace := dataset.Space()
	dims, _, _ := dataspace.SimpleExtentDims()

	if len(dims) != 2 {
		log.Fatal("expected 2 dimensions")
	}

	byteSize := getHDF5ByteSize(dataset)

	rows := dims[0]
	dimensions := dims[1]

	// Handle offsetting the data for product quantization
	i := uint(0)
	if maxRecords != 0 && maxRecords < rows {
		rows = maxRecords
	}

	if startOffset != 0 && i < rows {
		i = startOffset
	}

	batchSize := uint(cfg.BatchSize)

	log.WithFields(log.Fields{"rows": rows, "dimensions": dimensions}).Printf(
		"Reading HDF5 dataset")

	memspace, err := hdf5.CreateSimpleDataspace([]uint{batchSize, dimensions}, []uint{batchSize, dimensions})
	if err != nil {
		log.Fatalf("Error creating memspace: %v", err)
	}
	defer memspace.Close()

	for ; i < rows; i += batchSize {

		batchRows := batchSize
		// handle final smaller batch
		if i+batchSize > rows {
			batchRows = rows - i
			memspace, err = hdf5.CreateSimpleDataspace([]uint{batchRows, dimensions}, []uint{batchRows, dimensions})
			if err != nil {
				log.Fatalf("Error creating final memspace: %v", err)
			}
		}

		offset := []uint{i, 0}
		count := []uint{batchRows, dimensions}

		if err := dataspace.SelectHyperslab(offset, nil, count, nil); err != nil {
			log.Fatalf("Error selecting hyperslab: %v", err)
		}

		var chunkData [][]float32

		if byteSize == 4 {
			chunkData1D := make([]float32, batchRows*dimensions)

			if err := dataset.ReadSubset(&chunkData1D, memspace, dataspace); err != nil {
				log.Printf("BatchRows = %d, i = %d, rows = %d", batchRows, i, rows)
				log.Fatalf("Error reading subset: %v", err)
			}

			chunkData = convert1DChunk[float32](chunkData1D, int(dimensions), int(batchRows))

		} else if byteSize == 8 {
			chunkData1D := make([]float64, batchRows*dimensions)

			if err := dataset.ReadSubset(&chunkData1D, memspace, dataspace); err != nil {
				log.Printf("BatchRows = %d, i = %d, rows = %d", batchRows, i, rows)
				log.Fatalf("Error reading subset: %v", err)
			}

			chunkData = convert1DChunk[float64](chunkData1D, int(dimensions), int(batchRows))

		}

		if (i+batchRows)%10000 == 0 {
			log.Printf("Imported %d/%d rows", i+batchRows, rows)
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

	byteSize := getHDF5ByteSize(dataset)

	if len(dims) != 2 {
		log.Fatal("expected 2 dimensions")
	}

	rows := dims[0]
	dimensions := dims[1]

	var chunkData [][]float32

	if byteSize == 4 {
		chunkData1D := make([]float32, rows*dimensions)
		dataset.Read(&chunkData1D)
		chunkData = convert1DChunk[float32](chunkData1D, int(dimensions), int(rows))
	} else if byteSize == 8 {
		chunkData1D := make([]float64, rows*dimensions)
		dataset.Read(&chunkData1D)
		chunkData = convert1DChunk[float64](chunkData1D, int(dimensions), int(rows))
	}

	return chunkData
}

// Read an entire dataset from an hdf5 file at once (neighbours)
func loadHdf5Neighbors(file *hdf5.File, name string) [][]int {
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

	byteSize := getHDF5ByteSize(dataset)

	chunkData := make([][]int, rows)

	if byteSize == 4 {
		chunkData1D := make([]int32, rows*dimensions)
		dataset.Read(&chunkData1D)
		for i := range chunkData {
			chunkData[i] = make([]int, dimensions)
			for j := uint(0); j < dimensions; j++ {
				chunkData[i][j] = int(chunkData1D[uint(i)*dimensions+j])
			}
		}
	} else if byteSize == 8 {
		chunkData1D := make([]int, rows*dimensions)
		dataset.Read(&chunkData1D)
		for i := range chunkData {
			chunkData[i] = chunkData1D[i*int(dimensions) : (i+1)*int(dimensions)]
		}
	}

	return chunkData
}

func loadHdf5Train(file *hdf5.File, cfg *Config, offset uint, maxRows uint) uint {
	dataset, err := file.OpenDataset("train")
	if err != nil {
		log.Fatalf("Error opening dataset: %v", err)
	}
	defer dataset.Close()
	dataspace := dataset.Space()
	extent, _, _ := dataspace.SimpleExtentDims()
	dimensions := extent[1]

	chunks := make(chan Batch, 10)

	go func() {
		loadHdf5Streaming(dataset, chunks, cfg, offset, maxRows)
		close(chunks)
	}()

	var wg sync.WaitGroup

	for i := 0; i < 8; i++ {
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
	return dimensions
}

// Load an hdf5 file in the format of ann-benchmarks.com
// returns total time duration for load
func loadANNBenchmarksFile(file *hdf5.File, cfg *Config) time.Duration {

	if !cfg.ExistingSchema {
		createSchema(cfg)
	}
	addTenantIfNeeded(cfg)

	startTime := time.Now()

	if cfg.EnablePQ {
		dimensions := loadHdf5Train(file, cfg, 0, uint(cfg.TrainingLimit))
		log.Printf("Pausing to enable PQ.")
		enablePQ(cfg, dimensions)
		loadHdf5Train(file, cfg, uint(cfg.TrainingLimit), 0)

	} else {
		loadHdf5Train(file, cfg, 0, 0)
	}
	endTime := time.Now()
	log.WithFields(log.Fields{"duration": endTime.Sub(startTime)}).Printf("Total load time\n")

	importTime := waitReady(cfg, startTime, 4*time.Hour, 1000)
	sleepDuration := 30 * time.Second
	log.Printf("Waiting for %s to allow for compaction etc\n", sleepDuration)
	time.Sleep(sleepDuration)
	return importTime.Sub(startTime)
}

func parseEfValues(s string) ([]int, error) {
	strs := strings.Split(s, ",")
	nums := make([]int, len(strs))
	for i, str := range strs {
		num, err := strconv.Atoi(str)
		if err != nil {
			return nil, fmt.Errorf("error converting efArray '%s' to integer: %v", str, err)
		}
		nums[i] = num
	}
	return nums, nil
}

var annBenchmarkCommand = &cobra.Command{
	Use:   "ann-benchmark",
	Short: "Benchmark ANN Benchmark style hdf5 files",
	Long:  `Run a gRPC benchmark on an hdf5 file in the format of ann-benchmarks.com`,
	Run: func(cmd *cobra.Command, args []string) {

		cfg := globalConfig
		cfg.Mode = "ann-benchmark"

		if err := cfg.Validate(); err != nil {
			fatal(err)
		}

		cfg.parseLabels()

		efCandidates, err := parseEfValues(cfg.EfArray)
		if err != nil {
			log.Fatalf("Error parsing efArray, expected commas separated format \"16,32,64\" but:%v\n", err)
		}

		runID := strconv.FormatInt(time.Now().Unix(), 10)

		file, err := hdf5.OpenFile(cfg.BenchmarkFile, hdf5.F_ACC_RDONLY)
		if err != nil {
			log.Fatalf("Error opening file: %v\n", err)
		}
		defer file.Close()

		importTime := 0 * time.Second
		if !cfg.QueryOnly {
			log.WithFields(log.Fields{"efC": cfg.EfConstruction, "m": cfg.MaxConnections, "shards": cfg.Shards,
				"distance": cfg.DistanceMetric, "dataset": cfg.BenchmarkFile}).Info("Starting import")
			importTime = loadANNBenchmarksFile(file, &cfg)
		}

		log.WithFields(log.Fields{"efC": cfg.EfConstruction, "m": cfg.MaxConnections, "shards": cfg.Shards,
			"distance": cfg.DistanceMetric, "dataset": cfg.BenchmarkFile}).Info("Starting query")

		neighbors := loadHdf5Neighbors(file, "neighbors")
		testData := loadHdf5Float32(file, "test")

		var benchmarkResultsMap []map[string]interface{}

		for _, ef := range efCandidates {
			updateEf(ef, &cfg)

			var result Results

			if cfg.QueryDuration > 0 {
				result = benchmarkANNDuration(cfg, testData, neighbors)
			} else {
				result = benchmarkANN(cfg, testData, neighbors)
			}

			log.WithFields(log.Fields{"mean": result.Mean, "qps": result.QueriesPerSecond, "recall": result.Recall,
				"parallel": cfg.Parallel, "limit": cfg.Limit,
				"api": cfg.API, "ef": ef, "count": result.Total, "failed": result.Failed}).Info("Benchmark result")

			dataset := filepath.Base(cfg.BenchmarkFile)

			var resultMap map[string]interface{}

			benchResult := ResultsJSONBenchmark{
				Api:              cfg.API,
				Ef:               ef,
				EfConstruction:   cfg.EfConstruction,
				MaxConnections:   cfg.MaxConnections,
				Mean:             result.Mean.Seconds(),
				P99Latency:       result.Percentiles[len(result.Percentiles)-1].Seconds(),
				QueriesPerSecond: result.QueriesPerSecond,
				Shards:           cfg.Shards,
				Parallelization:  cfg.Parallel,
				Limit:            cfg.Limit,
				ImportTime:       importTime.Seconds(),
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
				for key, value := range cfg.LabelMap {
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

	numCPU := runtime.NumCPU()

	annBenchmarkCommand.PersistentFlags().StringVar(&globalConfig.Labels,
		"labels", "", "Labels of format key1=value1,key2=value2,...")
	annBenchmarkCommand.PersistentFlags().StringVarP(&globalConfig.BenchmarkFile,
		"vectors", "v", "", "Path to the hdf5 file containing the vectors")
	annBenchmarkCommand.PersistentFlags().StringVarP(&globalConfig.ClassName,
		"className", "c", "Vector", "Class name for testing")
	annBenchmarkCommand.PersistentFlags().StringVarP(&globalConfig.DistanceMetric,
		"distance", "d", "", "Set distance metric (mandatory)")
	annBenchmarkCommand.PersistentFlags().BoolVarP(&globalConfig.QueryOnly,
		"query", "q", false, "Do not import data and only run query tests")
	annBenchmarkCommand.PersistentFlags().IntVar(&globalConfig.QueryDuration,
		"queryDuration", 0, "Instead of querying the test dataset once, query for the specified duration in seconds (default 0)")
	annBenchmarkCommand.PersistentFlags().BoolVar(&globalConfig.EnablePQ,
		"pq", false, "Enable product quantization (default false)")
	annBenchmarkCommand.PersistentFlags().UintVar(&globalConfig.PQRatio,
		"pqRatio", 4, "Set PQ segments = dimensions / ratio (must divide evenly default 4)")
	annBenchmarkCommand.PersistentFlags().IntVar(&globalConfig.TrainingLimit,
		"trainingLimit", 100000, "Set PQ trainingLimit (default 100000)")
	annBenchmarkCommand.PersistentFlags().IntVar(&globalConfig.EfConstruction,
		"efConstruction", 256, "Set Weaviate efConstruction parameter (default 256)")
	annBenchmarkCommand.PersistentFlags().StringVar(&globalConfig.EfArray,
		"efArray", "16,24,32,48,64,96,128,256,512", "Array of ef parameters as comma separated list")
	annBenchmarkCommand.PersistentFlags().IntVar(&globalConfig.MaxConnections,
		"maxConnections", 16, "Set Weaviate efConstruction parameter (default 16)")
	annBenchmarkCommand.PersistentFlags().IntVar(&globalConfig.Shards,
		"shards", 1, "Set number of Weaviate shards")
	annBenchmarkCommand.PersistentFlags().IntVarP(&globalConfig.BatchSize,
		"batchSize", "b", 1000, "Batch size for insert operations")
	annBenchmarkCommand.PersistentFlags().IntVarP(&globalConfig.Parallel,
		"parallel", "p", numCPU, "Set the number of parallel threads which send queries")
	annBenchmarkCommand.PersistentFlags().BoolVar(&globalConfig.ExistingSchema,
		"existingSchema", false, "Leave the schema as-is (default false)")
	annBenchmarkCommand.PersistentFlags().StringVar(&globalConfig.Tenant,
		"tenant", "", "Tenant name to use")
	annBenchmarkCommand.PersistentFlags().StringVarP(&globalConfig.API,
		"api", "a", "grpc", "The API to use on benchmarks")
	annBenchmarkCommand.PersistentFlags().StringVarP(&globalConfig.Origin,
		"origin", "u", "localhost:50051", "The gRPC origin that Weaviate is running at")
	annBenchmarkCommand.PersistentFlags().StringVar(&globalConfig.HttpOrigin,
		"httpOrigin", "localhost:8080", "The http origin for Weaviate (only used if grpc enabled)")
	annBenchmarkCommand.PersistentFlags().StringVarP(&globalConfig.OutputFormat,
		"format", "f", "text", "Output format, one of [text, json]")
	annBenchmarkCommand.PersistentFlags().IntVarP(&globalConfig.Limit,
		"limit", "l", 10, "Set the query limit / k (default 10)")
	annBenchmarkCommand.PersistentFlags().StringVarP(&globalConfig.OutputFile,
		"output", "o", "", "Filename for an output file. If none provided, output to stdout only")
}

func benchmarkANN(cfg Config, queries Queries, neighbors Neighbors) Results {
	cfg.Queries = len(queries)

	i := 0
	return benchmark(cfg, func(className string) QueryWithNeighbors {
		defer func() { i++ }()

		return QueryWithNeighbors{
			Query:     nearVectorQueryGrpc(cfg.ClassName, queries[i], cfg.Limit, cfg.Tenant),
			Neighbors: neighbors[i],
		}

	})
}

func benchmarkANNDuration(cfg Config, queries Queries, neighbors Neighbors) Results {
	cfg.Queries = len(queries)

	startTime := time.Now()

	var results Results

	for time.Since(startTime) < time.Duration(cfg.QueryDuration)*time.Second {
		results = benchmarkANN(cfg, queries, neighbors)
	}

	return results

}
