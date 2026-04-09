package cmd

import (
	"encoding/binary"
	"math"
	"runtime"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// ResultsJSONBenchmark is the ann-benchmarks.com compatible output format.
// Mixed camel/snake case for compatibility with downstream tooling.
type ResultsJSONBenchmark struct {
	Api              string  `json:"api"`
	Ef               int     `json:"ef,omitempty"`
	RescoreLimit     int     `json:"rescoreLimit,omitempty"`
	SearchProbe      int     `json:"searchProbe,omitempty"`
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
	IterationRunID   string  `json:"iteration"`
	Dataset          string  `json:"dataset_file"`
	Recall           float64 `json:"recall"`
	NDCG             float64 `json:"ndcg"`
	HeapAllocBytes   float64 `json:"heap_alloc_bytes"`
	HeapInuseBytes   float64 `json:"heap_inuse_bytes"`
	HeapSysBytes     float64 `json:"heap_sys_bytes"`
	Timestamp        string  `json:"timestamp"`
}

// uuidFromInt converts an integer to a UUID-formatted string.
func uuidFromInt(val int) string {
	bytes := make([]byte, 16)
	binary.BigEndian.PutUint64(bytes[8:], uint64(val))
	id, err := uuid.FromBytes(bytes)
	if err != nil {
		panic(err)
	}

	return id.String()
}

// intFromUUID converts a UUID-formatted string back to an integer.
func intFromUUID(uuidStr string) int {
	id, err := uuid.Parse(uuidStr)
	if err != nil {
		panic(err)
	}
	val := binary.BigEndian.Uint64(id[8:])
	return int(val)
}

var annBenchmarkCommand = &cobra.Command{
	Use:   "ann-benchmark",
	Short: "Benchmark ANN Benchmark style datasets",
	Long:  `Run a gRPC benchmark on an hdf5 file in the format of ann-benchmarks.com`,
	Run: func(cmd *cobra.Command, args []string) {
		cfg := globalConfig
		cfg.Mode = "ann-benchmark"

		if err := cfg.Validate(); err != nil {
			fatal(err)
		}

		cfg.parseLabels()

		memoryMonitor := NewMemoryMonitor(&cfg)
		memoryMonitor.Start()
		defer memoryMonitor.Stop()

		var dataset Dataset
		if len(cfg.DatasetRepo) > 0 {
			dataset = NewParquetDataset(cfg.DatasetRepo, cfg.Dataset, cfg.MultiVectorDimensions, cfg.Filter)
		} else {
			dataset = NewHdf5Dataset(cfg.BenchmarkFile, cfg.MultiVectorDimensions, cfg.Filter)
		}
		defer dataset.Close()

		client := createClient(&cfg)

		importTime := 0 * time.Second

		if !cfg.QueryOnly {

			if !cfg.ExistingSchema {
				createSchema(&cfg, client)
			}

			log.WithFields(log.Fields{
				"index": cfg.IndexType, "efC": cfg.EfConstruction, "m": cfg.MaxConnections, "shards": cfg.Shards,
				"distance": cfg.DistanceMetric, "dataset": cfg.BenchmarkFile,
			}).Info("Starting import")

			if cfg.NumTenants > 0 {
				importTime = loadANNBenchmarksDataMultiTenant(dataset, &cfg, client)
			} else {
				importTime = loadANNBenchmarksData(dataset, &cfg, client, 0)
			}

			sleepDuration := time.Duration(cfg.QueryDelaySeconds) * time.Second
			log.Printf("Waiting for %s to allow for compaction etc\n", sleepDuration)
			time.Sleep(sleepDuration)
		}

		log.WithFields(log.Fields{
			"index": cfg.IndexType, "efC": cfg.EfConstruction, "m": cfg.MaxConnections, "shards": cfg.Shards,
			"distance": cfg.DistanceMetric, "dataset": cfg.BenchmarkFile,
		}).Info("Benchmark configuration")

		if cfg.SkipQuery {
			return
		}

		neighbors := dataset.Neighbors()
		testData := dataset.TestVectors()
		testFilters := dataset.TestFilters()

		runQueries(&cfg, importTime, testData, neighbors, testFilters)

		if cfg.performUpdates() {

			totalRowCount := dataset.NumTrainVectors()
			updateRowCount := uint(math.Floor(float64(totalRowCount) * cfg.UpdatePercentage))

			log.Printf("Performing %d update iterations\n", cfg.UpdateIterations)

			for i := 0; i < cfg.UpdateIterations; i++ {

				startTime := time.Now()

				if cfg.UpdateRandomized {
					loadTrainData(dataset, &cfg, 0, 0, float32(cfg.UpdatePercentage))
				} else {
					deleteUuidRange(&cfg, client, 0, int(updateRowCount))
					loadTrainData(dataset, &cfg, 0, updateRowCount, 0)
				}

				log.WithFields(log.Fields{"duration": time.Since(startTime)}).Printf("Total delete and update time\n")

				if !cfg.SkipTombstonesEmpty {
					err := waitTombstonesEmpty(&cfg)
					if err != nil {
						log.Fatalf("Error waiting for tombstones to be empty: %v", err)
					}
				}
				if !cfg.SkipAsyncReady {
					startTime := time.Now()
					waitReady(&cfg, client, startTime, 30*time.Minute, 1000)
				}

				runQueries(&cfg, importTime, testData, neighbors, testFilters)

			}

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
	annBenchmarkCommand.PersistentFlags().StringVar(&globalConfig.NamedVector,
		"namedVector", "", "Named vector")
	annBenchmarkCommand.PersistentFlags().StringVarP(&globalConfig.DistanceMetric,
		"distance", "d", "", "Set distance metric (mandatory)")
	annBenchmarkCommand.PersistentFlags().BoolVarP(&globalConfig.QueryOnly,
		"query", "q", false, "Do not import data and only run query tests")
	annBenchmarkCommand.PersistentFlags().IntVar(&globalConfig.QueryDuration,
		"queryDuration", 0, "Instead of querying the test dataset once, query for the specified duration in seconds (default 0)")
	annBenchmarkCommand.PersistentFlags().BoolVar(&globalConfig.BQ,
		"bq", false, "Set BQ")
	annBenchmarkCommand.PersistentFlags().BoolVar(&globalConfig.Cache,
		"cache", false, "Set cache")
	annBenchmarkCommand.PersistentFlags().BoolVar(&globalConfig.WaitForBackground,
		"waitForBackground", false, "Repeat query runs until background condition is satisfied")
	annBenchmarkCommand.PersistentFlags().IntVar(&globalConfig.RescoreLimit,
		"rescoreLimit", -1, "Rescore limit. If not set, it will be set by Weaviate automatically when rescoring is enabled")
	annBenchmarkCommand.PersistentFlags().StringVar(&globalConfig.PQ,
		"pq", "disabled", "Set PQ (disabled, auto, or enabled) (default disabled)")
	annBenchmarkCommand.PersistentFlags().StringVar(&globalConfig.SQ,
		"sq", "disabled", "Set SQ (disabled, auto, or enabled) (default disabled)")
	annBenchmarkCommand.PersistentFlags().UintVar(&globalConfig.PQRatio,
		"pqRatio", 4, "Set PQ segments = dimensions / ratio (must divide evenly default 4)")
	annBenchmarkCommand.PersistentFlags().UintVar(&globalConfig.PQSegments,
		"pqSegments", 256, "Set PQ segments")
	annBenchmarkCommand.PersistentFlags().StringVar(&globalConfig.RQ,
		"rq", "disabled", "Set RQ (disabled, auto, or enabled) (default disabled)")
	annBenchmarkCommand.PersistentFlags().UintVar(&globalConfig.RQBits,
		"rqBits", 8, "Set RQ bits (default 8)")
	annBenchmarkCommand.PersistentFlags().IntVarP(&globalConfig.MultiVectorDimensions,
		"multiVector", "m", 0, "Enable multi-dimensional vectors with the specified number of dimensions")
	annBenchmarkCommand.PersistentFlags().BoolVar(&globalConfig.MuveraEnabled,
		"muveraEnabled", false, "Enable muvera")
	annBenchmarkCommand.PersistentFlags().IntVar(&globalConfig.MuveraKSim,
		"muveraKSim", 4, "Set muvera ksim parameter")
	annBenchmarkCommand.PersistentFlags().IntVar(&globalConfig.MuveraDProjections,
		"muveraDProjections", 16, "Set muvera dprojections parameter")
	annBenchmarkCommand.PersistentFlags().IntVar(&globalConfig.MuveraRepetition,
		"muveraRepetition", 20, "Set muvera repetition parameter")
	annBenchmarkCommand.PersistentFlags().BoolVar(&globalConfig.SkipQuery,
		"skipQuery", false, "Only import data and skip query tests")
	annBenchmarkCommand.PersistentFlags().BoolVar(&globalConfig.SkipAsyncReady,
		"skipAsyncReady", false, "Skip async ready (default false)")
	annBenchmarkCommand.PersistentFlags().BoolVar(&globalConfig.SkipMemoryStats,
		"skipMemoryStats", false, "Skip memory stats (default false)")
	annBenchmarkCommand.PersistentFlags().BoolVar(&globalConfig.SkipTombstonesEmpty,
		"skipTombstonesEmpty", false, "Skip waiting for tombstone to be empty after update (default false)")
	annBenchmarkCommand.PersistentFlags().IntVar(&globalConfig.TrainingLimit,
		"trainingLimit", 100000, "Set PQ trainingLimit (default 100000)")
	annBenchmarkCommand.PersistentFlags().IntVar(&globalConfig.EfConstruction,
		"efConstruction", 256, "Set Weaviate efConstruction parameter (default 256)")
	annBenchmarkCommand.PersistentFlags().StringVar(&globalConfig.EfArray,
		"efArray", "16,24,32,48,64,96,128,256,512", "Array of ef parameters as comma separated list")
	annBenchmarkCommand.PersistentFlags().StringVar(&globalConfig.IndexType,
		"indexType", "hnsw", "Index type (hnsw, flat or hfresh)")
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
	annBenchmarkCommand.PersistentFlags().IntVar(&globalConfig.NumTenants,
		"numTenants", 0, "Number of tenants to use (default 0)")
	annBenchmarkCommand.PersistentFlags().IntVar(&globalConfig.StartTenantNum,
		"startTenant", 0, "Tenant # to start at if using multiple tenants (default 0)")
	annBenchmarkCommand.PersistentFlags().StringVarP(&globalConfig.API,
		"api", "a", "grpc", "The API to use on benchmarks")
	annBenchmarkCommand.PersistentFlags().StringVarP(&globalConfig.Origin,
		"grpcOrigin", "u", "localhost:50051", "The gRPC origin that Weaviate is running at")
	annBenchmarkCommand.PersistentFlags().StringVar(&globalConfig.HttpOrigin,
		"httpOrigin", "localhost:8080", "The http origin for Weaviate (only used if grpc enabled)")
	annBenchmarkCommand.PersistentFlags().StringVar(&globalConfig.HttpScheme,
		"httpScheme", "http", "The http scheme (http or https)")
	annBenchmarkCommand.PersistentFlags().StringVar(&globalConfig.MetricsEndpoint,
		"metricsEndpoint", "http://localhost:2112/metrics", "The metrics endpoint for Weaviate(default http://localhost:2112/metrics)")
	annBenchmarkCommand.PersistentFlags().StringVarP(&globalConfig.OutputFormat,
		"format", "f", "text", "Output format, one of [text, json]")
	annBenchmarkCommand.PersistentFlags().IntVarP(&globalConfig.Limit,
		"limit", "l", 10, "Set the query limit / k (default 10)")
	annBenchmarkCommand.PersistentFlags().Float64Var(&globalConfig.UpdatePercentage,
		"updatePercentage", 0.0, "After loading the dataset, update the specified percentage of vectors")
	annBenchmarkCommand.PersistentFlags().BoolVar(&globalConfig.UpdateRandomized,
		"updateRandomized", false, "Whether to randomize which vectors are updated (default false)")
	annBenchmarkCommand.PersistentFlags().IntVar(&globalConfig.UpdateIterations,
		"updateIterations", 1, "Number of iterations to update the dataset if updatePercentage is set")
	annBenchmarkCommand.PersistentFlags().IntVar(&globalConfig.CleanupIntervalSeconds,
		"cleanupIntervalSeconds", 300, "HNSW cleanup interval seconds (default 300)")
	annBenchmarkCommand.PersistentFlags().IntVar(&globalConfig.QueryDelaySeconds,
		"queryDelaySeconds", 30, "How long to wait before querying (default 30)")
	annBenchmarkCommand.PersistentFlags().IntVar(&globalConfig.Offset,
		"offset", 0, "Offset for uuids (useful to load the same dataset multiple times)")
	annBenchmarkCommand.PersistentFlags().StringVarP(&globalConfig.OutputFile,
		"output", "o", "", "Filename for an output file. If none provided, output to stdout only")
	annBenchmarkCommand.PersistentFlags().IntVar(&globalConfig.DynamicThreshold,
		"dynamicThreshold", 10_000, "Threshold to trigger the update in the dynamic index (default 10 000)")
	annBenchmarkCommand.PersistentFlags().BoolVar(&globalConfig.Filter,
		"filter", false, "Whether to use filtering for the dataset (default false)")
	annBenchmarkCommand.PersistentFlags().IntVar(&globalConfig.FlatSearchCutoff,
		"flatSearchCutoff", 40000, "Flat search cut off (default 40 000)")
	annBenchmarkCommand.PersistentFlags().StringVar(&globalConfig.FilterStrategy,
		"filterStrategy", "sweeping", "Use a different filter strategy (options are sweeping or acorn)")
	annBenchmarkCommand.PersistentFlags().IntVar(&globalConfig.ReplicationFactor,
		"replicationFactor", 1, "Replication factor (default 1)")
	annBenchmarkCommand.PersistentFlags().BoolVar(&globalConfig.AsyncReplicationEnabled,
		"asyncReplicationEnabled", false, "Enable asynchronous replication (default false)")
	annBenchmarkCommand.PersistentFlags().BoolVar(&globalConfig.MemoryMonitoringEnabled,
		"memoryMonitoringEnabled", false, "Enable continuous memory monitoring (default false)")
	annBenchmarkCommand.PersistentFlags().IntVar(&globalConfig.MemoryMonitoringInterval,
		"memoryMonitoringInterval", 5, "Memory monitoring interval in seconds (default 5)")
	annBenchmarkCommand.PersistentFlags().StringVar(&globalConfig.MemoryMonitoringFile,
		"memoryMonitoringFile", "", "Memory monitoring output file name (default: memory_metrics_<timestamp>.json)")
	annBenchmarkCommand.PersistentFlags().StringVar(&globalConfig.DatasetRepo,
		"datasetRepo", "", "Hugging Face dataset repo e.g. weaviate/ann-datasets")
	annBenchmarkCommand.PersistentFlags().StringVar(&globalConfig.Dataset,
		"dataset", "", "Dataset name e.g. dbpedia-openai-ada002-1536-float32-angular-100k")
	annBenchmarkCommand.PersistentFlags().IntVar(&globalConfig.MaxPostingSizeKB,
		"maxPostingSizeKB", 48, "Max posting size for HFresh index (default 48)")
	annBenchmarkCommand.PersistentFlags().IntVar(&globalConfig.Replicas,
		"replicas", 4, "Number of replicas for HFresh index (default 4)")
	annBenchmarkCommand.PersistentFlags().Float64Var(&globalConfig.RngFactor,
		"rngFactor", 10.0, "RNG factor for HFresh index (default 10.0)")
}
