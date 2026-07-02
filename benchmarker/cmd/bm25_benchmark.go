package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/filters"
	"github.com/weaviate/weaviate/entities/models"
)

func boolPtr(b bool) *bool { return &b }

var bm25BenchmarkCommand = &cobra.Command{
	Use:   "bm25-benchmark",
	Short: "Benchmark BM25 keyword search on a BEIR-format text corpus",
	Long: `Import a BEIR-format text corpus (corpus.jsonl) into a vectorless Weaviate
collection and benchmark BM25 keyword-search latency/QPS using queries.jsonl.

Optionally generates inverted-index tombstones (via deletes/updates) to measure
BM25 keyword-search performance under tombstone load.`,
	Run: func(cmd *cobra.Command, args []string) {
		cfg := globalConfig
		cfg.Mode = "bm25-benchmark"

		// All commands bind --className to the same globalConfig field, and pflag
		// applies each command's default at registration time, so a later-registered
		// command (colbert) clobbers this command's "Bm25Bench" default. Restore it
		// when the user did not explicitly pass --className.
		if !cmd.Flags().Changed("className") {
			cfg.ClassName = "Bm25Bench"
		}

		if err := cfg.Validate(); err != nil {
			fatal(err)
		}
		cfg.parseLabels()

		memoryMonitor := NewMemoryMonitor(&cfg)
		memoryMonitor.Start()
		defer memoryMonitor.Stop()

		ds := NewBeirDataset(cfg.CorpusFile, cfg.QueriesFile, cfg.Filter, cfg.FilterCount)
		defer ds.Close()

		client := createClient(&cfg)

		var importTime time.Duration
		if !cfg.QueryOnly {
			if !cfg.ExistingSchema {
				createBM25Schema(&cfg, client)
			}
			log.WithFields(log.Fields{
				"class": cfg.ClassName, "corpus": cfg.CorpusFile, "filter": cfg.Filter,
			}).Info("Starting BM25 import")
			importTime = loadBM25Data(ds, &cfg, client)

			sleepDuration := time.Duration(cfg.QueryDelaySeconds) * time.Second
			log.Printf("Waiting for %s to allow flush/compaction to settle", sleepDuration)
			time.Sleep(sleepDuration)
		}

		if cfg.SkipQuery {
			return
		}

		queries := ds.LoadQueries()
		if len(queries) == 0 {
			fatal(fmt.Errorf("no queries loaded from %s", cfg.QueriesFile))
		}
		log.WithFields(log.Fields{"queries": len(queries)}).Info("Loaded BM25 queries")

		runBM25Queries(&cfg, client, ds, queries, importTime)
	},
}

// createBM25Schema (re)creates the benchmark collection: a vectorless class with a
// searchable "text"/"title" property (BM25 inverted index), a numeric "docId"
// (enables range-based batch deletes for tombstone generation), and an optional
// filterable "category" property. NB: InvertedIndexConfig.CleanupIntervalSeconds
// is deliberately not set — it is a no-op for inverted buckets in Weaviate.
func createBM25Schema(cfg *Config, client *weaviate.Client) {
	if err := client.Schema().ClassDeleter().WithClassName(cfg.ClassName).Do(context.Background()); err != nil {
		log.Warnf("Error deleting class %s (may not exist yet): %v", cfg.ClassName, err)
	}

	tokenization := cfg.Tokenization
	if tokenization == "" {
		tokenization = "word"
	}

	properties := []*models.Property{
		{
			Name:            "text",
			DataType:        []string{"text"},
			Tokenization:    tokenization,
			IndexSearchable: boolPtr(true),
			IndexFilterable: boolPtr(false),
		},
		{
			Name:            "title",
			DataType:        []string{"text"},
			Tokenization:    tokenization,
			IndexSearchable: boolPtr(true),
			IndexFilterable: boolPtr(false),
		},
		{
			Name:            "docId",
			DataType:        []string{"int"},
			IndexFilterable: boolPtr(true),
			IndexSearchable: boolPtr(false),
		},
	}
	if cfg.Filter {
		properties = append(properties, &models.Property{
			Name:            "category",
			DataType:        []string{"text"},
			IndexFilterable: boolPtr(true),
			IndexSearchable: boolPtr(false),
		})
	}

	invertedIndexConfig := &models.InvertedIndexConfig{}
	if cfg.BM25K1 > 0 || cfg.BM25B > 0 {
		invertedIndexConfig.Bm25 = &models.BM25Config{
			K1: float32(cfg.BM25K1),
			B:  float32(cfg.BM25B),
		}
	}

	classObj := &models.Class{
		Class:               cfg.ClassName,
		Description:         fmt.Sprintf("Created by the Weaviate BM25 Benchmarker at %s", time.Now().String()),
		Vectorizer:          "none",
		Properties:          properties,
		InvertedIndexConfig: invertedIndexConfig,
	}
	if cfg.NumTenants > 0 {
		classObj.MultiTenancyConfig = &models.MultiTenancyConfig{Enabled: true}
	}
	if cfg.Shards > 1 {
		classObj.ShardingConfig = map[string]interface{}{"desiredCount": cfg.Shards}
	}
	if cfg.ReplicationFactor > 1 || cfg.AsyncReplicationEnabled {
		classObj.ReplicationConfig = &models.ReplicationConfig{
			Factor:       int64(cfg.ReplicationFactor),
			AsyncEnabled: cfg.AsyncReplicationEnabled,
		}
	}

	if err := client.Schema().ClassCreator().WithClass(classObj).Do(context.Background()); err != nil {
		log.Fatalf("Error creating class %s: %v", cfg.ClassName, err)
	}
	log.Printf("Created BM25 class %s", cfg.ClassName)
}

// loadBM25Data imports the corpus using the shared 8-worker import pipeline. With
// multi-tenancy it imports a full copy of the corpus into each tenant (mirroring
// the ANN multi-tenant path). Tenant-scoped work uses local Config copies so the
// shared cfg is never mutated.
func loadBM25Data(ds *BeirDataset, cfg *Config, client *weaviate.Client) time.Duration {
	start := time.Now()
	if cfg.NumTenants > 0 {
		for i := 0; i < cfg.NumTenants; i++ {
			tenantCfg := *cfg
			tenantCfg.Tenant = fmt.Sprintf("%d", i)
			addTenantIfNeeded(&tenantCfg, client)
			loadTrainData(ds, &tenantCfg, 0, 0, 0)
		}
	} else {
		loadTrainData(ds, cfg, 0, 0, 0)
	}
	elapsed := time.Since(start)
	log.WithFields(log.Fields{"duration": elapsed, "tenants": cfg.NumTenants}).Printf("BM25 import complete")
	return elapsed
}

// runBM25Queries runs the phased measurement (baseline, then tombstone phases if
// requested) and writes one labeled result row per phase to ./results/{runID}.json.
func runBM25Queries(cfg *Config, client *weaviate.Client, ds *BeirDataset, queries []string, importTime time.Duration) {
	runID := fmt.Sprintf("%d", time.Now().Unix())
	var rows []map[string]interface{}

	measure := func(phase string, iteration int, tombstoneRatio float64) {
		var result Results
		if cfg.QueryDuration > 0 {
			result = benchmarkBM25Duration(*cfg, queries)
		} else {
			result = benchmarkBM25(*cfg, queries)
		}
		log.WithFields(log.Fields{
			"phase": phase, "iteration": iteration, "tombstoneRatio": tombstoneRatio,
			"mean": result.Mean, "qps": result.QueriesPerSecond,
			"parallel": cfg.Parallel, "limit": cfg.Limit, "failed": result.Failed,
		}).Infof("BM25 %s result", phase)
		if _, err := result.WriteTextTo(os.Stdout); err != nil {
			log.Warnf("Error writing results to stdout: %v", err)
		}
		rows = append(rows, bm25ResultRow(cfg, result, importTime, runID, phase, iteration, tombstoneRatio))
	}

	// Baseline (0 tombstones). Always run when there is no tombstone phase, so at
	// least one measurement is produced.
	if cfg.MeasureBaseline || cfg.TombstonePercentage <= 0 {
		measure("baseline", 0, 0)
	}

	if cfg.TombstonePercentage > 0 {
		total := ds.NumTrainVectors()
		k := int(math.Floor(cfg.TombstonePercentage * float64(total)))
		if k < 1 {
			k = 1
		}
		iterations := cfg.TombstoneIterations
		if iterations < 1 {
			iterations = 1
		}

		if cfg.TombstoneConcurrent {
			// Sustain churn in the background while a single query window runs.
			stop := make(chan struct{})
			done := make(chan struct{})
			go func() {
				defer close(done)
				for {
					select {
					case <-stop:
						return
					default:
						generateTombstones(cfg, client, ds, k)
					}
				}
			}()
			measure("concurrent", 0, cfg.TombstonePercentage)
			close(stop)
			<-done
		} else {
			for it := 1; it <= iterations; it++ {
				generateTombstones(cfg, client, ds, k)
				measure("tombstoned", it, cfg.TombstonePercentage*float64(it))
			}
		}
	}

	writeBM25Results(cfg, runID, rows)
}

// benchmarkBM25 runs one pass (or cfg.Queries executions) of BM25 queries through
// the shared worker-pool harness. Neighbors are left empty so recall/NDCG are
// skipped (this is a pure performance test).
func benchmarkBM25(cfg Config, queries []string) Results {
	if cfg.Queries == 0 {
		cfg.Queries = len(queries)
	}
	i := 0
	return benchmark(cfg, func(className string) QueryWithNeighbors {
		query := queries[i%len(queries)]
		i++
		tenant := cfg.Tenant
		if cfg.NumTenants > 0 {
			tenant = fmt.Sprintf("%d", rand.Intn(cfg.NumTenants))
		}
		filter := -1
		if cfg.Filter && cfg.FilterCount > 0 {
			filter = rand.Intn(cfg.FilterCount)
		}
		return QueryWithNeighbors{
			Query: bm25QueryGrpc(&cfg, query, tenant, filter),
		}
	})
}

// benchmarkBM25Duration repeats benchmarkBM25 for cfg.QueryDuration seconds and
// returns the median across runs (mirrors benchmarkANNDuration).
func benchmarkBM25Duration(cfg Config, queries []string) Results {
	var samples sampledResults
	startTime := time.Now()
	var results Results
	iterations := 0
	for time.Since(startTime) < time.Duration(cfg.QueryDuration)*time.Second {
		results = benchmarkBM25(cfg, queries)
		samples.Min = append(samples.Min, results.Min)
		samples.Max = append(samples.Max, results.Max)
		samples.Mean = append(samples.Mean, results.Mean)
		samples.Took = append(samples.Took, results.Took)
		samples.QueriesPerSecond = append(samples.QueriesPerSecond, results.QueriesPerSecond)
		samples.Results = append(samples.Results, results)
		iterations++
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

// generateTombstones creates inverted-index tombstones for the first k documents
// (docId < k). Under multi-tenancy each tenant has its own inverted index, so it
// churns every tenant. It never mutates the shared cfg (it passes per-tenant
// Config copies), which keeps it safe to run concurrently with the query phase.
func generateTombstones(cfg *Config, client *weaviate.Client, ds *BeirDataset, k int) {
	if cfg.NumTenants > 0 {
		for i := 0; i < cfg.NumTenants; i++ {
			tombstoneOnce(*cfg, client, ds, k, fmt.Sprintf("%d", i))
		}
		return
	}
	tombstoneOnce(*cfg, client, ds, k, cfg.Tenant)
}

// tombstoneOnce deletes docId < k in a single tenant (empty tenant = single-tenant
// mode). In "update" mode it reinserts those same documents (identical UUIDs +
// docIds) so the corpus size holds and each old docID becomes a fresh tombstone —
// matching an update-heavy workload. cfg is taken by value so the tenant
// assignment stays local.
func tombstoneOnce(cfg Config, client *weaviate.Client, ds *BeirDataset, k int, tenant string) {
	cfg.Tenant = tenant
	deleted := batchDeleteDocIDRange(&cfg, client, k)
	log.WithFields(log.Fields{
		"deleted": deleted, "mode": cfg.TombstoneMode, "range": k, "tenant": tenant,
	}).Printf("Generated tombstones")

	if cfg.TombstoneMode == "delete" {
		return
	}
	// Default ("update"): reinsert the same document range.
	loadTrainData(ds, &cfg, 0, uint(k), 0)
}

// batchDeleteDocIDRange deletes all objects with docId < end, looping because the
// server deletes at most QUERY_MAXIMUM_RESULTS (~10k) objects per call. Returns
// the total number of objects deleted.
func batchDeleteDocIDRange(cfg *Config, client *weaviate.Client, end int) int64 {
	where := filters.Where().
		WithPath([]string{"docId"}).
		WithOperator(filters.LessThan).
		WithValueInt(int64(end))

	var total int64
	for {
		deleter := client.Batch().ObjectsBatchDeleter().
			WithClassName(cfg.ClassName).
			WithWhere(where).
			WithOutput("minimal")
		if cfg.Tenant != "" {
			deleter = deleter.WithTenant(cfg.Tenant)
		}
		resp, err := deleter.Do(context.Background())
		if err != nil {
			log.Fatalf("Error batch-deleting objects for tombstones: %v", err)
		}
		if resp.Results == nil || resp.Results.Successful == 0 {
			break
		}
		total += resp.Results.Successful
	}
	return total
}

// bm25ResultRow builds one JSON result row, reusing the ANN-compatible
// ResultsJSONBenchmark shape and adding BM25/tombstone-specific fields plus any
// user --labels, so runs remain ingestible by weaviate-performance-tests.
func bm25ResultRow(cfg *Config, result Results, importTime time.Duration, runID, phase string, iteration int, tombstoneRatio float64) map[string]interface{} {
	p99 := 0.0
	if len(result.Percentiles) > 0 {
		p99 = result.Percentiles[len(result.Percentiles)-1].Seconds()
	}
	benchResult := ResultsJSONBenchmark{
		Api:              cfg.API,
		Mean:             result.Mean.Seconds(),
		P99Latency:       p99,
		QueriesPerSecond: result.QueriesPerSecond,
		Shards:           cfg.Shards,
		Parallelization:  cfg.Parallel,
		Limit:            cfg.Limit,
		ImportTime:       importTime.Seconds(),
		RunID:            runID,
		IterationRunID:   fmt.Sprintf("%d", iteration),
		Dataset:          filepath.Base(cfg.CorpusFile),
		Timestamp:        time.Now().Format(time.RFC3339),
	}

	jsonData, err := json.Marshal(benchResult)
	if err != nil {
		log.Fatalf("Error converting result to json: %v", err)
	}
	var row map[string]interface{}
	if err := json.Unmarshal(jsonData, &row); err != nil {
		log.Fatalf("Error converting json to map: %v", err)
	}

	row["phase"] = phase
	row["tombstoneRatio"] = tombstoneRatio
	row["tombstoneMode"] = cfg.TombstoneMode
	row["bm25Operator"] = cfg.BM25Operator
	row["queryProperties"] = cfg.QueryProperties
	row["bm25k1"] = cfg.BM25K1
	row["bm25b"] = cfg.BM25B
	if cfg.Filter {
		row["filterCount"] = cfg.FilterCount
	}
	for key, value := range cfg.LabelMap {
		row[key] = value
	}
	return row
}

func writeBM25Results(cfg *Config, runID string, rows []map[string]interface{}) {
	if len(rows) == 0 {
		return
	}
	data, err := json.MarshalIndent(rows, "", "    ")
	if err != nil {
		log.Fatalf("Error marshaling benchmark results: %v", err)
	}

	if err := os.MkdirAll("./results", 0o755); err != nil {
		log.Fatalf("Error creating results directory: %v", err)
	}
	path := fmt.Sprintf("./results/%s.json", runID)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		log.Fatalf("Error writing benchmark results to file: %v", err)
	}
	log.Printf("Wrote BM25 results to %s", path)

	if cfg.OutputFile != "" {
		if err := os.WriteFile(cfg.OutputFile, data, 0o644); err != nil {
			log.Warnf("Error writing to output file %s: %v", cfg.OutputFile, err)
		}
	}
}

func initBM25Benchmark() {
	rootCmd.AddCommand(bm25BenchmarkCommand)

	numCPU := runtime.NumCPU()
	f := bm25BenchmarkCommand.PersistentFlags()

	// Dataset & schema
	f.StringVar(&globalConfig.CorpusFile, "corpus", "", "Path to the BEIR corpus.jsonl file (required unless --query)")
	f.StringVar(&globalConfig.QueriesFile, "queriesFile", "", "Path to the BEIR queries.jsonl file (required)")
	f.StringVarP(&globalConfig.ClassName, "className", "c", "Bm25Bench", "Class name for the benchmark collection")
	f.StringVar(&globalConfig.Tokenization, "tokenization", "word", "Tokenization for the text properties (word, lowercase, whitespace, field, trigram)")
	f.Float64Var(&globalConfig.BM25K1, "bm25k1", 1.2, "BM25 k1 parameter")
	f.Float64Var(&globalConfig.BM25B, "bm25b", 0.75, "BM25 b parameter")

	// Query behavior
	f.StringVar(&globalConfig.SearchType, "searchType", "bm25", "Search type (bm25; hybrid reserved for a future version)")
	f.StringVar(&globalConfig.QueryProperties, "queryProperties", "text", "Comma-separated properties to run BM25 against")
	f.StringVar(&globalConfig.BM25Operator, "bm25Operator", "", "BM25 search operator: or, and, or empty for server default")
	f.IntVar(&globalConfig.MinOrTokens, "minimumOrTokensMatch", 0, "minimumOrTokensMatch for the OR operator (0 = unset)")
	f.IntVar(&globalConfig.Queries, "queries", 0, "Number of query executions per phase (0 = one pass over the query set)")
	f.IntVar(&globalConfig.QueryDuration, "queryDuration", 0, "Query for the specified duration in seconds instead of a fixed count")
	f.IntVarP(&globalConfig.Limit, "limit", "l", 10, "Query limit (top_k)")
	f.IntVarP(&globalConfig.Parallel, "parallel", "p", numCPU, "Number of parallel query threads")

	// Filtering
	f.BoolVar(&globalConfig.Filter, "filter", false, "Combine BM25 with an equality filter on a category bucket")
	f.IntVar(&globalConfig.FilterCount, "filterCount", 10, "Number of category buckets (filter selectivity = 1/filterCount)")

	// Tombstone generation
	f.Float64Var(&globalConfig.TombstonePercentage, "tombstonePercentage", 0.0, "Fraction of docs (0..1) to tombstone per iteration (0 disables)")
	f.StringVar(&globalConfig.TombstoneMode, "tombstoneMode", "update", "How to create tombstones: update (delete+reinsert) or delete")
	f.IntVar(&globalConfig.TombstoneIterations, "tombstoneIterations", 1, "Number of tombstone-generation iterations (re-measured each time)")
	f.BoolVar(&globalConfig.TombstoneConcurrent, "tombstoneConcurrent", false, "Churn tombstones in the background during the query run")
	f.BoolVar(&globalConfig.MeasureBaseline, "measureBaseline", true, "Run a 0-tombstone baseline before the tombstone phase")

	// Import & topology
	f.IntVarP(&globalConfig.BatchSize, "batchSize", "b", 1000, "Batch size for import")
	f.IntVar(&globalConfig.NumTenants, "numTenants", 0, "Number of tenants; each gets a full corpus copy (0 = single-tenant)")
	f.IntVar(&globalConfig.Shards, "shards", 1, "Number of shards")
	f.IntVar(&globalConfig.ReplicationFactor, "replicationFactor", 1, "Replication factor")

	// Connection & output
	f.StringVarP(&globalConfig.Origin, "grpcOrigin", "u", "localhost:50051", "The gRPC origin that Weaviate is running at")
	f.StringVar(&globalConfig.HttpOrigin, "httpOrigin", "localhost:8080", "The HTTP origin for Weaviate")
	f.StringVar(&globalConfig.HttpScheme, "httpScheme", "http", "The HTTP scheme (http or https)")
	f.StringVarP(&globalConfig.API, "api", "a", "grpc", "API to use (only grpc is supported)")
	f.StringVar(&globalConfig.Labels, "labels", "", "Labels of format key1=value1,key2=value2 merged into each result row")
	f.StringVarP(&globalConfig.OutputFormat, "format", "f", "text", "Output format, one of [text, json]")
	f.StringVarP(&globalConfig.OutputFile, "output", "o", "", "Optional output file for the results JSON")

	// Lifecycle / skip options
	f.BoolVar(&globalConfig.QueryOnly, "query", false, "Do not import; query an existing collection")
	f.BoolVar(&globalConfig.SkipQuery, "skipQuery", false, "Only import data, skip the query phase")
	f.BoolVar(&globalConfig.ExistingSchema, "existingSchema", false, "Leave the schema as-is (do not recreate the class)")
	f.IntVar(&globalConfig.QueryDelaySeconds, "queryDelaySeconds", 30, "How long to wait after import before querying")

	// Memory monitoring
	f.StringVar(&globalConfig.MetricsEndpoint, "metricsEndpoint", "http://localhost:2112/metrics", "Weaviate metrics endpoint")
	f.BoolVar(&globalConfig.MemoryMonitoringEnabled, "memoryMonitoringEnabled", false, "Enable continuous memory monitoring")
	f.IntVar(&globalConfig.MemoryMonitoringInterval, "memoryMonitoringInterval", 5, "Memory monitoring interval in seconds")
	f.StringVar(&globalConfig.MemoryMonitoringFile, "memoryMonitoringFile", "", "Memory monitoring output file")
}
