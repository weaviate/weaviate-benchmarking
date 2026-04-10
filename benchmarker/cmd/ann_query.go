package cmd

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/constraints"
)

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

func runQueries(cfg *Config, importTime time.Duration, testData [][]float32, neighbors [][]int, filters []int) {
	runID := strconv.FormatInt(time.Now().Unix(), 10)

	efCandidates, err := parseEfValues(cfg.EfArray)
	if err != nil {
		log.Fatalf("Error parsing efArray, expected commas separated format \"16,32,64\" but:%v\n", err)
	}

	// Read once at this point (after import and compaction delay) to get accurate memory stats
	memstats := &Memstats{}
	if !cfg.SkipMemoryStats {
		memstats, err = readMemoryMetrics(cfg)
		if err != nil {
			log.Warnf("Error reading memory stats: %v", err)
			memstats = &Memstats{}
		}
	}

	client := createClient(cfg)
	os.Mkdir("./results", 0o755)

	iteration := 0
	for {
		shouldStop := shouldStopRunQueries(iteration, cfg)
		if cfg.WaitForBackground && shouldStop {
			// todo
			break
		}

		iteration++
		iterationRunID := fmt.Sprintf("%d", iteration)
		isFinalIteration := !cfg.WaitForBackground || shouldStop

		benchmarkResultsMap := make([]map[string]interface{}, 0, len(efCandidates))
		for _, ef := range efCandidates {
			updateEf(ef, cfg, client)

			var result Results

			if cfg.QueryDuration > 0 {
				result = benchmarkANNDuration(*cfg, testData, neighbors, filters)
			} else {
				result = benchmarkANN(*cfg, testData, neighbors, filters)
			}

			if cfg.IndexType == "hnsw" || cfg.IndexType == "dynamic" {
				log.WithFields(log.Fields{
					"mean": result.Mean, "qps": result.QueriesPerSecond, "recall": result.Recall, "ndcg": result.NDCG,
					"parallel": cfg.Parallel, "limit": cfg.Limit,
					"api": cfg.API, "ef": ef, "count": result.Total, "failed": result.Failed,
				}).Info("Benchmark result")
			} else if cfg.IndexType == "hfresh" {
				log.WithFields(log.Fields{
					"mean": result.Mean, "qps": result.QueriesPerSecond, "recall": result.Recall, "ndcg": result.NDCG,
					"parallel": cfg.Parallel, "limit": cfg.Limit,
					"api": cfg.API, "searchProbe": ef, "count": result.Total, "failed": result.Failed,
				}).Info("Benchmark result")
			} else {
				log.WithFields(log.Fields{
					"mean": result.Mean, "qps": result.QueriesPerSecond, "recall": result.Recall, "ndcg": result.NDCG,
					"parallel": cfg.Parallel, "limit": cfg.Limit,
					"api": cfg.API, "rescoreLimit": ef, "count": result.Total, "failed": result.Failed,
				}).Info("Benchmark result")
			}

			dataset := filepath.Base(cfg.BenchmarkFile)

			var resultMap map[string]interface{}

			benchResult := ResultsJSONBenchmark{
				Api:              cfg.API,
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
				IterationRunID:   iterationRunID,
				Dataset:          dataset,
				NDCG:             result.NDCG,
				Recall:           result.Recall,
				HeapAllocBytes:   memstats.HeapAllocBytes,
				HeapInuseBytes:   memstats.HeapInuseBytes,
				HeapSysBytes:     memstats.HeapSysBytes,
				Timestamp: time.Now().Format(time.RFC3339),
			}
			if cfg.MMRBalance >= 0 {
				balance := cfg.MMRBalance
				benchResult.MMRBalance = &balance
				mmrLimit := cfg.MMRLimit
				if mmrLimit == 0 {
					mmrLimit = cfg.Limit
				}
				benchResult.MMRLimit = &mmrLimit
			}
			switch cfg.IndexType {
			case "flat":
				benchResult.RescoreLimit = ef
			case "hnsw", "dynamic":
				benchResult.Ef = ef
			case "hfresh":
				benchResult.SearchProbe = ef
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

			if isFinalIteration {
				resultMap["finalIteration"] = true
			}

			benchmarkResultsMap = append(benchmarkResultsMap, resultMap)

		}

		data, err := json.MarshalIndent(benchmarkResultsMap, "", "    ")
		if err != nil {
			log.Fatalf("Error marshaling benchmark results: %v", err)
		}

		err = os.WriteFile(fmt.Sprintf("./results/%s.json", runID), data, 0o644)
		if err != nil {
			log.Fatalf("Error writing benchmark results to file: %v", err)
		}

		if !cfg.WaitForBackground {
			break
		}

	}
}

func shouldStopRunQueries(iteration int, cfg *Config) bool {
	if cfg.IndexType != "hfresh" {
		return true
	}
	if iteration == 0 { // we want to trigger merge operations
		return false
	}

	metrics, err := readHFreshMetrics(cfg)
	if err != nil {
		log.WithError(err).Warn("Failed to read HFresh pending operations metrics")
		return false
	}

	noPendingOps := metrics.PendingSplitOperations == 0 &&
		metrics.PendingMergeOperations == 0 &&
		metrics.PendingReassignOperations == 0

	if noPendingOps {
		log.WithFields(log.Fields{
			"iteration": iteration,
		}).Info("All HFresh background operations complete")
		return true
	}

	secs := 30

	for {
		metrics, err := readHFreshMetrics(cfg)
		if err != nil {
			log.WithError(err).Warn("Failed to read HFresh pending operations metrics")
			return false
		}

		log.WithFields(log.Fields{
			"iteration":                 iteration,
			"pendingSplitOperations":    metrics.PendingSplitOperations,
			"pendingMergeOperations":    metrics.PendingMergeOperations,
			"pendingReassignOperations": metrics.PendingReassignOperations,
		}).Info("HFresh background operations still running, checking again in ", secs, " seconds")
		noPendingOps := metrics.PendingSplitOperations == 0 &&
			metrics.PendingMergeOperations == 0 &&
			metrics.PendingReassignOperations == 0

		if noPendingOps {
			break
		}
		time.Sleep(time.Duration(secs) * time.Second)
	}

	return false
}

func benchmarkANN(cfg Config, queries Queries, neighbors Neighbors, filters []int) Results {
	cfg.Queries = len(queries)

	i := 0
	return benchmark(cfg, func(className string) QueryWithNeighbors {
		defer func() { i++ }()

		tenant := ""
		if cfg.NumTenants > 0 {
			tenant = fmt.Sprint(rand.Intn(cfg.NumTenants))
		}
		filter := -1
		if cfg.Filter {
			filter = filters[i]
		}

		return QueryWithNeighbors{
			Query:     nearVectorQueryGrpc(&cfg, queries[i], tenant, filter),
			Neighbors: neighbors[i],
		}
	})
}

type Number interface {
	constraints.Float | constraints.Integer
}

func median[T Number](data []T) float64 {
	dataCopy := make([]T, len(data))
	copy(dataCopy, data)

	slices.Sort(dataCopy)

	var median float64
	l := len(dataCopy)
	if l == 0 {
		return 0
	} else if l%2 == 0 {
		median = float64((dataCopy[l/2-1] + dataCopy[l/2]) / 2.0)
	} else {
		median = float64(dataCopy[l/2])
	}

	return median
}

type sampledResults struct {
	Min              []time.Duration
	Max              []time.Duration
	Mean             []time.Duration
	Took             []time.Duration
	QueriesPerSecond []float64
	Recall           []float64
	NDCG             []float64
	Results          []Results
}

func benchmarkANNDuration(cfg Config, queries Queries, neighbors Neighbors, filters []int) Results {
	cfg.Queries = len(queries)

	var samples sampledResults

	startTime := time.Now()

	var results Results

	for time.Since(startTime) < time.Duration(cfg.QueryDuration)*time.Second {
		results = benchmarkANN(cfg, queries, neighbors, filters)
		samples.Min = append(samples.Min, results.Min)
		samples.Max = append(samples.Max, results.Max)
		samples.Mean = append(samples.Mean, results.Mean)
		samples.Took = append(samples.Took, results.Took)
		samples.QueriesPerSecond = append(samples.QueriesPerSecond, results.QueriesPerSecond)
		samples.NDCG = append(samples.NDCG, results.NDCG)
		samples.Recall = append(samples.Recall, results.Recall)
		samples.Results = append(samples.Results, results)
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
	medianResult.Recall = median(samples.Recall)
	medianResult.NDCG = median(samples.NDCG)

	return medianResult
}
