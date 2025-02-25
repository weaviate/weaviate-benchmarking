package cmd

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	log "github.com/sirupsen/logrus"
)

// PrometheusConfig holds configuration for Prometheus metrics reporting
type PrometheusConfig struct {
	Enabled    bool
	PushURL    string
	JobName    string
	PushPeriod time.Duration
}

// BenchmarkMetrics holds the Prometheus metrics for the benchmark
type BenchmarkMetrics struct {
	MeanLatency      prometheus.Gauge
	P99Latency       prometheus.Gauge
	QueriesPerSecond prometheus.Gauge
	Recall           prometheus.Gauge
	ImportTime       prometheus.Gauge
	HeapAllocBytes   prometheus.Gauge
	HeapInuseBytes   prometheus.Gauge
	HeapSysBytes     prometheus.Gauge
	EfConstruction   prometheus.Gauge
	MaxConnections   prometheus.Gauge
	Shards           prometheus.Gauge
	Parallelization  prometheus.Gauge
	Limit            prometheus.Gauge
}

// NewBenchmarkMetrics creates a new set of benchmark metrics
func NewBenchmarkMetrics(registry *prometheus.Registry, labels prometheus.Labels) *BenchmarkMetrics {
	metrics := &BenchmarkMetrics{
		MeanLatency: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "weaviate_benchmark_mean_latency_seconds",
			Help:        "Mean latency of benchmark queries in seconds",
			ConstLabels: labels,
		}),
		P99Latency: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "weaviate_benchmark_p99_latency_seconds",
			Help:        "P99 latency of benchmark queries in seconds",
			ConstLabels: labels,
		}),
		QueriesPerSecond: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "weaviate_benchmark_queries_per_second",
			Help:        "Queries per second during benchmark",
			ConstLabels: labels,
		}),
		Recall: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "weaviate_benchmark_recall",
			Help:        "Recall of benchmark queries",
			ConstLabels: labels,
		}),
		ImportTime: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "weaviate_benchmark_import_time_seconds",
			Help:        "Import time in seconds",
			ConstLabels: labels,
		}),
		HeapAllocBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "weaviate_benchmark_heap_alloc_bytes",
			Help:        "Heap allocation in bytes",
			ConstLabels: labels,
		}),
		HeapInuseBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "weaviate_benchmark_heap_inuse_bytes",
			Help:        "Heap in use in bytes",
			ConstLabels: labels,
		}),
		HeapSysBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "weaviate_benchmark_heap_sys_bytes",
			Help:        "Heap system in bytes",
			ConstLabels: labels,
		}),
		EfConstruction: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "weaviate_benchmark_ef_construction",
			Help:        "EF construction parameter",
			ConstLabels: labels,
		}),
		MaxConnections: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "weaviate_benchmark_max_connections",
			Help:        "Max connections parameter",
			ConstLabels: labels,
		}),
		Shards: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "weaviate_benchmark_shards",
			Help:        "Number of shards",
			ConstLabels: labels,
		}),
		Parallelization: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "weaviate_benchmark_parallelization",
			Help:        "Parallelization level",
			ConstLabels: labels,
		}),
		Limit: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "weaviate_benchmark_limit",
			Help:        "Query limit",
			ConstLabels: labels,
		}),
	}

	registry.MustRegister(
		metrics.MeanLatency,
		metrics.P99Latency,
		metrics.QueriesPerSecond,
		metrics.Recall,
		metrics.ImportTime,
		metrics.HeapAllocBytes,
		metrics.HeapInuseBytes,
		metrics.HeapSysBytes,
		metrics.EfConstruction,
		metrics.MaxConnections,
		metrics.Shards,
		metrics.Parallelization,
		metrics.Limit,
	)

	return metrics
}

// PushMetricsToPrometheus pushes the benchmark results to a Prometheus pushgateway
func PushMetricsToPrometheus(cfg *Config, benchResult *ResultsJSONBenchmark) error {
	if !cfg.PrometheusConfig.Enabled || cfg.PrometheusConfig.PushURL == "" {
		return nil
	}

	registry := prometheus.NewRegistry()

	// Create labels from the benchmark result
	labels := prometheus.Labels{
		"api":       benchResult.Api,
		"ef":        fmt.Sprintf("%d", benchResult.Ef),
		"dataset":   benchResult.Dataset,
		"run_id":    benchResult.RunID,
		"timestamp": benchResult.Timestamp,
	}

	// Add custom labels from config
	if cfg.LabelMap != nil {
		for key, value := range cfg.LabelMap {
			labels[key] = value
		}
	}

	// Create metrics
	metrics := NewBenchmarkMetrics(registry, labels)

	// Set metric values
	metrics.MeanLatency.Set(benchResult.Mean)
	metrics.P99Latency.Set(benchResult.P99Latency)
	metrics.QueriesPerSecond.Set(benchResult.QueriesPerSecond)
	metrics.Recall.Set(benchResult.Recall)
	metrics.ImportTime.Set(benchResult.ImportTime)
	metrics.HeapAllocBytes.Set(benchResult.HeapAllocBytes)
	metrics.HeapInuseBytes.Set(benchResult.HeapInuseBytes)
	metrics.HeapSysBytes.Set(benchResult.HeapSysBytes)
	metrics.EfConstruction.Set(float64(benchResult.EfConstruction))
	metrics.MaxConnections.Set(float64(benchResult.MaxConnections))
	metrics.Shards.Set(float64(benchResult.Shards))
	metrics.Parallelization.Set(float64(benchResult.Parallelization))
	metrics.Limit.Set(float64(benchResult.Limit))

	// Create a pusher
	pusher := push.New(cfg.PrometheusConfig.PushURL, cfg.PrometheusConfig.JobName).
		Gatherer(registry)

	// Push metrics
	if err := pusher.Push(); err != nil {
		log.WithError(err).Error("Failed to push metrics to Prometheus")
		return err
	}

	log.WithFields(log.Fields{
		"url":     cfg.PrometheusConfig.PushURL,
		"job":     cfg.PrometheusConfig.JobName,
		"run_id":  benchResult.RunID,
		"dataset": benchResult.Dataset,
	}).Info("Successfully pushed metrics to Prometheus")

	return nil
}
