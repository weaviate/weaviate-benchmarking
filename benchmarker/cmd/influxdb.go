package cmd

import (
	"context"
	"fmt"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	log "github.com/sirupsen/logrus"
)

// InfluxDBConfig holds configuration for InfluxDB metrics reporting
type InfluxDBConfig struct {
	Enabled bool
	URL     string
	Token   string
	Org     string
	Bucket  string
}

// PushMetricsToInfluxDB pushes the benchmark results to an InfluxDB instance
func PushMetricsToInfluxDB(cfg *Config, benchResult *ResultsJSONBenchmark) error {
	if !cfg.InfluxDBConfig.Enabled || cfg.InfluxDBConfig.URL == "" {
		return nil
	}

	client := influxdb2.NewClient(cfg.InfluxDBConfig.URL, cfg.InfluxDBConfig.Token)
	defer client.Close()

	writeAPI := client.WriteAPIBlocking(cfg.InfluxDBConfig.Org, cfg.InfluxDBConfig.Bucket)

	// Create a point and add to batch
	p := influxdb2.NewPointWithMeasurement("weaviate_benchmark").
		AddTag("api", benchResult.Api).
		AddTag("ef", fmt.Sprintf("%d", benchResult.Ef)).
		AddTag("dataset", benchResult.Dataset).
		AddTag("run_id", benchResult.RunID).
		AddTag("timestamp", benchResult.Timestamp).
		AddField("mean_latency", benchResult.Mean).
		AddField("p99_latency", benchResult.P99Latency).
		AddField("queries_per_second", benchResult.QueriesPerSecond).
		AddField("recall", benchResult.Recall).
		AddField("import_time", benchResult.ImportTime).
		AddField("heap_alloc_bytes", benchResult.HeapAllocBytes).
		AddField("heap_inuse_bytes", benchResult.HeapInuseBytes).
		AddField("heap_sys_bytes", benchResult.HeapSysBytes).
		AddField("ef_construction", benchResult.EfConstruction).
		AddField("max_connections", benchResult.MaxConnections).
		AddField("shards", benchResult.Shards).
		AddField("parallelization", benchResult.Parallelization).
		AddField("limit", benchResult.Limit).
		SetTime(time.Now())

	// Write the point
	if err := writeAPI.WritePoint(context.Background(), p); err != nil {
		log.WithError(err).Error("Failed to push metrics to InfluxDB")
		return err
	}

	log.WithFields(log.Fields{
		"url":     cfg.InfluxDBConfig.URL,
		"bucket":  cfg.InfluxDBConfig.Bucket,
		"run_id":  benchResult.RunID,
		"dataset": benchResult.Dataset,
	}).Info("Successfully pushed metrics to InfluxDB")

	return nil
}
