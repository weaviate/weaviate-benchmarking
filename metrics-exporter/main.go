package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
)

const (
	namespace = "benchmark"
)

type MetricData struct {
	API            string  `json:"api"`
	Branch         string  `json:"branch"`
	DatasetFile    string  `json:"dataset_file"`
	EF             int     `json:"ef"`
	EFConstruction int     `json:"efConstruction"`
	Limit          int     `json:"limit"`
	MaxConnections int     `json:"maxConnections"`
	MeanLatency    float64 `json:"meanLatency"`
	P99Latency     float64 `json:"p99Latency"`
	QPS            float64 `json:"qps"`
	Recall         float64 `json:"recall"`
	Shards         int     `json:"shards"`
	ImportTime     float64 `json:"importTime"`
	HeapAllocBytes float64 `json:"heap_alloc_bytes"`
	HeapInuseBytes float64 `json:"heap_inuse_bytes"`
	HeapSysBytes   float64 `json:"heap_sys_bytes"`
}

type Exporter struct {
	metrics map[string]*prometheus.GaugeVec
}

func NewExporter() *Exporter {
	return &Exporter{
		metrics: make(map[string]*prometheus.GaugeVec),
	}
}

func (e *Exporter) initializeMetrics() {
	labels := []string{"branch", "dataset", "ef_construction", "max_connections", "limit", "ef", "shards"}

	metricNames := []struct {
		name string
		help string
	}{
		{"latency_mean", "Mean latency of queries"},
		{"latency_p99", "99th percentile latency of queries"},
		{"qps", "Queries per second"},
		{"recall", "Recall metric"},
		{"heap_alloc_bytes", "Heap alloc bytes"},
		{"heap_sys_bytes", "Heap sys bytes"},
		{"heap_inuse_bytes", "Heap inuse bytes"},
		{"import_time", "Import time"},
	}

	for _, metric := range metricNames {
		e.metrics[metric.name] = promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      metric.name,
				Help:      metric.help,
			},
			labels,
		)
	}
}

func (e *Exporter) processJSONFile(filepath string) error {
	content, err := os.ReadFile(filepath)
	if err != nil {
		return fmt.Errorf("error reading file %s: %v", filepath, err)
	}
	var metricsData []MetricData
	if err := json.Unmarshal(content, &metricsData); err != nil {
		return fmt.Errorf("error parsing JSON from file %s: %v", filepath, err)
	}

	// Reset metrics before processing new data
	for _, metric := range e.metrics {
		metric.Reset()
	}

	// Update metrics with new values
	for _, data := range metricsData {
		if data.Branch == "" {
			data.Branch = "main"
		}

		labels := prometheus.Labels{
			"branch":          data.Branch,
			"dataset":         data.DatasetFile,
			"ef_construction": fmt.Sprintf("%d", data.EFConstruction),
			"max_connections": fmt.Sprintf("%d", data.MaxConnections),
			"limit":           fmt.Sprintf("%d", data.Limit),
			"ef":              fmt.Sprintf("%d", data.EF),
			"shards":          fmt.Sprintf("%d", data.Shards),
		}

		if metric := e.metrics["latency_mean"]; metric != nil {
			metric.With(labels).Set(data.MeanLatency)
		}
		if metric := e.metrics["latency_p99"]; metric != nil {
			metric.With(labels).Set(data.P99Latency)
		}
		if metric := e.metrics["qps"]; metric != nil {
			metric.With(labels).Set(data.QPS)
		}
		if metric := e.metrics["recall"]; metric != nil {
			metric.With(labels).Set(data.Recall)
		}
		if metric := e.metrics["import_time"]; metric != nil {
			metric.With(labels).Set(data.ImportTime)
		}
		if metric := e.metrics["heap_inuse_bytes"]; metric != nil {
			metric.With(labels).Set(data.HeapInuseBytes)
		}
		if metric := e.metrics["heap_alloc_bytes"]; metric != nil {
			metric.With(labels).Set(data.HeapAllocBytes)
		}
		if metric := e.metrics["heap_sys_bytes"]; metric != nil {
			metric.With(labels).Set(data.HeapSysBytes)
		}
	}

	log.Printf("Successfully processed file: %s", filepath)
	return nil
}

func findLatestJSONFile(dirPath string) (string, error) {
	var latestFile string
	var latestTime time.Time

	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if filepath.Ext(path) == ".json" && !info.IsDir() {
			if info.ModTime().After(latestTime) {
				latestTime = info.ModTime()
				latestFile = path
			}
		}
		return nil
	})

	if err != nil {
		return "", fmt.Errorf("error walking directory: %v", err)
	}

	if latestFile == "" {
		return "", fmt.Errorf("awaiting results")
	}

	return latestFile, nil
}

func pollDirectory(dirPath string, exporter *Exporter) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	var lastProcessedFile string

	for range ticker.C {
		latestFile, err := findLatestJSONFile(dirPath)
		if err != nil {
			log.Printf("Unable to public metrics: %v", err)
			continue
		}

		// Only process if it's a new file or hasn't been processed yet
		if latestFile != lastProcessedFile {
			if err := exporter.processJSONFile(latestFile); err != nil {
				log.Printf("Error processing file %s: %v", latestFile, err)
				continue
			}
			lastProcessedFile = latestFile
		}
	}
}

func main() {
	var (
		dirPath string
		port    int
	)

	// Create root command
	rootCmd := &cobra.Command{
		Use:   "metrics-exporter",
		Short: "Performance Metrics Exporter",
		Long:  `Monitor weaviate performance metrics and export via Prometheus.`,
		Run: func(cmd *cobra.Command, args []string) {

			prometheus.Unregister(prometheus.NewGoCollector())
			exporter := NewExporter()
			exporter.initializeMetrics()

			// Start polling directory
			go pollDirectory(dirPath, exporter)

			// Set up HTTP server
			http.Handle("/metrics", promhttp.Handler())
			http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte(`<html>
					<head><title>Performance Metrics Exporter</title></head>
					<body>
						<h1>Performance Metrics Exporter</h1>
						<p><a href="/metrics">Metrics</a></p>
					</body>
					</html>`))
			})

			// Start server
			serverAddr := fmt.Sprintf(":%d", port)
			log.Printf("Starting metrics server on port %s", serverAddr)
			if err := http.ListenAndServe(serverAddr, nil); err != nil {
				log.Fatal(err)
			}
		},
		PreRunE: func(cmd *cobra.Command, args []string) error {
			// Validate required arguments
			if dirPath == "" {
				return fmt.Errorf("directory path is required")
			}
			return nil
		},
	}

	rootCmd.Flags().StringVarP(&dirPath, "dir", "d", "", "Results directory path to watch (required)")
	rootCmd.MarkFlagRequired("dir")
	rootCmd.Flags().IntVarP(&port, "port", "p", 2120, "Port to serve metrics on")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
