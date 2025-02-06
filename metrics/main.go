package main

import (
	"encoding/json"
	"fmt"
	"github.com/fsnotify/fsnotify"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
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
	labels := []string{"branch", "dataset", "ef_construction", "max_connections", "limit", "ef"}

	metricNames := []struct {
		name string
		help string
	}{
		{"latency_mean", "Mean latency of queries"},
		{"latency_p99", "99th percentile latency of queries"},
		{"qps", "Queries per second"},
		{"recall", "Recall metric"},
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
	content, err := ioutil.ReadFile(filepath)
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
			"ef":             fmt.Sprintf("%d", data.EF),
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
	}

	log.Printf("Successfully processed file: %s", filepath)
	return nil
}

func watchDirectory(dirPath string, exporter *Exporter) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("error creating watcher: %v", err)
	}
	defer watcher.Close()

	// Process existing files
	files, err := ioutil.ReadDir(dirPath)
	if err != nil {
		return fmt.Errorf("error reading directory: %v", err)
	}

	for _, file := range files {
		if filepath.Ext(file.Name()) == ".json" {
			fullPath := filepath.Join(dirPath, file.Name())
			if err := exporter.processJSONFile(fullPath); err != nil {
				log.Printf("Error processing existing file %s: %v", fullPath, err)
			}
		}
	}

	// Watch for new files and changes
	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				if event.Op&fsnotify.Write == fsnotify.Write || event.Op&fsnotify.Create == fsnotify.Create {
					if filepath.Ext(event.Name) == ".json" {
						if err := exporter.processJSONFile(event.Name); err != nil {
							log.Printf("Error processing file %s: %v", event.Name, err)
						}
					}
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Printf("Error watching directory: %v", err)
			}
		}
	}()

	if err := watcher.Add(dirPath); err != nil {
		return fmt.Errorf("error adding directory to watcher: %v", err)
	}

	return nil
}

func main() {
	if len(os.Args) != 2 {
		log.Fatal("Please provide the directory path to watch as an argument")
	}
	dirPath := os.Args[1]

	// Create and initialize exporter
	exporter := NewExporter()
	exporter.initializeMetrics()

	// Start watching directory
	if err := watchDirectory(dirPath, exporter); err != nil {
		log.Fatal(err)
	}

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
	log.Printf("Starting server on :2120")
	if err := http.ListenAndServe(":2120", nil); err != nil {
		log.Fatal(err)
	}
}
