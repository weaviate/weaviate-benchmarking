package main

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
)

func newTestExporter(t *testing.T) (*Exporter, *prometheus.Registry) {
	t.Helper()

	reg := prometheus.NewRegistry()
	exporter := NewExporter()
	exporter.initializeMetrics(reg)
	return exporter, reg
}

func writeJSONFile(t *testing.T, dir, name, content string) string {
	t.Helper()

	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	return path
}

func touchFile(t *testing.T, path string, modTime time.Time) {
	t.Helper()

	if err := os.Chtimes(path, modTime, modTime); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
}

func TestPrometheusLabelsDefaults(t *testing.T) {
	labels := prometheusLabels(MetricData{DatasetFile: "x.hdf5"})
	if labels["branch"] != "main" {
		t.Fatalf("branch = %q, want main", labels["branch"])
	}
	if labels["test_id"] != "NA" {
		t.Fatalf("test_id = %q, want NA", labels["test_id"])
	}
}

func TestPrometheusLabelsPreservesValues(t *testing.T) {
	labels := prometheusLabels(MetricData{
		Branch:         "release",
		DatasetFile:    "data.hdf5",
		EFConstruction: 256,
		MaxConnections: 32,
		Limit:          10,
		EF:             16,
		SearchProbe:    8,
		Shards:         2,
		TestID:         "run-42",
	})

	want := map[string]string{
		"branch":          "release",
		"dataset":         "data.hdf5",
		"ef_construction": "256",
		"max_connections": "32",
		"limit":           "10",
		"ef":              "16",
		"search_probe":    "8",
		"shards":          "2",
		"test_id":         "run-42",
	}
	for key, value := range want {
		if labels[key] != value {
			t.Fatalf("label %s = %q, want %q", key, labels[key], value)
		}
	}
}

func TestFindLatestJSONFile(t *testing.T) {
	dir := t.TempDir()

	olderPath := writeJSONFile(t, dir, "older.json", `[]`)
	newerPath := writeJSONFile(t, dir, "newer.json", `[]`)
	writeJSONFile(t, dir, "notes.txt", `not json`)

	base := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	touchFile(t, olderPath, base)
	touchFile(t, newerPath, base.Add(time.Hour))

	got, err := findLatestJSONFile(dir)
	if err != nil {
		t.Fatalf("findLatestJSONFile: %v", err)
	}
	if got != newerPath {
		t.Fatalf("latest file = %q, want %q", got, newerPath)
	}
}

func TestFindLatestJSONFileAwaitingResults(t *testing.T) {
	dir := t.TempDir()

	_, err := findLatestJSONFile(dir)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "awaiting results") {
		t.Fatalf("error = %q, want awaiting results", err.Error())
	}
}

func TestFindLatestJSONFileWalkError(t *testing.T) {
	_, err := findLatestJSONFile(filepath.Join(t.TempDir(), "missing-subdir", "nope"))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestProcessJSONFile(t *testing.T) {
	exporter, reg := newTestExporter(t)

	fixture := filepath.Join("testdata", "sample_results.json")
	if err := exporter.processJSONFile(fixture); err != nil {
		t.Fatalf("processJSONFile: %v", err)
	}

	labels := map[string]string{
		"branch":          "feature-branch",
		"dataset":         "sample.hdf5",
		"ef_construction": "256",
		"max_connections": "32",
		"limit":           "10",
		"ef":              "16",
		"search_probe":    "8",
		"shards":          "2",
		"test_id":         "run-001",
	}
	if got := gaugeValue(t, reg, "benchmark_latency_mean", labels); got != 0.01 {
		t.Fatalf("latency_mean = %v, want 0.01", got)
	}
	if got := gaugeValue(t, reg, "benchmark_qps", labels); got != 1000 {
		t.Fatalf("qps = %v, want 1000", got)
	}

	defaultLabels := map[string]string{
		"branch":          "main",
		"dataset":         "defaults.hdf5",
		"ef_construction": "128",
		"max_connections": "16",
		"limit":           "5",
		"ef":              "24",
		"search_probe":    "0",
		"shards":          "1",
		"test_id":         "NA",
	}
	if got := gaugeValue(t, reg, "benchmark_latency_mean", defaultLabels); got != 0.02 {
		t.Fatalf("default latency_mean = %v, want 0.02", got)
	}
}

func TestProcessJSONFileResetsStaleSeries(t *testing.T) {
	exporter, reg := newTestExporter(t)
	dir := t.TempDir()

	first := writeJSONFile(t, dir, "first.json", `[
		{
			"branch": "only-once",
			"dataset_file": "gone.hdf5",
			"ef": 1,
			"efConstruction": 1,
			"limit": 1,
			"maxConnections": 1,
			"meanLatency": 1,
			"test_id": "t1"
		}
	]`)
	second := writeJSONFile(t, dir, "second.json", `[
		{
			"branch": "replacement",
			"dataset_file": "stay.hdf5",
			"ef": 2,
			"efConstruction": 2,
			"limit": 2,
			"maxConnections": 2,
			"meanLatency": 2,
			"test_id": "t2"
		}
	]`)
	touchFile(t, first, time.Now().Add(-time.Hour))
	touchFile(t, second, time.Now())

	if err := exporter.processJSONFile(first); err != nil {
		t.Fatalf("process first file: %v", err)
	}
	if err := exporter.processJSONFile(second); err != nil {
		t.Fatalf("process second file: %v", err)
	}

	staleLabels := map[string]string{
		"branch":          "only-once",
		"dataset":         "gone.hdf5",
		"ef_construction": "1",
		"max_connections": "1",
		"limit":           "1",
		"ef":              "1",
		"search_probe":    "0",
		"shards":          "0",
		"test_id":         "t1",
	}
	if _, ok := gaugeValueOK(reg, "benchmark_latency_mean", staleLabels); ok {
		t.Fatal("expected stale series to be removed after reset")
	}
}

func TestProcessJSONFileErrors(t *testing.T) {
	exporter, _ := newTestExporter(t)

	if err := exporter.processJSONFile(filepath.Join(t.TempDir(), "missing.json")); !errors.Is(err, os.ErrNotExist) && !strings.Contains(err.Error(), "error reading file") {
		t.Fatalf("missing file error = %v", err)
	}

	badJSON := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(badJSON, []byte("{"), 0o644); err != nil {
		t.Fatalf("write bad json: %v", err)
	}
	if err := exporter.processJSONFile(badJSON); err == nil || !strings.Contains(err.Error(), "error parsing JSON") {
		t.Fatalf("bad json error = %v", err)
	}
}

func TestMetricsHTTPServe(t *testing.T) {
	exporter, reg := newTestExporter(t)

	fixture := filepath.Join("testdata", "sample_results.json")
	if err := exporter.processJSONFile(fixture); err != nil {
		t.Fatalf("processJSONFile: %v", err)
	}

	srv := httptest.NewServer(promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	text := string(body)
	for _, want := range []string{
		"benchmark_latency_mean",
		`branch="feature-branch"`,
		`dataset="sample.hdf5"`,
		"benchmark_qps",
		"benchmark_recall",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("metrics body missing %q\nbody:\n%s", want, text)
		}
	}
}

func gaugeValueOK(reg *prometheus.Registry, name string, labels map[string]string) (float64, bool) {
	mfs, err := reg.Gather()
	if err != nil {
		return 0, false
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, metric := range mf.GetMetric() {
			if labelsMatch(metric.GetLabel(), labels) {
				return metric.GetGauge().GetValue(), true
			}
		}
	}
	return 0, false
}

func gaugeValue(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) float64 {
	t.Helper()

	value, ok := gaugeValueOK(reg, name, labels)
	if !ok {
		t.Fatalf("metric %s with labels %#v not found", name, labels)
	}
	return value
}

func labelsMatch(metricLabels []*dto.LabelPair, want map[string]string) bool {
	if len(metricLabels) != len(want) {
		return false
	}
	for _, pair := range metricLabels {
		if want[pair.GetName()] != pair.GetValue() {
			return false
		}
	}
	return true
}
