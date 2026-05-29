package main

import (
	"context"
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
)

func TestPollDirectoryPublishesLatestFile(t *testing.T) {
	dir := t.TempDir()
	reg := prometheus.NewRegistry()
	exporter := NewExporter()
	exporter.initializeMetrics(reg)

	older := filepath.Join(dir, "older.json")
	newer := filepath.Join(dir, "newer.json")
	if err := os.WriteFile(older, []byte(`[{"branch":"old","dataset_file":"a.hdf5","ef":1,"efConstruction":1,"limit":1,"maxConnections":1,"meanLatency":1,"test_id":"old"}]`), 0o644); err != nil {
		t.Fatalf("write older: %v", err)
	}
	if err := os.WriteFile(newer, []byte(`[{"branch":"new","dataset_file":"b.hdf5","ef":2,"efConstruction":2,"limit":2,"maxConnections":2,"meanLatency":2,"test_id":"new"}]`), 0o644); err != nil {
		t.Fatalf("write newer: %v", err)
	}

	now := time.Now()
	if err := os.Chtimes(older, now.Add(-time.Hour), now.Add(-time.Hour)); err != nil {
		t.Fatalf("chtimes older: %v", err)
	}
	if err := os.Chtimes(newer, now, now); err != nil {
		t.Fatalf("chtimes newer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go pollDirectory(ctx, dir, exporter, 25*time.Millisecond)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/":
			w.Write([]byte(`<a href="/metrics">Metrics</a>`))
		case "/metrics":
			promhttp.HandlerFor(reg, promhttp.HandlerOpts{}).ServeHTTP(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)

	deadline := time.Now().Add(2 * time.Second)
	var body string
	for time.Now().Before(deadline) {
		resp, err := http.Get(srv.URL + "/metrics")
		if err == nil {
			payload, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			if readErr == nil && resp.StatusCode == http.StatusOK {
				body = string(payload)
				if strings.Contains(body, `branch="new"`) && strings.Contains(body, "benchmark_latency_mean") {
					break
				}
			}
		}
		time.Sleep(25 * time.Millisecond)
	}

	if !strings.Contains(body, `branch="new"`) {
		t.Fatalf("metrics never picked up newest file\nbody:\n%s", body)
	}
	if strings.Contains(body, `branch="old"`) {
		t.Fatalf("metrics still contain stale branch=old\nbody:\n%s", body)
	}
}
