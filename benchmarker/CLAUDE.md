# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is a multi-threaded benchmarking CLI for Weaviate (a vector database), written in Go. It measures query performance, throughput, recall, and memory usage across various benchmark scenarios. It supports gRPC and GraphQL APIs and works with HDF5 datasets in ann-benchmarks.com format.

## Build & Run

**Prerequisite:** HDF5 library must be installed (`brew install hdf5` on macOS, `apt install libhdf5-dev` on Linux). CGO is required.

```bash
# Run directly (no compile step)
go run . help
go run . ann-benchmark -v ~/datasets/dataset.hdf5 -d l2-squared

# Compile and install
CGO_ENABLED=1 go install .

# Docker
docker build -t weaviate-benchmarker .
```

## Tests

```bash
# Unit tests (no Weaviate required)
go test ./cmd/...
go test -v -run TestAnalyzer ./cmd/...   # run a specific test

# Integration tests (require Weaviate at localhost:8080 / localhost:50051)
docker run -p 8080:8080 -p 50051:50051 semitechnologies/weaviate:latest
go test -tags integration ./cmd/...
go test -tags integration -v -run TestIntegration_RecallForExactNeighbors ./cmd/...
```

Unit tests live in `cmd/benchmark_run_test.go` and cover UUID conversions, NDCG calculation, and results analysis. Integration tests live in `cmd/integration_test.go` and exercise the full insert→query cycle against a real Weaviate instance; they skip automatically if Weaviate is not reachable.

## Architecture

The CLI is built with Cobra. Entry point is `main.go` → `cmd.Execute()`.

**Commands** (each in its own file in `cmd/`):
- `ann-benchmark` — primary command; loads an HDF5 dataset into Weaviate, then queries it
- `random-vectors` — queries with randomly generated vectors
- `random-text` — queries with random text
- `raw` — executes raw GraphQL queries
- `dataset` — loads from dataset repositories
- `colbert` — multi-vector (ColBERT) search

**Core execution flow** (`cmd/benchmark_run.go`):
1. Parse flags → build `Config`
2. Create Weaviate client (gRPC or HTTP)
3. Load dataset via `Dataset` interface (HDF5 or Parquet) using a producer-consumer pipeline with 8 worker goroutines writing batches
4. Wait for async indexing to complete
5. Distribute test queries across a parallel worker pool
6. Collect latencies, compute recall/NDCG against ground truth neighbors
7. Output results as text or JSON; optionally write memory metrics

**Key files:**
| File | Role |
|------|------|
| `cmd/config.go` | `Config` struct and all flag definitions |
| `cmd/ann_benchmark.go` | `ann-benchmark` command — dataset loading orchestration |
| `cmd/benchmark_run.go` | Core query execution, metrics collection, recall/NDCG |
| `cmd/load_data.go` | Data loading pipeline (batching, compression setup) |
| `cmd/hdf5_dataset.go` | HDF5 file I/O via `Dataset` interface |
| `cmd/metrics.go` | Prometheus scraping for Weaviate memory stats |
| `cmd/memory_monitor.go` | Background goroutine for periodic memory snapshots |

**Dataset interface** (`cmd/dataset_interface.go`): abstracts over HDF5, Parquet, and other formats. Implement this interface to add a new dataset source.

## Configuration

All flags are defined in `cmd/config.go`. Key ones for `ann-benchmark`:

| Flag | Default | Notes |
|------|---------|-------|
| `-v` / `--vectors` | — | Path to HDF5 file (required) |
| `-d` / `--distance` | — | Distance metric, e.g. `cosine`, `l2-squared` (required) |
| `-u` / `--origin` | `localhost:50051` | gRPC endpoint |
| `--httpOrigin` | `localhost:8080` | HTTP endpoint |
| `-a` / `--api` | `grpc` | `grpc` or `graphql` |
| `-p` / `--parallel` | `8` | Worker threads |
| `-l` / `--limit` | `10` | Top-k results |
| `--pq` / `--sq` / `--rq` | `disabled` | Compression: `disabled`, `auto`, or `enabled` |
| `--efConstruction` | `256` | HNSW build parameter |
| `--maxConnections` | `16` | HNSW build parameter |
| `--queryDuration` | `0` | Run queries for N seconds instead of one pass |
| `--updatePercentage` | `0` | Percentage of vectors to update after loading |
| `--numTenants` | `0` | Multi-tenancy: number of tenants |

## Automated Benchmark Plans

`plan_runner.py` drives multi-run benchmarks using `plan.yml` (see `plan.yml.example`). It checks out branches of the Weaviate repo, starts/stops Weaviate, and runs the benchmarker for each configuration.

## Python Scripts

`scripts/python/` contains analysis tools:
- `memory_analysis.py` — visualize memory metrics from JSON output
- `collate-results.py` — aggregate results across multiple runs
- `ann.py` — ANN benchmark runner wrapper
- `performance-graphs.py` — generate performance comparison graphs

Install Python dependencies: `pip install -r requirements.txt`
