//go:build integration

package cmd

import (
	"context"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	weaviategrpc "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"google.golang.org/grpc"
)

const (
	integrationClassName   = "BenchmarkIntTest"
	integrationDimensions  = 16
	integrationVectorCount = 300
)

func integrationCfg() Config {
	return Config{
		Origin:                 "localhost:50051",
		HttpOrigin:             "localhost:8080",
		HttpScheme:             "http",
		API:                    "grpc",
		ClassName:              integrationClassName,
		Dimensions:             integrationDimensions,
		Parallel:               4,
		Queries:                50,
		Limit:                  10,
		DistanceMetric:         "cosine",
		EfConstruction:         64,
		MaxConnections:         16,
		BatchSize:              100,
		IndexType:              "hnsw",
		Mode:                   "ann-benchmark",
		OutputFormat:           "text",
		RescoreLimit:           -1,
		CleanupIntervalSeconds: 300,
		FlatSearchCutoff:       40000,
	}
}

// skipIfWeaviateUnavailable skips the test if Weaviate is not reachable.
// Start a local instance with:
//
//	docker run -p 8080:8080 -p 50051:50051 semitechnologies/weaviate:latest
func skipIfWeaviateUnavailable(t *testing.T, origin string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, origin, grpc.WithInsecure(), grpc.WithBlock()) //nolint:staticcheck
	if err != nil {
		t.Skipf("Weaviate not available at %s: %v", origin, err)
	}
	conn.Close()
}

// generateVectors creates n deterministic random float32 vectors of the given dimension.
func generateVectors(n, dims int, seed int64) [][]float32 {
	rng := rand.New(rand.NewSource(seed))
	vecs := make([][]float32, n)
	for i := range vecs {
		v := make([]float32, dims)
		for j := range v {
			v[j] = rng.Float32()*2 - 1
		}
		vecs[i] = v
	}
	return vecs
}

// setupTestCollection creates a fresh collection, inserts the given vectors via
// gRPC, and registers a t.Cleanup to delete the collection after the test.
func setupTestCollection(t *testing.T, cfg *Config, vectors [][]float32) {
	t.Helper()
	client := createClient(cfg)

	// Delete first (ignore error — collection may not exist yet).
	_ = client.Schema().ClassDeleter().WithClassName(cfg.ClassName).Do(context.Background())

	classObj := &models.Class{
		Class:           cfg.ClassName,
		VectorIndexType: "hnsw",
		VectorIndexConfig: map[string]interface{}{
			"distance":               cfg.DistanceMetric,
			"efConstruction":         float64(cfg.EfConstruction),
			"maxConnections":         float64(cfg.MaxConnections),
			"cleanupIntervalSeconds": cfg.CleanupIntervalSeconds,
			"flatSearchCutoff":       cfg.FlatSearchCutoff,
		},
	}
	err := client.Schema().ClassCreator().WithClass(classObj).Do(context.Background())
	require.NoError(t, err, "create collection %q", cfg.ClassName)

	t.Cleanup(func() {
		_ = client.Schema().ClassDeleter().WithClassName(cfg.ClassName).Do(context.Background())
	})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, cfg.Origin, grpc.WithInsecure()) //nolint:staticcheck
	require.NoError(t, err)
	defer conn.Close()
	grpcClient := weaviategrpc.NewWeaviateClient(conn)

	writeChunk(&Batch{Vectors: vectors, Offset: 0}, &grpcClient, cfg)

	// Give HNSW time to finish indexing before querying.
	time.Sleep(3 * time.Second)
}

// TestIntegration_QueriesSucceed runs a full insert→query cycle with random
// query vectors and asserts that no queries fail and throughput is positive.
func TestIntegration_QueriesSucceed(t *testing.T) {
	cfg := integrationCfg()
	skipIfWeaviateUnavailable(t, cfg.Origin)

	vectors := generateVectors(integrationVectorCount, integrationDimensions, 42)
	setupTestCollection(t, &cfg, vectors)

	results := benchmark(cfg, func(_ string) QueryWithNeighbors {
		return QueryWithNeighbors{
			Query: nearVectorQueryGrpc(&cfg, randomVector(cfg.Dimensions), "", -1),
		}
	})

	assert.Equal(t, 0, results.Failed, "expected zero failed queries")
	assert.Equal(t, cfg.Queries, results.Successful, "all queries should succeed")
	assert.Greater(t, results.QueriesPerSecond, 0.0, "QPS should be positive")
	assert.Greater(t, int64(results.Mean), int64(0), "mean latency should be positive")
	assert.GreaterOrEqual(t, results.Max, results.Mean, "max latency must be >= mean")
	assert.LessOrEqual(t, results.Min, results.Mean, "min latency must be <= mean")
}

// TestIntegration_RecallForExactNeighbors inserts a known set of vectors and
// then queries with exact copies of those vectors. Because each query vector is
// already in the index its distance to itself is 0, so it must be the top result.
// Recall should be ≥ 0.9 even at low ef values.
func TestIntegration_RecallForExactNeighbors(t *testing.T) {
	cfg := integrationCfg()
	cfg.Queries = 50
	cfg.Limit = 1 // top-1: the exact match must appear
	skipIfWeaviateUnavailable(t, cfg.Origin)

	vectors := generateVectors(integrationVectorCount, integrationDimensions, 99)
	setupTestCollection(t, &cfg, vectors)

	// getQueryFn is called sequentially inside benchmark() before workers start,
	// so queryIdx is not accessed concurrently.
	queryIdx := 0
	results := benchmark(cfg, func(_ string) QueryWithNeighbors {
		i := queryIdx % len(vectors)
		queryIdx++
		return QueryWithNeighbors{
			Query:     nearVectorQueryGrpc(&cfg, vectors[i], "", -1),
			Neighbors: []int{i}, // ground truth: the vector itself is its nearest neighbour
		}
	})

	assert.Equal(t, 0, results.Failed, "expected zero failed queries")
	assert.Greater(t, results.Recall, 0.9, "recall must be >90%% for exact vector matches")
}

// TestIntegration_ResultsJSON checks that the JSON output of a real benchmark
// round-trip is well-formed and contains the expected top-level keys.
func TestIntegration_ResultsJSON(t *testing.T) {
	cfg := integrationCfg()
	cfg.Queries = 20
	cfg.OutputFormat = "json"
	skipIfWeaviateUnavailable(t, cfg.Origin)

	vectors := generateVectors(integrationVectorCount, integrationDimensions, 7)
	setupTestCollection(t, &cfg, vectors)

	results := benchmark(cfg, func(_ string) QueryWithNeighbors {
		return QueryWithNeighbors{
			Query: nearVectorQueryGrpc(&cfg, randomVector(cfg.Dimensions), "", -1),
		}
	})

	// Verify the JSON serialiser doesn't error and produces non-empty output.
	var buf strings.Builder
	n, err := results.WriteJSONTo(&buf)
	require.NoError(t, err)
	assert.Greater(t, n, 0, "JSON output should not be empty")

	// Basic sanity: the output should contain throughput and metadata keys.
	out := buf.String()
	assert.Contains(t, out, `"qps"`)
	assert.Contains(t, out, `"successful"`)
	assert.Contains(t, out, `"recall"`)
}
