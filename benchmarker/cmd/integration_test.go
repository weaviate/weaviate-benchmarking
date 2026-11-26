//go:build integration

package cmd

import (
	"context"
	"math"
	"math/rand"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	weaviategrpc "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
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

// skipIfServerBelow142 skips tests that need vector_index_params on servers
// that silently drop the field.
func skipIfServerBelow142(t *testing.T, cfg *Config) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	version, err := fetchServerVersion(ctx, createClient(cfg))
	require.NoError(t, err)
	supported, err := serverSupportsVectorIndexParams(version)
	require.NoError(t, err)
	if !supported {
		t.Skipf("server %s is below 1.42 and ignores vector_index_params", version)
	}
	return version
}

// searchIDs sends one pre-marshalled query and returns the integer ids of the
// hits (the collection is written with uuidFromInt). Unlike benchmark() it
// surfaces the gRPC error instead of exiting the process.
func searchIDs(t *testing.T, client weaviategrpc.WeaviateClient, query []byte) ([]int, error) {
	t.Helper()
	var req weaviategrpc.SearchRequest
	require.NoError(t, proto.Unmarshal(query, &req))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	reply, err := client.Search(ctx, &req)
	if err != nil {
		return nil, err
	}
	ids := make([]int, 0, len(reply.Results))
	for _, r := range reply.Results {
		raw := strings.ReplaceAll(r.Metadata.Id, "-", "")
		id, err := strconv.ParseInt(raw[len(raw)-12:], 16, 64)
		require.NoError(t, err)
		ids = append(ids, int(id))
	}
	return ids, nil
}

func newGrpcSearchClient(t *testing.T, origin string) weaviategrpc.WeaviateClient {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, origin, grpc.WithInsecure(), grpc.WithBlock()) //nolint:staticcheck
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return weaviategrpc.NewWeaviateClient(conn)
}

// bruteForceTopK returns the ids of the k nearest vectors by cosine distance.
func bruteForceTopK(vectors [][]float32, query []float32, k int) []int {
	type scored struct {
		id   int
		dist float64
	}
	norm := func(v []float32) float64 {
		var s float64
		for _, x := range v {
			s += float64(x) * float64(x)
		}
		return math.Sqrt(s)
	}
	qn := norm(query)
	all := make([]scored, len(vectors))
	for i, v := range vectors {
		var dot float64
		for j := range v {
			dot += float64(v[j]) * float64(query[j])
		}
		all[i] = scored{id: i, dist: 1 - dot/(norm(v)*qn)}
	}
	sort.Slice(all, func(a, b int) bool { return all[a].dist < all[b].dist })
	ids := make([]int, k)
	for i := range ids {
		ids[i] = all[i].id
	}
	return ids
}

func recallOf(got, truth []int) float64 {
	set := make(map[int]struct{}, len(truth))
	for _, id := range truth {
		set[id] = struct{}{}
	}
	hits := 0
	for _, id := range got {
		if _, ok := set[id]; ok {
			hits++
		}
	}
	return float64(hits) / float64(len(truth))
}

// TestIntegration_VectorIndexParams_RejectedAboveCap is the deterministic proof
// that the server parses vector_index_params: an ef above HNSW_MAX_SEARCH_EF is
// rejected with InvalidArgument naming the cap. A pre-1.42 server would accept it.
func TestIntegration_VectorIndexParams_RejectedAboveCap(t *testing.T) {
	cfg := integrationCfg()
	skipIfWeaviateUnavailable(t, cfg.Origin)
	skipIfServerBelow142(t, &cfg)

	vectors := generateVectors(integrationVectorCount, integrationDimensions, 11)
	setupTestCollection(t, &cfg, vectors)
	client := newGrpcSearchClient(t, cfg.Origin)

	cfg.RequestEf = 1_000_000
	_, err := searchIDs(t, client, nearVectorQueryGrpc(&cfg, vectors[0], "", -1))
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.Contains(t, err.Error(), "HNSW_MAX_SEARCH_EF")

	cfg.RequestEf = 64
	ids, err := searchIDs(t, client, nearVectorQueryGrpc(&cfg, vectors[0], "", -1))
	require.NoError(t, err)
	assert.Equal(t, 0, ids[0], "the query vector itself must be the top hit")
}

// TestIntegration_VectorIndexParams_EfChangesResults sweeps ef per request on
// an unchanged collection: the floor (ef = limit) and a generous value must
// produce different result sets for at least one query, and the generous value
// must not lose recall.
func TestIntegration_VectorIndexParams_EfChangesResults(t *testing.T) {
	cfg := integrationCfg()
	cfg.Dimensions = 32
	skipIfWeaviateUnavailable(t, cfg.Origin)
	skipIfServerBelow142(t, &cfg)

	const n = 3000
	vectors := generateVectors(n, cfg.Dimensions, 2024)
	setupTestCollection(t, &cfg, vectors)
	client := newGrpcSearchClient(t, cfg.Origin)

	queries := generateVectors(50, cfg.Dimensions, 77)
	var differ int
	var recallLow, recallHigh float64
	for _, q := range queries {
		truth := bruteForceTopK(vectors, q, cfg.Limit)

		cfg.RequestEf = cfg.Limit
		low, err := searchIDs(t, client, nearVectorQueryGrpc(&cfg, q, "", -1))
		require.NoError(t, err)

		cfg.RequestEf = 512
		high, err := searchIDs(t, client, nearVectorQueryGrpc(&cfg, q, "", -1))
		require.NoError(t, err)

		if !slices.Equal(low, high) {
			differ++
		}
		recallLow += recallOf(low, truth)
		recallHigh += recallOf(high, truth)
	}
	recallLow /= float64(len(queries))
	recallHigh /= float64(len(queries))
	t.Logf("ef=%d recall %.3f, ef=512 recall %.3f, %d/%d result sets differ", cfg.Limit, recallLow, recallHigh, differ, len(queries))

	assert.Greater(t, differ, 0, "per-request ef had no effect on any query")
	assert.GreaterOrEqual(t, recallHigh, recallLow)
	assert.Greater(t, recallHigh, 0.95)
}

// TestIntegration_EfModeAuto checks the mode decision against the live server.
func TestIntegration_EfModeAuto(t *testing.T) {
	cfg := integrationCfg()
	cfg.EfMode = "auto"
	skipIfWeaviateUnavailable(t, cfg.Origin)
	version := skipIfServerBelow142(t, &cfg)

	mode, reason := resolveEfMode(&cfg, version)
	assert.Equal(t, efModeRequest, mode, reason)

	cfg.IndexType = "flat"
	mode, _ = resolveEfMode(&cfg, version)
	assert.Equal(t, efModeSchema, mode)
}
