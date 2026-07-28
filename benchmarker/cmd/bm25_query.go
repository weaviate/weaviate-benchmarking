package cmd

import (
	"fmt"
	"strconv"
	"strings"

	weaviategrpc "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"google.golang.org/protobuf/proto"
)

// queryPropertiesList parses the comma-separated --queryProperties flag into the
// set of properties a BM25 query scores against. Defaults to ["text"].
func (cfg *Config) queryPropertiesList() []string {
	parts := strings.Split(cfg.QueryProperties, ",")
	props := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			props = append(props, t)
		}
	}
	if len(props) == 0 {
		return []string{"text"}
	}
	return props
}

// bm25QueryGrpc builds a marshalled gRPC SearchRequest for a BM25 keyword query.
// It mirrors nearVectorQueryGrpc:
//   - Metadata requests the object UUID (required — processQueueGrpc decodes each
//     result's UUID back into an int and uuid.Parse("") would panic).
//   - Properties are explicitly not returned (empty PropertiesRequest).
//   - When BM25Operator is "and"/"or", the corresponding SearchOperatorOptions is
//     set (with MinimumOrTokensMatch for the OR case); otherwise it is left nil so
//     the server default applies.
//   - filter >= 0 adds an equality filter on the "category" property, enabling
//     filtered BM25 runs of controllable selectivity.
func bm25QueryGrpc(cfg *Config, query, tenant string, filter int) []byte {
	metadata := &weaviategrpc.MetadataRequest{
		Uuid: true,
	}

	bm25 := &weaviategrpc.BM25{
		Query:      query,
		Properties: cfg.queryPropertiesList(),
	}

	switch cfg.BM25Operator {
	case "and":
		bm25.SearchOperator = &weaviategrpc.SearchOperatorOptions{
			Operator: weaviategrpc.SearchOperatorOptions_OPERATOR_AND,
		}
	case "or":
		op := &weaviategrpc.SearchOperatorOptions{
			Operator: weaviategrpc.SearchOperatorOptions_OPERATOR_OR,
		}
		if cfg.MinOrTokens > 0 {
			minTokens := int32(cfg.MinOrTokens)
			op.MinimumOrTokensMatch = &minTokens
		}
		bm25.SearchOperator = op
	}

	searchRequest := &weaviategrpc.SearchRequest{
		Collection: cfg.ClassName,
		Limit:      uint32(cfg.Limit),
		Bm25Search: bm25,
		Metadata:   metadata,
		// An absent Properties field makes the server return every non-ref
		// property — full documents, which at high limits exceed the gRPC
		// client's 4MB receive cap (~10MB at limit 10000 on fiqa) and would
		// make the timed phase measure payload serialization rather than
		// search. The harness only decodes UUIDs, so request no properties:
		// an explicitly empty PropertiesRequest.
		Properties: &weaviategrpc.PropertiesRequest{},
	}

	if tenant != "" {
		searchRequest.Tenant = tenant
	}

	if filter >= 0 {
		searchRequest.Filters = &weaviategrpc.Filters{
			TestValue: &weaviategrpc.Filters_ValueText{
				ValueText: strconv.Itoa(filter),
			},
			On:       []string{"category"},
			Operator: weaviategrpc.Filters_OPERATOR_EQUAL,
		}
	}

	data, err := proto.Marshal(searchRequest)
	if err != nil {
		fmt.Printf("grpc marshal err: %v\n", err)
	}

	return data
}

// Hybrid (BM25 + vector) seam for a future version. The gRPC message already
// exists as weaviategrpc.Hybrid{Query, Properties, Vector/Vectors, Alpha,
// FusionType, NearVector, Bm25SearchOperator}. To add hybrid benchmarking:
//   1. Implement hybridQueryGrpc setting SearchRequest.HybridSearch (analogous to
//      the Bm25Search wiring above).
//   2. Add a "hybrid" branch to the query selector in benchmarkBM25.
//   3. Give each imported document a vector (Batch.Vectors) and a vector index in
//      createBM25Schema — for perf-only, a deterministic randomVector suffices.
// The BEIR loader, worker-pool harness, and results output are all unchanged.
