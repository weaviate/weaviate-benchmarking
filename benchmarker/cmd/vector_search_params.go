package cmd

import (
	weaviategrpc "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"google.golang.org/protobuf/proto"
)

// vectorSearchParams builds SearchRequest.vector_search_params for the current
// run. It returns nil when nothing is to be sent: cfg.RequestEf is zero (schema
// mode, or the random-vectors and dataset commands) or the index type has no
// per-request knob (flat). efArray values mean ef for hnsw and dynamic and
// searchProbe for hfresh, matching updateEf. The filter strategy is HNSW-only
// and only meaningful on a filtered query, so it is sent for hnsw and dynamic
// when filter >= 0 and --filterStrategy is set.
func vectorSearchParams(cfg *Config, filter int) *weaviategrpc.VectorSearchParams {
	if cfg.RequestEf <= 0 {
		return nil
	}
	switch cfg.IndexType {
	case "hnsw", "dynamic":
		params := &weaviategrpc.VectorSearchParams{Ef: proto.Uint32(uint32(cfg.RequestEf))}
		if filter >= 0 {
			if strategy, ok := filterStrategyEnum(cfg.FilterStrategy); ok {
				params.FilterStrategy = &strategy
			}
		}
		return params
	case "hfresh":
		return &weaviategrpc.VectorSearchParams{SearchProbe: proto.Uint32(uint32(cfg.RequestEf))}
	default:
		return nil
	}
}

// filterStrategyEnum maps a --filterStrategy value to the proto enum. Empty and
// unknown values return FILTER_STRATEGY_UNSPECIFIED and false.
func filterStrategyEnum(s string) (weaviategrpc.FilterStrategy, bool) {
	switch s {
	case "sweeping":
		return weaviategrpc.FilterStrategy_FILTER_STRATEGY_SWEEPING, true
	case "acorn":
		return weaviategrpc.FilterStrategy_FILTER_STRATEGY_ACORN, true
	case "pathseer":
		return weaviategrpc.FilterStrategy_FILTER_STRATEGY_PATHSEER, true
	default:
		return weaviategrpc.FilterStrategy_FILTER_STRATEGY_UNSPECIFIED, false
	}
}

// schemaFilterStrategy is the value createSchema writes into vectorIndexConfig.
// The benchmarker has always pinned sweeping explicitly; the server default
// moved to acorn in 1.35, so an unset flag keeps writing sweeping to leave
// historical filtered numbers comparable. --filterStrategy overrides it.
func schemaFilterStrategy(cfg *Config) string {
	if cfg.FilterStrategy == "" {
		return "sweeping"
	}
	return cfg.FilterStrategy
}
