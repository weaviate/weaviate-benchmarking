package cmd

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	weaviategrpc "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"google.golang.org/protobuf/proto"
)

func annConfigForValidation() Config {
	return Config{
		BenchmarkFile:  "vectors.hdf5",
		API:            "grpc",
		DistanceMetric: "cosine",
		RQ:             "disabled",
		IndexType:      "hnsw",
		EfMode:         "auto",
	}
}

func TestValidateANN_EfModeAndFilterStrategy(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{name: "defaults are valid", mutate: func(*Config) {}},
		{name: "efMode request", mutate: func(c *Config) { c.EfMode = "request" }},
		{name: "efMode schema", mutate: func(c *Config) { c.EfMode = "schema" }},
		{name: "efMode unknown", mutate: func(c *Config) { c.EfMode = "bogus" }, wantErr: "efMode"},
		{name: "efMode request on flat", mutate: func(c *Config) { c.EfMode = "request"; c.IndexType = "flat" }, wantErr: "rescoreLimit"},
		{name: "efMode schema on flat", mutate: func(c *Config) { c.EfMode = "schema"; c.IndexType = "flat" }},
		{name: "filterStrategy sweeping", mutate: func(c *Config) { c.FilterStrategy = "sweeping" }},
		{name: "filterStrategy acorn", mutate: func(c *Config) { c.FilterStrategy = "acorn" }},
		{name: "filterStrategy pathseer", mutate: func(c *Config) { c.FilterStrategy = "pathseer" }},
		{name: "filterStrategy unknown", mutate: func(c *Config) { c.FilterStrategy = "foo" }, wantErr: "filterStrategy"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := annConfigForValidation()
			tt.mutate(&cfg)
			err := cfg.validateANN()
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func decodeSearchRequest(t *testing.T, data []byte) *weaviategrpc.SearchRequest {
	t.Helper()
	var req weaviategrpc.SearchRequest
	require.NoError(t, proto.Unmarshal(data, &req))
	return &req
}

func TestFilterStrategyEnum(t *testing.T) {
	tests := []struct {
		in     string
		want   weaviategrpc.FilterStrategy
		wantOk bool
	}{
		{"sweeping", weaviategrpc.FilterStrategy_FILTER_STRATEGY_SWEEPING, true},
		{"acorn", weaviategrpc.FilterStrategy_FILTER_STRATEGY_ACORN, true},
		{"pathseer", weaviategrpc.FilterStrategy_FILTER_STRATEGY_PATHSEER, true},
		{"", weaviategrpc.FilterStrategy_FILTER_STRATEGY_UNSPECIFIED, false},
		{"bogus", weaviategrpc.FilterStrategy_FILTER_STRATEGY_UNSPECIFIED, false},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, ok := filterStrategyEnum(tt.in)
			require.Equal(t, tt.wantOk, ok)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestNearVectorQueryGrpc_VectorIndexParams(t *testing.T) {
	acorn := weaviategrpc.FilterStrategy_FILTER_STRATEGY_ACORN
	pathseer := weaviategrpc.FilterStrategy_FILTER_STRATEGY_PATHSEER
	sweeping := weaviategrpc.FilterStrategy_FILTER_STRATEGY_SWEEPING
	tests := []struct {
		name           string
		indexType      string
		requestEf      int
		filterStrategy string
		filter         int
		namedVector    string
		multiVectorDim int
		wantNil        bool
		wantEf         *uint32
		wantProbe      *uint32
		wantStrategy   *weaviategrpc.FilterStrategy
	}{
		{name: "hnsw sends ef", indexType: "hnsw", requestEf: 64, filter: -1, wantEf: proto.Uint32(64)},
		{name: "dynamic sends ef", indexType: "dynamic", requestEf: 64, filter: -1, wantEf: proto.Uint32(64)},
		{name: "hfresh sends search probe", indexType: "hfresh", requestEf: 64, filter: -1, wantProbe: proto.Uint32(64)},
		{name: "flat sends nothing", indexType: "flat", requestEf: 64, filter: -1, wantNil: true},
		{name: "ef zero sends nothing", indexType: "hnsw", requestEf: 0, filter: -1, wantNil: true},
		{name: "ef zero with filter strategy sends nothing", indexType: "hnsw", requestEf: 0, filterStrategy: "acorn", filter: 3, wantNil: true},
		{name: "unknown index type sends nothing", indexType: "", requestEf: 64, filter: -1, wantNil: true},
		{name: "filtered hnsw sends acorn", indexType: "hnsw", requestEf: 64, filterStrategy: "acorn", filter: 3, wantEf: proto.Uint32(64), wantStrategy: &acorn},
		{name: "filtered hnsw sends pathseer", indexType: "hnsw", requestEf: 64, filterStrategy: "pathseer", filter: 3, wantEf: proto.Uint32(64), wantStrategy: &pathseer},
		{name: "filtered hnsw sends sweeping", indexType: "hnsw", requestEf: 64, filterStrategy: "sweeping", filter: 3, wantEf: proto.Uint32(64), wantStrategy: &sweeping},
		{name: "filtered dynamic sends strategy", indexType: "dynamic", requestEf: 64, filterStrategy: "acorn", filter: 3, wantEf: proto.Uint32(64), wantStrategy: &acorn},
		{name: "unfiltered query omits strategy", indexType: "hnsw", requestEf: 64, filterStrategy: "acorn", filter: -1, wantEf: proto.Uint32(64)},
		{name: "empty strategy omitted", indexType: "hnsw", requestEf: 64, filterStrategy: "", filter: 3, wantEf: proto.Uint32(64)},
		{name: "hfresh omits strategy", indexType: "hfresh", requestEf: 64, filterStrategy: "acorn", filter: 3, wantProbe: proto.Uint32(64)},
		{name: "named vector carries ef", indexType: "hnsw", requestEf: 64, filter: -1, namedVector: "vec", wantEf: proto.Uint32(64)},
		{name: "multivector carries ef", indexType: "hnsw", requestEf: 64, filter: -1, multiVectorDim: 2, wantEf: proto.Uint32(64)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{
				ClassName:             "Vector",
				Limit:                 10,
				IndexType:             tt.indexType,
				RequestEf:             tt.requestEf,
				FilterStrategy:        tt.filterStrategy,
				NamedVector:           tt.namedVector,
				MultiVectorDimensions: tt.multiVectorDim,
			}
			req := decodeSearchRequest(t, nearVectorQueryGrpc(&cfg, []float32{0.1, 0.2, 0.3, 0.4}, "", tt.filter))

			require.Equal(t, "Vector", req.Collection)
			require.Equal(t, uint32(10), req.Limit)
			require.True(t, req.Metadata.Uuid)
			require.NotNil(t, req.NearVector)
			if tt.filter >= 0 {
				require.Equal(t, []string{"category"}, req.Filters.On)
				require.Equal(t, "3", req.Filters.GetValueText())
			} else {
				require.Nil(t, req.Filters)
			}

			params := req.GetVectorIndexParams()
			if tt.wantNil {
				require.Nil(t, params)
				return
			}
			require.NotNil(t, params)
			require.Equal(t, tt.wantEf, params.Ef)
			require.Equal(t, tt.wantProbe, params.SearchProbe)
			require.Equal(t, tt.wantStrategy, params.FilterStrategy)
			require.Empty(t, req.VectorIndexParamsForTargets)
		})
	}
}

// The filter-catalog path builds an unfiltered query and adds its own filter,
// so it must still carry the filter strategy of a filtered query.
func TestNearVectorQueryGrpcCatalog_VectorIndexParams(t *testing.T) {
	acorn := weaviategrpc.FilterStrategy_FILTER_STRATEGY_ACORN
	tests := []struct {
		name         string
		indexType    string
		requestEf    int
		inverted     bool
		wantNil      bool
		wantEf       *uint32
		wantProbe    *uint32
		wantStrategy *weaviategrpc.FilterStrategy
	}{
		{name: "hnsw sends ef and strategy", indexType: "hnsw", requestEf: 64, wantEf: proto.Uint32(64), wantStrategy: &acorn},
		{name: "inverted filter sends ef and strategy", indexType: "hnsw", requestEf: 64, inverted: true, wantEf: proto.Uint32(64), wantStrategy: &acorn},
		{name: "dynamic sends ef and strategy", indexType: "dynamic", requestEf: 64, wantEf: proto.Uint32(64), wantStrategy: &acorn},
		{name: "hfresh sends search probe only", indexType: "hfresh", requestEf: 64, wantProbe: proto.Uint32(64)},
		{name: "schema mode sends nothing", indexType: "hnsw", requestEf: 0, wantNil: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{
				ClassName:      "Vector",
				Limit:          10,
				IndexType:      tt.indexType,
				RequestEf:      tt.requestEf,
				FilterStrategy: "acorn",
			}
			f := &catalogFilter{Prop: "filter_a", Inverted: tt.inverted}
			req := decodeSearchRequest(t, nearVectorQueryGrpcCatalog(&cfg, []float32{0.1, 0.2, 0.3, 0.4}, "", f))

			require.Equal(t, []string{"filter_a"}, req.Filters.On)
			params := req.GetVectorIndexParams()
			if tt.wantNil {
				require.Nil(t, params)
				return
			}
			require.NotNil(t, params)
			require.Equal(t, tt.wantEf, params.Ef)
			require.Equal(t, tt.wantProbe, params.SearchProbe)
			require.Equal(t, tt.wantStrategy, params.FilterStrategy)
		})
	}
}

func TestServerSupportsVectorIndexParams(t *testing.T) {
	tests := []struct {
		version string
		want    bool
		wantErr bool
	}{
		{version: "1.40.6", want: false},
		{version: "1.41.0", want: false},
		{version: "1.41.9", want: false},
		{version: "1.42.0-rc.1", want: true},
		{version: "1.42.0", want: true},
		{version: "v1.42.0", want: true},
		{version: "1.42.0-rc.1-352208b", want: true},
		{version: "2.0.0", want: true},
		{version: "1.9.9", want: false},
		{version: "1.42", want: true},
		{version: "latest", wantErr: true},
		{version: "", wantErr: true},
		{version: "1.x", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.version, func(t *testing.T) {
			got, err := serverSupportsVectorIndexParams(tt.version)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestResolveEfMode(t *testing.T) {
	tests := []struct {
		name      string
		efMode    string
		indexType string
		version   string
		want      efMode
	}{
		{name: "auto hnsw old server", efMode: "auto", indexType: "hnsw", version: "1.40.6", want: efModeSchema},
		{name: "auto hnsw 1.41 release", efMode: "auto", indexType: "hnsw", version: "1.41.0", want: efModeSchema},
		{name: "auto hnsw new server", efMode: "auto", indexType: "hnsw", version: "1.42.0-rc.1", want: efModeRequest},
		{name: "auto hfresh new server", efMode: "auto", indexType: "hfresh", version: "1.42.0", want: efModeRequest},
		{name: "auto flat new server", efMode: "auto", indexType: "flat", version: "1.42.0", want: efModeSchema},
		{name: "auto dynamic new server", efMode: "auto", indexType: "dynamic", version: "1.42.0", want: efModeSchema},
		{name: "auto unknown version", efMode: "auto", indexType: "hnsw", version: "", want: efModeSchema},
		{name: "auto unparsable version", efMode: "auto", indexType: "hnsw", version: "latest", want: efModeSchema},
		{name: "request forces request on old server", efMode: "request", indexType: "hnsw", version: "1.40.0", want: efModeRequest},
		{name: "request on dynamic", efMode: "request", indexType: "dynamic", version: "1.42.0", want: efModeRequest},
		{name: "request on flat still schema", efMode: "request", indexType: "flat", version: "1.42.0", want: efModeSchema},
		{name: "schema forces schema on new server", efMode: "schema", indexType: "hnsw", version: "1.42.0", want: efModeSchema},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{EfMode: tt.efMode, IndexType: tt.indexType}
			mode, reason := resolveEfMode(&cfg, tt.version)
			require.Equal(t, tt.want, mode)
			require.NotEmpty(t, reason)
		})
	}
}

func TestResultsJSONBenchmark_EfModeKeys(t *testing.T) {
	data, err := json.Marshal(ResultsJSONBenchmark{EfMode: "request", ServerVersion: "1.42.0"})
	require.NoError(t, err)
	require.Contains(t, string(data), `"efMode":"request"`)
	require.Contains(t, string(data), `"serverVersion":"1.42.0"`)
}

func TestSchemaFilterStrategy(t *testing.T) {
	tests := []struct {
		flag string
		want string
	}{
		{flag: "", want: "sweeping"},
		{flag: "sweeping", want: "sweeping"},
		{flag: "acorn", want: "acorn"},
		{flag: "pathseer", want: "pathseer"},
	}
	for _, tt := range tests {
		t.Run("flag="+tt.flag, func(t *testing.T) {
			cfg := Config{FilterStrategy: tt.flag}
			require.Equal(t, tt.want, schemaFilterStrategy(&cfg))
		})
	}
}
