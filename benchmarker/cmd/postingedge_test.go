package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPostingEdgeIndexConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want map[string]interface{}
	}{
		{
			name: "only what was set: Weaviate's defaults for the rest",
			cfg:  Config{DistanceMetric: "cosine", CacheMode: "routing", RescoreLimit: -1, EfConstruction: 256, MaxPostingSizeKB: 48},
			want: map[string]interface{}{
				"distance": "cosine",
				"rq":       map[string]interface{}{"cacheMode": "routing"},
			},
		},
		{
			name: "everything given",
			cfg: Config{
				DistanceMetric: "cosine", CacheMode: "none", RescoreLimit: 150,
				EfConstruction: 80, EfConstructionSet: true, MaxPostingSizeKB: 64, MaxPostingSizeKBSet: true,
			},
			want: map[string]interface{}{
				"distance":         "cosine",
				"efConstruction":   80,
				"maxPostingSizeKB": 64,
				"rq":               map[string]interface{}{"cacheMode": "none", "rescoreLimit": 150},
			},
		},
		{
			name: "a rescore limit of 0 is given, -1 is automatic",
			cfg:  Config{DistanceMetric: "cosine", CacheMode: "routing", RescoreLimit: 0},
			want: map[string]interface{}{
				"distance": "cosine",
				"rq":       map[string]interface{}{"cacheMode": "routing", "rescoreLimit": 0},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, postingEdgeIndexConfig(&tt.cfg))
		})
	}
}

func TestValidatePostingEdge(t *testing.T) {
	valid := func() Config {
		return Config{DistanceMetric: "cosine", PQ: "disabled", SQ: "disabled", RQ: "disabled", CacheMode: "routing"}
	}
	tests := []struct {
		name    string
		change  func(c *Config)
		wantErr string
	}{
		{name: "the defaults", change: func(c *Config) {}},
		{name: "no codes in memory", change: func(c *Config) { c.CacheMode = "none" }},
		{name: "l2-squared", change: func(c *Config) { c.DistanceMetric = "l2-squared" }, wantErr: "only supports distance cosine"},
		{name: "dot", change: func(c *Config) { c.DistanceMetric = "dot" }, wantErr: "only supports distance cosine"},
		{name: "rq", change: func(c *Config) { c.RQ = "auto" }, wantErr: "its own 1-bit RQ codes"},
		{name: "pq", change: func(c *Config) { c.PQ = "auto" }, wantErr: "its own 1-bit RQ codes"},
		{name: "bq", change: func(c *Config) { c.BQ = true }, wantErr: "its own 1-bit RQ codes"},
		{name: "sq", change: func(c *Config) { c.SQ = "auto" }, wantErr: "its own 1-bit RQ codes"},
		{name: "multi-vectors", change: func(c *Config) { c.MultiVectorDimensions = 128 }, wantErr: "multi-vectors"},
		{name: "an unknown cache mode", change: func(c *Config) { c.CacheMode = "scan" }, wantErr: "cacheMode must be routing or none"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := valid()
			tt.change(&c)
			err := c.validatePostingEdge()
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}
