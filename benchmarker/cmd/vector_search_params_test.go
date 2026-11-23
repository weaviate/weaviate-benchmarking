package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"
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
