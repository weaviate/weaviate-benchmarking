package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate-go-client/v6/cluster"
)

func TestValidateCanonicalizesClassName(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"dbpedia", "Dbpedia"},
		{"Dbpedia", "Dbpedia"},
		{"sift_128", "Sift_128"},
		{"Vector", "Vector"},
		{"", ""},
		{"x", "X"},
		{"tenantns:dbpedia", "tenantns:Dbpedia"},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			cfg := Config{Origin: "localhost:8080", API: "grpc", ClassName: tc.in}
			require.NoError(t, cfg.validateCommon())
			assert.Equal(t, tc.want, cfg.ClassName)
		})
	}
}

func TestIndexingProgress(t *testing.T) {
	shard := func(coll string, queue int64, status string) cluster.Shard {
		return cluster.Shard{Collection: coll, VectorQueueLength: queue, VectorIndexingStatus: status}
	}
	tests := []struct {
		name         string
		nodes        []cluster.Node
		wantQueued   int64
		wantIndexing int
	}{
		{
			name:  "empty queue and ready",
			nodes: []cluster.Node{{Shards: []cluster.Shard{shard("Dbpedia", 0, "READY")}}},
		},
		{
			name: "sums queues across shards and nodes, ignores other collections",
			nodes: []cluster.Node{
				{Shards: []cluster.Shard{shard("Dbpedia", 500, "INDEXING"), shard("Other", 9000, "INDEXING")}},
				{Shards: []cluster.Shard{shard("Dbpedia", 300, "INDEXING")}},
			},
			wantQueued:   800,
			wantIndexing: 2,
		},
		{
			name:         "queue drained but shard still indexing",
			nodes:        []cluster.Node{{Shards: []cluster.Shard{shard("Dbpedia", 0, "INDEXING")}}},
			wantIndexing: 1,
		},
		{
			name:       "small remaining queue is still pending",
			nodes:      []cluster.Node{{Shards: []cluster.Shard{shard("Dbpedia", 999, "READY")}}},
			wantQueued: 999,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			queued, indexing := indexingProgress(tc.nodes, "Dbpedia")
			assert.Equal(t, tc.wantQueued, queued)
			assert.Equal(t, tc.wantIndexing, indexing)
		})
	}
}
