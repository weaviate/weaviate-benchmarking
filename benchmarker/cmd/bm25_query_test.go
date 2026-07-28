package cmd

import (
	"testing"

	weaviategrpc "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"google.golang.org/protobuf/proto"
)

func unmarshalSearchRequest(t *testing.T, data []byte) *weaviategrpc.SearchRequest {
	t.Helper()
	var sr weaviategrpc.SearchRequest
	if err := proto.Unmarshal(data, &sr); err != nil {
		t.Fatalf("unmarshal SearchRequest: %v", err)
	}
	return &sr
}

func TestBM25QueryGrpc_OrOperatorAndProperties(t *testing.T) {
	cfg := &Config{
		ClassName:       "Bm25Bench",
		Limit:           10,
		QueryProperties: "text,title",
		BM25Operator:    "or",
		MinOrTokens:     2,
	}

	sr := unmarshalSearchRequest(t, bm25QueryGrpc(cfg, "alpha beta", "", -1))

	if sr.Collection != "Bm25Bench" {
		t.Errorf("collection = %q, want Bm25Bench", sr.Collection)
	}
	if sr.Limit != 10 {
		t.Errorf("limit = %d, want 10", sr.Limit)
	}
	if sr.Metadata == nil || !sr.Metadata.Uuid {
		t.Error("metadata.uuid must be true (else result-id decode panics)")
	}
	if sr.Bm25Search == nil {
		t.Fatal("bm25Search is nil")
	}
	if sr.Bm25Search.Query != "alpha beta" {
		t.Errorf("query = %q, want 'alpha beta'", sr.Bm25Search.Query)
	}
	if got := sr.Bm25Search.Properties; len(got) != 2 || got[0] != "text" || got[1] != "title" {
		t.Errorf("properties = %v, want [text title]", got)
	}
	op := sr.Bm25Search.SearchOperator
	if op == nil || op.Operator != weaviategrpc.SearchOperatorOptions_OPERATOR_OR {
		t.Fatalf("search operator = %v, want OR", op)
	}
	if op.MinimumOrTokensMatch == nil || *op.MinimumOrTokensMatch != 2 {
		t.Errorf("minimumOrTokensMatch = %v, want 2", op.MinimumOrTokensMatch)
	}
	if sr.Filters != nil {
		t.Errorf("filters must be nil when filter < 0, got %v", sr.Filters)
	}
}

func TestBM25QueryGrpc_FilterAndTenant(t *testing.T) {
	cfg := &Config{ClassName: "C", Limit: 5, QueryProperties: "text"}
	sr := unmarshalSearchRequest(t, bm25QueryGrpc(cfg, "gamma", "tenantA", 3))

	if sr.Tenant != "tenantA" {
		t.Errorf("tenant = %q, want tenantA", sr.Tenant)
	}
	if sr.Filters == nil {
		t.Fatal("filters nil, want category==3")
	}
	if sr.Filters.Operator != weaviategrpc.Filters_OPERATOR_EQUAL {
		t.Errorf("filter operator = %v, want EQUAL", sr.Filters.Operator)
	}
	if len(sr.Filters.On) != 1 || sr.Filters.On[0] != "category" {
		t.Errorf("filter on = %v, want [category]", sr.Filters.On)
	}
	vt, ok := sr.Filters.TestValue.(*weaviategrpc.Filters_ValueText)
	if !ok || vt.ValueText != "3" {
		t.Errorf("filter value = %v, want ValueText 3", sr.Filters.TestValue)
	}
}

func TestBM25QueryGrpc_Defaults(t *testing.T) {
	// Empty QueryProperties defaults to ["text"]; AND operator; no min-tokens.
	cfg := &Config{ClassName: "C", Limit: 5, BM25Operator: "and"}
	sr := unmarshalSearchRequest(t, bm25QueryGrpc(cfg, "q", "", -1))
	if got := sr.Bm25Search.Properties; len(got) != 1 || got[0] != "text" {
		t.Errorf("default properties = %v, want [text]", got)
	}
	if sr.Bm25Search.SearchOperator == nil ||
		sr.Bm25Search.SearchOperator.Operator != weaviategrpc.SearchOperatorOptions_OPERATOR_AND {
		t.Error("expected AND operator")
	}

	// No operator specified → SearchOperator left nil (server default applies).
	cfgNoOp := &Config{ClassName: "C", Limit: 5}
	srNoOp := unmarshalSearchRequest(t, bm25QueryGrpc(cfgNoOp, "q", "", -1))
	if srNoOp.Bm25Search.SearchOperator != nil {
		t.Errorf("expected nil SearchOperator when BM25Operator unset, got %v", srNoOp.Bm25Search.SearchOperator)
	}

	// Return properties must be explicitly suppressed: an absent Properties
	// field makes the server return full documents, which at high limits
	// exceed the gRPC client's receive cap.
	if sr.Properties == nil {
		t.Fatal("Properties must be set (empty) to suppress returned properties")
	}
	if sr.Properties.ReturnAllNonrefProperties || len(sr.Properties.NonRefProperties) > 0 {
		t.Errorf("expected empty PropertiesRequest, got %v", sr.Properties)
	}
}
