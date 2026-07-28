package cmd

import (
	"strings"
	"testing"
)

// validBM25Config returns a minimal Config that passes validateBM25, for
// mutation in table tests.
func validBM25Config() Config {
	return Config{
		Mode:          "bm25-benchmark",
		API:           "grpc",
		CorpusFile:    "corpus.jsonl",
		QueriesFile:   "queries.jsonl",
		SearchType:    "bm25",
		TombstoneMode: "update",
		Parallel:      1,
	}
}

func TestParseLimitValues(t *testing.T) {
	got, err := parseLimitValues("10,100,1000,10000")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []int{10, 100, 1000, 10000}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}

	got, err = parseLimitValues(" 10, 100 ")
	if err != nil {
		t.Fatalf("values with spaces must parse, got error: %v", err)
	}
	if len(got) != 2 || got[0] != 10 || got[1] != 100 {
		t.Fatalf("got %v, want [10 100]", got)
	}

	if _, err = parseLimitValues("10,abc"); err == nil {
		t.Fatal("expected error for non-integer value")
	} else if !strings.Contains(err.Error(), "limitArray") {
		t.Errorf("error should name the limitArray flag, got: %v", err)
	}
}

func TestValidateBM25LimitArray(t *testing.T) {
	tests := []struct {
		name                string
		limitArray          string
		tombstonePercentage float64
		wantErr             string
	}{
		{name: "no sweep", limitArray: "", wantErr: ""},
		{name: "valid sweep", limitArray: "10,100,1000,10000", wantErr: ""},
		{name: "tombstones without sweep stay valid", limitArray: "", tombstonePercentage: 0.3, wantErr: ""},
		{name: "invalid csv", limitArray: "10,x", wantErr: "limitArray"},
		{name: "non-positive value", limitArray: "10,0", wantErr: "at least 1"},
		{name: "combined with tombstones", limitArray: "10,100", tombstonePercentage: 0.3, wantErr: "cannot be combined"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validBM25Config()
			cfg.LimitArray = tt.limitArray
			cfg.TombstonePercentage = tt.tombstonePercentage

			err := cfg.validateBM25()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error %q does not contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}
