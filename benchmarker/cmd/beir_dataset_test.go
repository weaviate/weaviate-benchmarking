package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeBeirFixture(t *testing.T, dir, name string, lines []string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatalf("write fixture %s: %v", name, err)
	}
	return path
}

func drainBatches(ds *BeirDataset, batchSize, startOffset, maxRecords int) (offsets []int, texts, titles []string, filters []int) {
	chunks := make(chan Batch, 16)
	go func() {
		ds.StreamTrainData(chunks, batchSize, startOffset, maxRecords)
		close(chunks)
	}()
	for b := range chunks {
		offsets = append(offsets, b.Offset)
		texts = append(texts, b.Text...)
		titles = append(titles, b.Titles...)
		filters = append(filters, b.Filters...)
	}
	return offsets, texts, titles, filters
}

func TestBeirDatasetStreamAndQueries(t *testing.T) {
	dir := t.TempDir()
	corpus := writeBeirFixture(t, dir, "corpus.jsonl", []string{
		`{"_id":"d0","title":"T0","text":"alpha beta"}`,
		`{"_id":"d1","title":"T1","text":"gamma"}`,
		`{"_id":"d2","title":"T2","text":"delta epsilon"}`,
		`{"_id":"d3","title":"T3","text":"zeta"}`,
		`{"_id":"d4","title":"T4","text":"eta"}`,
	})
	queries := writeBeirFixture(t, dir, "queries.jsonl", []string{
		`{"_id":"q0","text":"alpha"}`,
		`{"_id":"q1","text":"delta"}`,
	})

	ds := NewBeirDataset(corpus, queries, true, 2)

	if got := ds.NumTrainVectors(); got != 5 {
		t.Errorf("NumTrainVectors = %d, want 5", got)
	}

	offsets, texts, titles, filters := drainBatches(ds, 2, 0, 0)

	wantTexts := []string{"alpha beta", "gamma", "delta epsilon", "zeta", "eta"}
	if len(texts) != len(wantTexts) {
		t.Fatalf("got %d texts, want %d", len(texts), len(wantTexts))
	}
	for i, w := range wantTexts {
		if texts[i] != w {
			t.Errorf("text[%d] = %q, want %q", i, texts[i], w)
		}
	}
	wantTitles := []string{"T0", "T1", "T2", "T3", "T4"}
	for i, w := range wantTitles {
		if titles[i] != w {
			t.Errorf("title[%d] = %q, want %q", i, titles[i], w)
		}
	}

	// Filter buckets = absolute index % filterCount(2).
	wantFilters := []int{0, 1, 0, 1, 0}
	if len(filters) != len(wantFilters) {
		t.Fatalf("got %d filters, want %d", len(filters), len(wantFilters))
	}
	for i, w := range wantFilters {
		if filters[i] != w {
			t.Errorf("filter[%d] = %d, want %d", i, filters[i], w)
		}
	}

	// Batch offsets are the absolute corpus index of each batch's first doc.
	wantOffsets := []int{0, 2, 4}
	if len(offsets) != len(wantOffsets) {
		t.Fatalf("got %d batches, want %d", len(offsets), len(wantOffsets))
	}
	for i, w := range wantOffsets {
		if offsets[i] != w {
			t.Errorf("offset[%d] = %d, want %d", i, offsets[i], w)
		}
	}

	// Range re-stream (used by tombstone "update" reinsert): only the first 2 docs.
	rangeOffsets, rangeTexts, _, _ := drainBatches(ds, 2, 0, 2)
	if len(rangeTexts) != 2 {
		t.Errorf("range restream got %d docs, want 2", len(rangeTexts))
	}
	if len(rangeOffsets) != 1 || rangeOffsets[0] != 0 {
		t.Errorf("range restream offsets = %v, want [0]", rangeOffsets)
	}

	qs := ds.LoadQueries()
	if len(qs) != 2 || qs[0] != "alpha" || qs[1] != "delta" {
		t.Errorf("LoadQueries = %v, want [alpha delta]", qs)
	}
}
