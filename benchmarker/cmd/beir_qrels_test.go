package cmd

import (
	"path/filepath"
	"testing"
)

func TestLoadQrels(t *testing.T) {
	dir := t.TempDir()
	path := writeBeirFixture(t, dir, "test.tsv", []string{
		"query-id\tcorpus-id\tscore", // header, must be skipped
		"q0\tMED-1\t2",
		"q0\tMED-3\t1",
		"q1\tMED-2\t1",
		"q1\tMED-9\t0", // explicit non-relevant
	})

	qrels, err := LoadQrels(path)
	if err != nil {
		t.Fatalf("LoadQrels: %v", err)
	}
	if len(qrels) != 2 {
		t.Fatalf("got %d queries, want 2", len(qrels))
	}
	if qrels["q0"]["MED-1"] != 2 || qrels["q0"]["MED-3"] != 1 {
		t.Errorf("q0 grades = %v", qrels["q0"])
	}
	if qrels["q1"]["MED-2"] != 1 || qrels["q1"]["MED-9"] != 0 {
		t.Errorf("q1 grades = %v", qrels["q1"])
	}
}

func TestLoadQrelsBadColumns(t *testing.T) {
	dir := t.TempDir()
	path := writeBeirFixture(t, dir, "bad.tsv", []string{"q0\tMED-1"}) // only 2 cols, no header
	if _, err := LoadQrels(path); err == nil {
		t.Error("expected error for 2-column qrels line")
	}
}

func TestResolveQrelsPath(t *testing.T) {
	dir := t.TempDir()
	corpus := filepath.Join(dir, "corpus.jsonl")

	// explicit flag wins
	if got := resolveQrelsPath(corpus, "/explicit/qrels.tsv"); got != "/explicit/qrels.tsv" {
		t.Errorf("explicit = %q", got)
	}
	// none present
	if got := resolveQrelsPath(corpus, ""); got != "" {
		t.Errorf("expected empty, got %q", got)
	}
	// dev.tsv fallback
	dev := writeBeirFixture(t, filepath.Join(dir, "qrels"), "dev.tsv", []string{"q0\tD-1\t1"})
	if got := resolveQrelsPath(corpus, ""); got != dev {
		t.Errorf("dev fallback = %q, want %q", got, dev)
	}
	// test.tsv preferred over dev.tsv
	test := writeBeirFixture(t, filepath.Join(dir, "qrels"), "test.tsv", []string{"q0\tD-1\t1"})
	if got := resolveQrelsPath(corpus, ""); got != test {
		t.Errorf("test preferred = %q, want %q", got, test)
	}
}

func TestScanJudged(t *testing.T) {
	dir := t.TempDir()
	corpus := writeBeirFixture(t, dir, "corpus.jsonl", []string{
		`{"_id":"MED-0","title":"T0","text":"alpha"}`,
		`{"_id":"MED-1","title":"T1","text":"beta"}`,
		`{"_id":"MED-2","title":"T2","text":"gamma"}`,
		`{"_id":"MED-3","title":"T3","text":"delta"}`,
	})
	ds := NewBeirDataset(corpus, "", false, 1)

	judged := map[string]bool{"MED-1": true, "MED-3": true}
	idToIndex, byIndex, err := ds.scanJudged(judged)
	if err != nil {
		t.Fatalf("scanJudged: %v", err)
	}
	// docIndex must match the import ordinal (uuidFromInt uses the same value).
	if idToIndex["MED-1"] != 1 || idToIndex["MED-3"] != 3 {
		t.Errorf("idToIndex = %v, want MED-1:1 MED-3:3", idToIndex)
	}
	if len(idToIndex) != 2 {
		t.Errorf("idToIndex holds %d entries, want only judged (2)", len(idToIndex))
	}
	if byIndex[1].Text != "beta" || byIndex[3].Text != "delta" {
		t.Errorf("byIndex content wrong: %v", byIndex)
	}
}
