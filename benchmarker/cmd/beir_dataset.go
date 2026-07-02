package cmd

import (
	"bufio"
	"encoding/json"
	"os"

	log "github.com/sirupsen/logrus"
)

// beirMaxLineBytes bounds a single JSONL line. BEIR corpus documents (e.g. full
// Wikipedia passages) routinely exceed bufio.Scanner's default 64KB limit, so we
// raise it to avoid "token too long" errors mid-stream.
const beirMaxLineBytes = 16 * 1024 * 1024

// beirCorpusDoc mirrors one line of a BEIR corpus.jsonl file.
type beirCorpusDoc struct {
	ID    string `json:"_id"`
	Title string `json:"title"`
	Text  string `json:"text"`
}

// beirQuery mirrors one line of a BEIR queries.jsonl file.
type beirQuery struct {
	ID   string `json:"_id"`
	Text string `json:"text"`
}

// BeirDataset streams a BEIR-format text corpus (corpus.jsonl) into text batches
// for import and loads the query set (queries.jsonl) for BM25 benchmarking.
//
// It implements the Dataset interface so it can reuse the existing loadTrainData
// import pipeline unchanged. BM25 benchmarking is vectorless, so the
// vector-specific interface methods are no-ops. Qrels are intentionally ignored:
// this is a performance benchmark, not a relevance evaluation.
type BeirDataset struct {
	corpusPath  string
	queriesPath string
	useFilter   bool
	filterCount int
	corpusCount int // cached line count; -1 until computed
}

// NewBeirDataset creates a loader for a BEIR dataset. When useFilter is set each
// document is assigned a "category" bucket (index % filterCount) so BM25 queries
// can be combined with a property filter of controllable selectivity.
func NewBeirDataset(corpusPath, queriesPath string, useFilter bool, filterCount int) *BeirDataset {
	if filterCount <= 0 {
		filterCount = 1
	}
	return &BeirDataset{
		corpusPath:  corpusPath,
		queriesPath: queriesPath,
		useFilter:   useFilter,
		filterCount: filterCount,
		corpusCount: -1,
	}
}

// StreamTrainData reads corpus.jsonl and emits Batches of text objects. It honors
// startOffset (skip the first N documents) and maxRecords (stop after N emitted
// documents, 0 = all) so the same corpus can be re-streamed to reinsert a
// document range during tombstone generation. Each batch's Offset is the absolute
// corpus index of its first document, keeping generated UUIDs / docIds stable
// across passes.
func (d *BeirDataset) StreamTrainData(chunks chan<- Batch, batchSize, startOffset, maxRecords int) {
	f, err := os.Open(d.corpusPath)
	if err != nil {
		log.Fatalf("Error opening corpus file %s: %v", d.corpusPath, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 1024*1024), beirMaxLineBytes)

	batchText := make([]string, 0, batchSize)
	batchTitles := make([]string, 0, batchSize)
	var batchFilters []int
	batchStart := startOffset

	flush := func() {
		if len(batchText) == 0 {
			return
		}
		chunks <- Batch{
			Text:    batchText,
			Titles:  batchTitles,
			Filters: batchFilters,
			Offset:  batchStart,
		}
		batchText = make([]string, 0, batchSize)
		batchTitles = make([]string, 0, batchSize)
		batchFilters = nil
	}

	idx := 0     // absolute position in the corpus file
	emitted := 0 // documents emitted so far (after startOffset)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		if idx < startOffset {
			idx++
			continue
		}
		if maxRecords > 0 && emitted >= maxRecords {
			break
		}

		var doc beirCorpusDoc
		if err := json.Unmarshal(line, &doc); err != nil {
			log.Fatalf("Error parsing corpus line %d: %v", idx, err)
		}
		batchText = append(batchText, doc.Text)
		batchTitles = append(batchTitles, doc.Title)
		if d.useFilter {
			batchFilters = append(batchFilters, idx%d.filterCount)
		}
		idx++
		emitted++

		if len(batchText) >= batchSize {
			flush()
			batchStart = idx
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading corpus file %s: %v", d.corpusPath, err)
	}
	flush()
}

// LoadQueries reads queries.jsonl and returns the query strings (the "text"
// field of each line). Query IDs and qrels are ignored for performance testing.
func (d *BeirDataset) LoadQueries() []string {
	f, err := os.Open(d.queriesPath)
	if err != nil {
		log.Fatalf("Error opening queries file %s: %v", d.queriesPath, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 1024*1024), beirMaxLineBytes)

	var queries []string
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var q beirQuery
		if err := json.Unmarshal(line, &q); err != nil {
			log.Fatalf("Error parsing query line: %v", err)
		}
		if q.Text != "" {
			queries = append(queries, q.Text)
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading queries file %s: %v", d.queriesPath, err)
	}
	return queries
}

// NumTrainVectors returns the number of documents in the corpus (non-empty
// lines). The result is cached; the first call scans the file once. Used to
// convert a tombstone percentage into an absolute document count.
func (d *BeirDataset) NumTrainVectors() int {
	if d.corpusCount >= 0 {
		return d.corpusCount
	}
	f, err := os.Open(d.corpusPath)
	if err != nil {
		log.Fatalf("Error opening corpus file %s: %v", d.corpusPath, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 1024*1024), beirMaxLineBytes)
	count := 0
	for scanner.Scan() {
		if len(scanner.Bytes()) > 0 {
			count++
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading corpus file %s: %v", d.corpusPath, err)
	}
	d.corpusCount = count
	return count
}

// Dataset interface methods that are not meaningful for a vectorless BM25 corpus.
func (d *BeirDataset) TestVectors() [][]float32 { return nil }
func (d *BeirDataset) Neighbors() [][]int       { return nil }
func (d *BeirDataset) TrainFilters() []int      { return nil }
func (d *BeirDataset) TestFilters() []int       { return nil }
func (d *BeirDataset) Dimension() int           { return 0 }
func (d *BeirDataset) Close()                   {}
