package cmd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

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
// for import and loads the query set (queries.jsonl) for BM25 benchmarking. When
// quality measurement is enabled it also loads qrels and maps relevance judgments
// onto document indices.
//
// It implements the Dataset interface so it can reuse the existing loadTrainData
// import pipeline unchanged. BM25 benchmarking is vectorless, so the
// vector-specific interface methods are no-ops.
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

// scanCorpus iterates the non-empty lines of a JSONL corpus file, calling fn with
// the document's ordinal index (0-based among non-empty lines) and the raw line.
// The ordinal is exactly what uuidFromInt encodes at import, so every consumer
// (StreamTrainData, NumTrainVectors, scanJudged) shares one definition of
// "docIndex" and cannot drift on empty-line handling. fn returns false to stop.
func scanCorpus(path string, fn func(idx int, line []byte) bool) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 1024*1024), beirMaxLineBytes)

	idx := 0
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		if !fn(idx, line) {
			return scanner.Err()
		}
		idx++
	}
	return scanner.Err()
}

// StreamTrainData reads corpus.jsonl and emits Batches of text objects. It honors
// startOffset (skip the first N documents) and maxRecords (stop after N emitted
// documents, 0 = all) so the same corpus can be re-streamed to reinsert a
// document range during tombstone generation. Each batch's Offset is the absolute
// corpus index of its first document, keeping generated UUIDs / docIds stable
// across passes.
func (d *BeirDataset) StreamTrainData(chunks chan<- Batch, batchSize, startOffset, maxRecords int) {
	batchText := make([]string, 0, batchSize)
	batchTitles := make([]string, 0, batchSize)
	var batchFilters []int
	batchStart := startOffset
	emitted := 0

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

	err := scanCorpus(d.corpusPath, func(idx int, line []byte) bool {
		if idx < startOffset {
			return true
		}
		if maxRecords > 0 && emitted >= maxRecords {
			return false
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
		emitted++
		if len(batchText) >= batchSize {
			flush()
			batchStart = idx + 1
		}
		return true
	})
	if err != nil {
		log.Fatalf("Error reading corpus file %s: %v", d.corpusPath, err)
	}
	flush()
}

// LoadQueriesWithIDs reads queries.jsonl and returns (id, text) pairs. Queries
// with empty text are skipped.
func (d *BeirDataset) LoadQueriesWithIDs() []beirQuery {
	f, err := os.Open(d.queriesPath)
	if err != nil {
		log.Fatalf("Error opening queries file %s: %v", d.queriesPath, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 1024*1024), beirMaxLineBytes)

	var queries []beirQuery
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
			queries = append(queries, q)
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading queries file %s: %v", d.queriesPath, err)
	}
	return queries
}

// LoadQueries returns just the query strings (throughput path).
func (d *BeirDataset) LoadQueries() []string {
	qs := d.LoadQueriesWithIDs()
	out := make([]string, len(qs))
	for i, q := range qs {
		out[i] = q.Text
	}
	return out
}

// NumTrainVectors returns the number of documents in the corpus (non-empty
// lines). The result is cached; the first call scans the file once.
func (d *BeirDataset) NumTrainVectors() int {
	if d.corpusCount >= 0 {
		return d.corpusCount
	}
	count := 0
	err := scanCorpus(d.corpusPath, func(idx int, line []byte) bool {
		count++
		return true
	})
	if err != nil {
		log.Fatalf("Error reading corpus file %s: %v", d.corpusPath, err)
	}
	d.corpusCount = count
	return count
}

// LoadQrels parses a BEIR qrels TSV (columns: query-id, corpus-id, score) into
// queryID -> docID -> grade. A header row is tolerated (skipped when the score
// column of the first line does not parse as a number). IDs are kept as strings.
func LoadQrels(path string) (map[string]map[string]int, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 1024*1024), beirMaxLineBytes)

	qrels := make(map[string]map[string]int)
	lineNum := 0
	firstContent := true // header detection keys on the first NON-blank line
	for scanner.Scan() {
		lineNum++
		line := strings.TrimRight(scanner.Text(), "\r\n")
		if strings.TrimSpace(line) == "" {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) != 3 {
			return nil, fmt.Errorf("qrels line %d: expected 3 tab-separated columns (BEIR format), got %d", lineNum, len(fields))
		}
		qid, did, scoreStr := fields[0], fields[1], strings.TrimSpace(fields[2])
		score, perr := strconv.ParseFloat(scoreStr, 64)
		if perr != nil {
			if firstContent {
				firstContent = false
				continue // header row
			}
			return nil, fmt.Errorf("qrels line %d: bad score %q: %v", lineNum, scoreStr, perr)
		}
		firstContent = false
		if qrels[qid] == nil {
			qrels[qid] = make(map[string]int)
		}
		qrels[qid][did] = int(score)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return qrels, nil
}

// scanJudged scans the corpus once and returns, for every doc-id present in
// `judged`, its docIndex (matching import) and its content (for targeted
// reinsertion during judged-doc-biased tombstone churn). Duplicate _ids are
// warned about (they undercount recall).
func (d *BeirDataset) scanJudged(judged map[string]bool) (idToIndex map[string]int, byIndex map[int]beirCorpusDoc, err error) {
	idToIndex = make(map[string]int, len(judged))
	byIndex = make(map[int]beirCorpusDoc, len(judged))
	scanErr := scanCorpus(d.corpusPath, func(idx int, line []byte) bool {
		var doc beirCorpusDoc
		if uerr := json.Unmarshal(line, &doc); uerr != nil {
			log.Fatalf("Error parsing corpus line %d: %v", idx, uerr)
		}
		if judged[doc.ID] {
			if prev, dup := idToIndex[doc.ID]; dup {
				log.Warnf("Duplicate corpus _id %q at indices %d and %d; recall may undercount", doc.ID, prev, idx)
			}
			idToIndex[doc.ID] = idx
			byIndex[idx] = doc
		}
		return true
	})
	return idToIndex, byIndex, scanErr
}

// resolveQrelsPath returns the explicit --qrels path if set, otherwise
// auto-detects <dir(corpus)>/qrels/test.tsv then .../dev.tsv. Returns "" if none.
func resolveQrelsPath(corpusPath, qrelsFlag string) string {
	if qrelsFlag != "" {
		return qrelsFlag
	}
	dir := filepath.Dir(corpusPath)
	for _, name := range []string{"test.tsv", "dev.tsv"} {
		p := filepath.Join(dir, "qrels", name)
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return ""
}

// Dataset interface methods that are not meaningful for a vectorless BM25 corpus.
func (d *BeirDataset) TestVectors() [][]float32 { return nil }
func (d *BeirDataset) Neighbors() [][]int       { return nil }
func (d *BeirDataset) TrainFilters() []int      { return nil }
func (d *BeirDataset) TestFilters() []int       { return nil }
func (d *BeirDataset) Dimension() int           { return 0 }
func (d *BeirDataset) Close()                   {}
