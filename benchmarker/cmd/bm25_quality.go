package cmd

import (
	"context"
	"crypto/tls"
	"fmt"
	"sort"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	weaviategrpc "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/structpb"
)

// bm25Quality holds everything the qrels-based quality pass, judged-doc-biased
// churn, and retrievability probe need. It is built once (buildQuality) when
// --measureQuality is set.
type bm25Quality struct {
	queries       []beirQuery           // all queries (with ids)
	relevant      []map[int]int         // per-query-index relevant map (docIndex -> grade, grade>0); nil if query unscored
	scoredQueries int                   // queries with >=1 relevant doc mapped
	qrelsFile     string                // resolved qrels path
	relevantIdx   []int                 // sorted union of relevant docIndices (churn target for biased churn)
	relevantDocs  map[int]beirCorpusDoc // content for targeted reinsert during biased churn
	depth         int                   // retrieval depth for the quality pass = max(ndcgCutoff, recallCutoff)
}

// buildQuality resolves qrels, maps relevance judgments onto document indices, and
// fails fast when nothing is scorable (an id/split mismatch would otherwise yield a
// silent recall=0 that reads as a passing gate).
func buildQuality(cfg *Config, ds *BeirDataset) *bm25Quality {
	qrelsPath := resolveQrelsPath(cfg.CorpusFile, cfg.QrelsFile)
	if qrelsPath == "" {
		fatal(fmt.Errorf("--measureQuality requires qrels; pass --qrels or place qrels/test.tsv (or dev.tsv) next to the corpus"))
	}
	qrels, err := LoadQrels(qrelsPath)
	if err != nil {
		fatal(fmt.Errorf("loading qrels %s: %w", qrelsPath, err))
	}
	queries := ds.LoadQueriesWithIDs()

	judged := make(map[string]bool)
	for _, docs := range qrels {
		for did, g := range docs {
			if g > 0 {
				judged[did] = true
			}
		}
	}
	idToIndex, byIndex, err := ds.scanJudged(judged)
	if err != nil {
		fatal(fmt.Errorf("scanning corpus for judged docs: %w", err))
	}

	relevant := make([]map[int]int, len(queries))
	relevantIdxSet := make(map[int]bool)
	scored := 0
	for i, q := range queries {
		grades := qrels[q.ID]
		if grades == nil {
			continue
		}
		rm := make(map[int]int)
		for did, g := range grades {
			if g <= 0 {
				continue
			}
			if idx, ok := idToIndex[did]; ok {
				rm[idx] = g
				relevantIdxSet[idx] = true
			}
		}
		if len(rm) > 0 {
			relevant[i] = rm
			scored++
		}
	}
	if scored == 0 {
		fatal(fmt.Errorf("--measureQuality: 0 scored queries against %s (qrels/corpus id mismatch or wrong split); queries=%d", qrelsPath, len(queries)))
	}

	relevantIdx := make([]int, 0, len(relevantIdxSet))
	relevantDocs := make(map[int]beirCorpusDoc, len(relevantIdxSet))
	for idx := range relevantIdxSet {
		relevantIdx = append(relevantIdx, idx)
		if doc, ok := byIndex[idx]; ok {
			relevantDocs[idx] = doc
		}
	}
	sort.Ints(relevantIdx)

	depth := cfg.NDCGCutoff
	if cfg.RecallCutoff > depth {
		depth = cfg.RecallCutoff
	}

	log.WithFields(log.Fields{
		"scored": scored, "total": len(queries), "relevantDocs": len(relevantIdx),
		"qrels": qrelsPath, "ndcgCutoff": cfg.NDCGCutoff, "recallCutoff": cfg.RecallCutoff,
	}).Info("Quality measurement enabled")

	return &bm25Quality{
		queries: queries, relevant: relevant, scoredQueries: scored,
		qrelsFile: qrelsPath, relevantIdx: relevantIdx, relevantDocs: relevantDocs, depth: depth,
	}
}

// measureBM25Quality runs a deterministic pass — each scored query once, retrieval
// depth = q.depth, unfiltered/single-tenant — and returns Results whose Recall =
// Recall@recallCutoff and NDCG = NDCG@ndcgCutoff (graded, vs qrels). Latency here
// is not used for the perf axis.
func measureBM25Quality(cfg Config, q *bm25Quality) Results {
	cfg.Limit = q.depth
	cfg.Filter = false

	type scoredQuery struct {
		text string
		rel  map[int]int
	}
	scored := make([]scoredQuery, 0, q.scoredQueries)
	for i, query := range q.queries {
		if q.relevant[i] != nil {
			scored = append(scored, scoredQuery{query.Text, q.relevant[i]})
		}
	}
	cfg.Queries = len(scored)

	i := 0
	return benchmark(cfg, func(className string) QueryWithNeighbors {
		s := scored[i]
		i++
		return QueryWithNeighbors{
			Query:     bm25QueryGrpc(&cfg, s.text, "", -1),
			Relevance: s.rel,
		}
	})
}

// dialGrpcConn opens a gRPC connection to the configured origin (matches the
// transport setup used by benchmark()/loadTrainData).
func dialGrpcConn(cfg *Config) (*grpc.ClientConn, error) {
	opt := grpc.WithInsecure()
	if cfg.HttpScheme == "https" {
		creds := credentials.NewTLS(&tls.Config{InsecureSkipVerify: true})
		opt = grpc.WithTransportCredentials(creds)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return grpc.DialContext(ctx, cfg.Origin, opt)
}

// reinsertDocs writes a specific, possibly non-contiguous set of documents back at
// their exact docIndex (same UUID + docId), used by judged-doc-biased tombstone
// churn where the target set is scattered across the corpus.
func reinsertDocs(cfg *Config, docs map[int]beirCorpusDoc) {
	if len(docs) == 0 {
		return
	}
	conn, err := dialGrpcConn(cfg)
	if err != nil {
		log.Fatalf("reinsertDocs: grpc dial: %v", err)
	}
	defer conn.Close()
	client := weaviategrpc.NewWeaviateClient(conn)

	const chunk = 500
	objects := make([]*weaviategrpc.BatchObject, 0, chunk)
	flush := func() {
		if len(objects) == 0 {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*300)
		defer cancel()
		if _, err := client.BatchObjects(ctx, &weaviategrpc.BatchObjectsRequest{Objects: objects}); err != nil {
			log.Fatalf("reinsertDocs: batch: %v", err)
		}
		objects = objects[:0]
	}

	for idx, doc := range docs {
		nonRef, err := structpb.NewStruct(map[string]interface{}{
			"text":  doc.Text,
			"title": doc.Title,
			"docId": float64(idx + cfg.Offset),
		})
		if err != nil {
			log.Fatalf("reinsertDocs: struct: %v", err)
		}
		obj := &weaviategrpc.BatchObject{
			Uuid:       uuidFromInt(idx + cfg.Offset),
			Collection: cfg.ClassName,
			Properties: &weaviategrpc.BatchObject_Properties{NonRefProperties: nonRef},
		}
		if cfg.Tenant != "" {
			obj.Tenant = cfg.Tenant
		}
		objects = append(objects, obj)
		if len(objects) >= chunk {
			flush()
		}
	}
	flush()
}

// tombstoneRetrievability probes whether reinserted (relevant) documents are still
// findable via BM25 after churn. For a sample of docs it issues a keyword query
// built from the doc's own text and checks the doc is returned. A just-reinserted
// doc MUST be findable; a fraction < 1.0 signals an inverted-index tombstone
// mishandling. This is a within-run invariant needing no baseline.
func tombstoneRetrievability(cfg *Config, q *bm25Quality, sampleSize int) float64 {
	sample := q.relevantIdx
	if len(sample) == 0 {
		return 1.0
	}
	if len(sample) > sampleSize {
		sample = sample[:sampleSize]
	}

	conn, err := dialGrpcConn(cfg)
	if err != nil {
		log.Warnf("retrievability probe: grpc dial: %v", err)
		return -1
	}
	defer conn.Close()
	client := weaviategrpc.NewWeaviateClient(conn)

	found := 0
	for _, idx := range sample {
		doc := q.relevantDocs[idx]
		queryText := firstTokens(doc.Title+" "+doc.Text, 12)
		if queryText == "" {
			found++ // nothing to probe with; don't penalize
			continue
		}
		// Competition-free probe: a BM25 query over the doc's own terms restricted
		// to docId == idx. If the doc's postings survived the delete+reinsert it is
		// a BM25 candidate and comes back (Limit 1); if the tombstone orphaned its
		// postings it isn't a candidate and nothing returns — no ranking against
		// other docs, so common query terms can't cause a false negative.
		req := &weaviategrpc.SearchRequest{
			Collection: cfg.ClassName,
			Limit:      1,
			Bm25Search: &weaviategrpc.BM25{Query: queryText, Properties: []string{"text", "title"}},
			Filters: &weaviategrpc.Filters{
				On:        []string{"docId"},
				Operator:  weaviategrpc.Filters_OPERATOR_EQUAL,
				TestValue: &weaviategrpc.Filters_ValueInt{ValueInt: int64(idx + cfg.Offset)},
			},
			Metadata: &weaviategrpc.MetadataRequest{Uuid: true},
		}
		if cfg.Tenant != "" {
			req.Tenant = cfg.Tenant
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		reply, err := client.Search(ctx, req)
		cancel()
		if err != nil {
			log.Debugf("retrievability probe query error (soft): %v", err)
			continue
		}
		if len(reply.GetResults()) > 0 {
			found++
		}
	}
	return float64(found) / float64(len(sample))
}

// stableQuality runs the quality pass repeatedly until the metric stops improving,
// then returns the best (settled) Results. The qrels metric is deterministic for a
// settled index; while the inverted index is still building (import/reinsert lag on
// a multi-node cluster) NDCG/Recall climb — often in steps — toward the true value,
// so we wait until they stop climbing rather than until two samples merely agree
// (which a transient mid-indexing plateau would satisfy prematurely). The patience
// counter resets on any climb, so a stepwise settle (e.g. 0.17 → 0.23) is handled.
// A genuine regression (results never recover) still surfaces: the returned value
// stays low.
func stableQuality(cfg Config, q *bm25Quality, timeout time.Duration) Results {
	// patience×interval = 60s of no improvement ⇒ settled; generous enough to ride
	// out the stepwise index-build plateaus seen on a lagging multi-node cluster.
	return settleMetric(func() Results { return measureBM25Quality(cfg, q) }, 12*time.Second, 5, 0.003, timeout)
}

// settleMetric polls `poll` every `interval` until neither NDCG nor Recall has
// improved by more than `epsilon` for `patience` consecutive polls, then returns
// the per-metric maxima seen. Factored out of stableQuality so the settle logic is
// unit-testable with a scripted poll sequence.
func settleMetric(poll func() Results, interval time.Duration, patience int, epsilon float64, timeout time.Duration) Results {
	best := poll()
	stale := 0
	start := time.Now()
	for time.Since(start) < timeout {
		time.Sleep(interval)
		cur := poll()
		// Track each metric's running max independently: the index builds
		// bottom-up, so NDCG@10 typically saturates before Recall@100. Advancing
		// `best` only on an NDCG rise would freeze `best.Recall` at an understated
		// value while a still-climbing Recall looks like perpetual improvement.
		improved := cur.NDCG > best.NDCG+epsilon || cur.Recall > best.Recall+epsilon
		if cur.NDCG > best.NDCG {
			best.NDCG = cur.NDCG
		}
		if cur.Recall > best.Recall {
			best.Recall = cur.Recall
		}
		if improved {
			stale = 0
			log.WithFields(log.Fields{"ndcg": cur.NDCG, "recall": cur.Recall}).Debug("quality still improving; index settling")
			continue
		}
		if stale++; stale >= patience {
			return best
		}
	}
	log.Warnf("quality metric did not settle within %s (ndcg=%.4f) — index still building or a real regression", timeout, best.NDCG)
	return best
}

// waitRetrievable polls the retrievability probe until reinserted docs are
// searchable again (>= threshold) or the timeout elapses, then returns the final
// value. Used after a delete+reinsert to wait out indexing/replication lag before
// measuring quality, so a transient window isn't read as a regression. A genuine
// tombstone bug (docs never recover) surfaces: the value stays low and is logged.
func waitRetrievable(cfg *Config, q *bm25Quality, threshold float64, timeout time.Duration) float64 {
	start := time.Now()
	for {
		r := tombstoneRetrievability(cfg, q, 50)
		if r < 0 || r >= threshold {
			return r
		}
		if time.Since(start) > timeout {
			log.Warnf("retrievability %.2f still below %.2f after %s — possible tombstone regression or very slow indexing", r, threshold, timeout)
			return r
		}
		log.Debugf("waiting for reinserted docs to become searchable: retrievability %.2f", r)
		time.Sleep(3 * time.Second)
	}
}

// firstTokens returns the first n whitespace-separated tokens of s.
func firstTokens(s string, n int) string {
	fields := strings.Fields(s)
	if len(fields) > n {
		fields = fields[:n]
	}
	return strings.Join(fields, " ")
}
