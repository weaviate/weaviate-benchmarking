package cmd

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	wv1 "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

type QueryWithNeighbors struct {
	Query     []byte
	Neighbors []int
}

func processQueueHttp(queue []QueryWithNeighbors, cfg *Config, c *http.Client, m *sync.Mutex, times *[]time.Duration) {
	for _, query := range queue {
		r := bytes.NewReader(query.Query)
		before := time.Now()
		var url string
		origin := fmt.Sprintf("%s://%s", cfg.HttpScheme, cfg.HttpOrigin)
		if cfg.API == "graphql" {
			url = origin + "/v1/graphql"
		} else if cfg.API == "rest" {
			url = fmt.Sprintf("%s/v1/objects/%s/_search", origin, cfg.ClassName)
		}
		req, err := http.NewRequest("POST", url, r)
		if err != nil {
			fmt.Printf("ERROR: %v\n", err)
			continue
		}

		req.Header.Set("content-type", "application/json")

		if cfg.HttpAuth != "" {
			req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", cfg.HttpAuth))
		}

		res, err := c.Do(req)
		if err != nil {
			fmt.Printf("ERROR: %v\n", err)
			continue
		}
		took := time.Since(before)
		bytes, _ := ioutil.ReadAll(res.Body)
		res.Body.Close()
		var result map[string]interface{}
		if err := json.Unmarshal(bytes, &result); err != nil {
			fmt.Printf("JSON error: %v\n", err)
		}
		if cfg.API == "graphql" {
			if result["data"] != nil && result["errors"] == nil {
				m.Lock()
				*times = append(*times, took)
				m.Unlock()
			} else {
				fmt.Printf("GraphQL Error: %v\n", result)
			}
		} else {
			if list, ok := result["objects"].([]interface{}); ok {
				if len(list) > 0 {
					m.Lock()
					*times = append(*times, took)
					m.Unlock()
				} else {
					fmt.Printf("REST Error: %v\n", result)
				}
			} else {
				fmt.Printf("REST Error: %v\n", result)
			}
		}
	}
}

func processQueueGrpc(queue []QueryWithNeighbors, cfg *Config, grpcConn *grpc.ClientConn, m *sync.Mutex, times *[]time.Duration, recall *[]float64, ndcg *[]float64) {

	grpcClient := wv1.NewWeaviateClient(grpcConn)

	for _, query := range queue {

		searchRequest := &wv1.SearchRequest{}
		err := proto.Unmarshal(query.Query, searchRequest)
		if err != nil {
			log.Fatalf("Failed to unmarshal grpc query: %v", err)
		}

		before := time.Now()

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if cfg.HttpAuth != "" {
			md := metadata.Pairs(
				"Authorization", fmt.Sprintf("Bearer %s", cfg.HttpAuth),
			)
			ctx = metadata.NewOutgoingContext(ctx, md)
		}

		searchReply, err := grpcClient.Search(ctx, searchRequest)
		if err != nil {
			log.Fatalf("Could not search with grpc: %v", err)
		}
		took := time.Since(before)

		if len(searchReply.GetResults()) != cfg.Limit {
			fmt.Printf("Warning grpc got %d results, expected %d\n", len(searchReply.GetResults()), cfg.Limit)
		}

		ids := make([]int, 0, len(searchReply.GetResults()))
		for _, result := range searchReply.GetResults() {
			ids = append(ids, intFromUUID(result.GetMetadata().Id))
		}

		neighborLimit := min(cfg.Limit, len(query.Neighbors))
		recallQuery := float64(len(intersection(ids, query.Neighbors[:neighborLimit]))) / float64(neighborLimit)
		ndcgQuery := computeNDCG(ids, query.Neighbors[:neighborLimit], neighborLimit)
		log.Debugf("Query took %s, recall %f, ndcg %f", took, recallQuery, ndcgQuery)

		m.Lock()
		*times = append(*times, took)
		*recall = append(*recall, recallQuery)
		*ndcg = append(*ndcg, ndcgQuery)
		m.Unlock()
	}
}

func computeDCG(ranking []int, relevance map[int]int, k int) float64 {
	var score float64
	for i := 0; i < k && i < len(ranking); i++ {
		docID := ranking[i]
		rel := float64(relevance[docID])
		score += rel / math.Log2(float64(i+2))
	}
	return score
}

func computeNDCG(trueRanking, predictedRanking []int, k int) float64 {
	// Assign relevance scores based on true ranking
	relevance := make(map[int]int)
	for i, docID := range trueRanking {
		relevance[docID] = len(trueRanking) - i
	}

	ideal := trueRanking
	if len(ideal) > k {
		ideal = ideal[:k]
	}

	idcg := computeDCG(ideal, relevance, k)
	if idcg == 0 {
		return 0.0
	}
	actualDCG := computeDCG(predictedRanking, relevance, k)
	return actualDCG / idcg
}

func benchmark(cfg Config, getQueryFn func(className string) QueryWithNeighbors) Results {
	var times []time.Duration
	var recall []float64
	var ndcg []float64
	m := &sync.Mutex{}

	t := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 120 * time.Second,
		}).DialContext,
		MaxIdleConnsPerHost:   100,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	httpClient := &http.Client{Transport: t}

	httpOption := grpc.WithInsecure()

	if cfg.HttpScheme == "https" {
		creds := credentials.NewTLS(&tls.Config{
			InsecureSkipVerify: true,
		})
		httpOption = grpc.WithTransportCredentials(creds)
	}

	grpcCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	grpcConn, err := grpc.DialContext(grpcCtx, cfg.Origin, httpOption)
	if err != nil {
		log.Fatalf("Did not connect: %v", err)
	}
	defer grpcConn.Close()

	queues := make([][]QueryWithNeighbors, cfg.Parallel)
	rand.Seed(time.Now().UnixNano())

	for i := 0; i < cfg.Queries; i++ {
		query := getQueryFn(cfg.ClassName)
		worker := i % cfg.Parallel
		queues[worker] = append(queues[worker], query)
	}

	wg := &sync.WaitGroup{}
	before := time.Now()
	for _, queue := range queues {
		wg.Add(1)
		go func(queue []QueryWithNeighbors) {
			defer wg.Done()
			if cfg.API == "grpc" {
				processQueueGrpc(queue, &cfg, grpcConn, m, &times, &recall, &ndcg)
			} else {
				processQueueHttp(queue, &cfg, httpClient, m, &times)
			}
		}(queue)
	}
	wg.Wait()

	return analyze(cfg, times, time.Since(before), recall, ndcg)
}

var targetPercentiles = []int{50, 90, 95, 98, 99}

type Results struct {
	Min               time.Duration
	Max               time.Duration
	Mean              time.Duration
	Took              time.Duration
	QueriesPerSecond  float64
	Percentiles       []time.Duration
	PercentilesLabels []int
	Total             int
	Successful        int
	Failed            int
	Parallelization   int
	Recall            float64
	NDCG              float64
}

func analyze(cfg Config, times []time.Duration, total time.Duration, recall []float64, ndcg []float64) Results {
	out := Results{Min: math.MaxInt64, PercentilesLabels: targetPercentiles}
	var sum time.Duration

	for _, time := range times {
		if time < out.Min {
			out.Min = time
		}

		if time > out.Max {
			out.Max = time
		}

		out.Successful++
		sum += time
	}

	var sumRecall float64
	for _, r := range recall {
		sumRecall += r
	}

	var sumNDCG float64
	for _, n := range ndcg {
		sumNDCG += n
	}

	out.Total = cfg.Queries
	out.Failed = cfg.Queries - out.Successful
	out.Parallelization = cfg.Parallel
	out.Mean = sum / time.Duration(len(times))
	out.Took = total
	out.QueriesPerSecond = float64(len(times)) / float64(float64(total)/float64(time.Second))
	out.Recall = sumRecall / float64(len(recall))
	out.NDCG = sumNDCG / float64(len(ndcg))

	sort.Slice(times, func(a, b int) bool {
		return times[a] < times[b]
	})

	percentilePos := func(percentile int) int {
		return int(float64(len(times)*percentile)/100) + 1
	}

	out.Percentiles = make([]time.Duration, len(targetPercentiles))
	for i, percentile := range targetPercentiles {
		pos := percentilePos(percentile)
		if pos >= len(times) {
			pos = len(times) - 1
		}
		out.Percentiles[i] = times[pos]
	}

	return out
}

func intersection(a, b []int) []int {
	setA := make(map[int]bool)
	var result []int

	for _, item := range a {
		setA[item] = true
	}

	for _, item := range b {
		if setA[item] {
			result = append(result, item)
			delete(setA, item) // Ensure unique items in the result
		}
	}

	return result
}

func (r Results) WriteTextTo(w io.Writer) (int64, error) {
	b := strings.Builder{}

	for i, percentile := range targetPercentiles {
		b.WriteString(
			fmt.Sprintf("p%d: %s\n", percentile, r.Percentiles[i]),
		)
	}

	n, err := w.Write([]byte(fmt.Sprintf(
		"Results\nSuccessful: %d\nMin: %s\nMean: %s\n%sTook: %s\nQPS: %f\nRecall: %f\n",
		r.Successful, r.Min, r.Mean, b.String(), r.Took, r.QueriesPerSecond, r.Recall)))
	return int64(n), err
}

type resultsJSON struct {
	Metadata           resultsJSONMetadata   `json:"metadata"`
	Latencies          map[string]int64      `json:"latencies"`
	LatenciesFormatted map[string]string     `json:"latenciesFormatted"`
	Throughput         resultsJSONThroughput `json:"throughput"`
}

type resultsJSONMetadata struct {
	Successful      int    `json:"successful"`
	Failed          int    `json:"failed"`
	Total           int    `json:"total"`
	Parallelization int    `json:"parallelization"`
	Took            int64  `json:"took"`
	TookFormatted   string `json:"tookFormatted"`
}

type resultsJSONThroughput struct {
	QPS float64 `json:"qps"`
}

func (r Results) WriteJSONTo(w io.Writer) (int, error) {
	obj := resultsJSON{
		Metadata: resultsJSONMetadata{
			Successful:      r.Successful,
			Total:           r.Total,
			Failed:          r.Failed,
			Parallelization: r.Parallelization,
			Took:            int64(r.Took),
			TookFormatted:   fmt.Sprint(r.Took),
		},
		Latencies: map[string]int64{
			"mean": int64(r.Mean),
			"min":  int64(r.Min),
		},
		LatenciesFormatted: map[string]string{
			"mean": fmt.Sprint(r.Mean),
			"min":  fmt.Sprint(r.Min),
		},
		Throughput: resultsJSONThroughput{
			QPS: r.QueriesPerSecond,
		},
	}

	for i, percentile := range targetPercentiles {
		obj.Latencies[fmt.Sprintf("p%d", percentile)] = int64(r.Percentiles[i])
		obj.LatenciesFormatted[fmt.Sprintf("p%d", percentile)] = fmt.Sprint(r.Percentiles[i])
	}

	bytes, err := json.MarshalIndent(obj, "", "  ")
	if err != nil {
		return 0, err
	}

	return w.Write(bytes)
}
