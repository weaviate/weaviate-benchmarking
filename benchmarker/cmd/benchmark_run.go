package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

func benchmark(cfg Config, getQueryFn func(className string) []byte) Results {
	var times []time.Duration
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

	c := &http.Client{Transport: t}

	httpAuth, httpAuthPresent := os.LookupEnv("HTTP_AUTH")

	queues := make([][][]byte, cfg.Parallel)
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
		go func(queue [][]byte) {
			defer wg.Done()

			for _, query := range queue {
				r := bytes.NewReader(query)
				before := time.Now()
				var url string
				if cfg.API == "graphql" {
					url = cfg.Origin + "/v1/graphql"
				} else if cfg.API == "rest" {
					url = fmt.Sprintf("%s/v1/objects/%s/_search", cfg.Origin, cfg.ClassName)
				}
				req, err := http.NewRequest("POST", url, r)
				if err != nil {
					fmt.Printf("ERROR: %v\n", err)
					continue
				}

				req.Header.Set("content-type", "application/json")

				if httpAuthPresent {
					req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", httpAuth))
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
						times = append(times, took)
						m.Unlock()
					} else {
						fmt.Printf("GraphQL Error: %v\n", result)
					}
				} else {
					if list, ok := result["objects"].([]interface{}); ok {
						if len(list) > 0 {
							m.Lock()
							times = append(times, took)
							m.Unlock()
						} else {
							fmt.Printf("REST Error: %v\n", result)
						}
					} else {
						fmt.Printf("REST Error: %v\n", result)
					}
				}
			}
		}(queue)
	}

	wg.Wait()

	return analyze(cfg, times, time.Since(before))
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
}

func analyze(cfg Config, times []time.Duration, total time.Duration) Results {
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

	out.Total = cfg.Queries
	out.Failed = cfg.Queries - out.Successful
	out.Parallelization = cfg.Parallel
	out.Mean = sum / time.Duration(len(times))
	out.Took = total
	out.QueriesPerSecond = float64(len(times)) / float64(float64(total)/float64(time.Second))

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

func (r Results) WriteTextTo(w io.Writer) (int64, error) {
	b := strings.Builder{}

	for i, percentile := range targetPercentiles {
		b.WriteString(
			fmt.Sprintf("p%d: %s\n", percentile, r.Percentiles[i]),
		)
	}

	n, err := w.Write([]byte(fmt.Sprintf(
		"Results\nSuccessful: %d\nMin: %s\nMean: %s\n%sTook: %s\nQPS: %f\n",
		r.Successful, r.Min, r.Mean, b.String(), r.Took, r.QueriesPerSecond)))
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
