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

func benchmark(getQueryFn func(className string) []byte) {
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

	queues := make([][][]byte, parallel)
	rand.Seed(time.Now().UnixNano())

	for i := 0; i < queries; i++ {
		query := getQueryFn(className)

		worker := i % parallel
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
				if api == "graphql" {
					url = "http://localhost:8080/v1/graphql"
				} else if api == "rest" {
					url = fmt.Sprintf("http://localhost:8080/v1/objects/%s/_search", className)
				} else {
					fmt.Printf("unknown api\n")
					os.Exit(1)
				}
				req, err := http.NewRequest("POST", url, r)
				if err != nil {
					fmt.Printf("ERROR: %v\n", err)
					continue
				}

				req.Header.Set("content-type", "application/json")

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
				if api == "graphql" {
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

	results := analyze(times, time.Since(before))
	results.WriteTo(os.Stdout)
}

var targetPercentiles = []int{50, 90, 95, 98, 99}

type results struct {
	min              time.Duration
	max              time.Duration
	successful       int
	mean             time.Duration
	took             time.Duration
	queriesPerSecond float64
	percentiles      []time.Duration
}

func analyze(times []time.Duration, total time.Duration) results {
	out := results{min: math.MaxInt64}
	var sum time.Duration

	for _, time := range times {
		if time < out.min {
			out.min = time
		}

		if time > out.max {
			out.max = time
		}

		out.successful++
		sum += time
	}

	out.mean = sum / time.Duration(len(times))
	out.took = total
	out.queriesPerSecond = float64(len(times)) / float64(float64(total)/float64(time.Second))

	sort.Slice(times, func(a, b int) bool {
		return times[a] < times[b]
	})

	percentilePos := func(percentile int) int {
		return int(float64(len(times)*percentile)/100) + 1
	}

	out.percentiles = make([]time.Duration, len(targetPercentiles))
	for i, percentile := range targetPercentiles {
		out.percentiles[i] = times[percentilePos(percentile)]
	}

	return out
}

func (r results) WriteTo(w io.Writer) (int64, error) {
	b := strings.Builder{}

	for i, percentile := range targetPercentiles {
		b.WriteString(
			fmt.Sprintf("p%d: %s\n", percentile, r.percentiles[i]),
		)
	}

	n, err := w.Write([]byte(fmt.Sprintf(
		"Results\nSuccessful: %d\nMin: %s\nMean: %s\n%sTook: %s\nQPS: %f\n",
		r.successful, r.min, r.mean, b.String(), r.took, r.queriesPerSecond)))
	return int64(n), err
}
