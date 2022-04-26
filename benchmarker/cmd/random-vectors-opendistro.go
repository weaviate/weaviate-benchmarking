package cmd

// func benchmarkOpendistroVector() {
// 	benchmarkOpendistro(func(className string) []byte {
// 		return opendistroQueryJSON(className, randomVector())
// 	})
// }

// func opendistroQueryJSON(className string, vec []float32) []byte {
// 	vecJSON, _ := json.Marshal(vec)
// 	return []byte(fmt.Sprintf(`{
//       "query": {
//         "knn": {
//           "vector": {
//             "vector": %s,
//             "k": 10
//           }
//         }
//       }
// }`, string(vecJSON)))
// }

// func benchmarkOpendistro(getQueryFn func(className string) []byte) {
// 	var times []time.Duration
// 	m := &sync.Mutex{}

// 	queues := make([][][]byte, parallel)
// 	rand.Seed(time.Now().UnixNano())

// 	for i := 0; i < queries; i++ {
// 		query := getQueryFn(className)

// 		worker := i % parallel
// 		queues[worker] = append(queues[worker], query)
// 	}

// 	wg := &sync.WaitGroup{}
// 	before := time.Now()
// 	for _, queue := range queues {
// 		wg.Add(1)
// 		go func(queue [][]byte) {
// 			defer wg.Done()

// 			for _, query := range queue {
// 				r := bytes.NewReader(query)
// 				req, err := http.NewRequest("POST", fmt.Sprintf("https://localhost:9200/%s/_search", className), r)
// 				if err != nil {
// 					fmt.Printf("ERROR: %v\n", err)
// 				}
// 				req.Header.Add("Content-Type", "application/json")
// 				req.Header.Add("Authorization", "Basic YWRtaW46YWRtaW4=")

// 				client := &http.Client{
// 					Transport: http.DefaultTransport,
// 				}
// 				client.Transport.(*http.Transport).TLSClientConfig = &tls.Config{
// 					InsecureSkipVerify: true,
// 				}

// 				before := time.Now()
// 				res, err := client.Do(req)
// 				if err != nil {
// 					fmt.Printf("ERROR: %v\n", err)
// 				}
// 				took := time.Since(before)
// 				defer res.Body.Close()
// 				bytes, _ := ioutil.ReadAll(res.Body)
// 				var result map[string]interface{}
// 				json.Unmarshal(bytes, &result)
// 				resLen := len(result["hits"].(map[string]interface{})["hits"].([]interface{}))
// 				if resLen == 10 {
// 					m.Lock()
// 					times = append(times, took)
// 					m.Unlock()
// 				} else {
// 					fmt.Printf("Error: %v\n", result)
// 				}
// 			}
// 		}(queue)
// 	}

// 	wg.Wait()

// 	results := analyze(times, time.Since(before))
// 	results.WriteTo(os.Stdout)
// }
