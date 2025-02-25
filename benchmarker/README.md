# Multi-threaded weaviate query time benchmarker

Once installed (see-below), the tools tries to be entirely self-documenting. Every command has a `-h` help option that can tell you where to go from there. For example, start with a root help command running `benchmarker -h` and it will print something like the following output to tell you where to go from there:

```
A Weaviate Benchmarker

Usage:
  benchmarker [flags]
  benchmarker [command]

Available Commands:
  ann-benchmark  Benchmark ANN Benchmark style datasets
  help           Help about any command
  random-vectors Benchmark random vector queries
  raw            Benchmark raw GraphQL queries

Flags:
  -h, --help   help for benchmarker

Use "benchmarker [command] --help" for more information about a command.
```

Once you picked the command you're interested in, you can again use the help command to learn about the flags, for example running `benchmarker ann-benchmark -h` results in the following output:

```
Run a gRPC benchmark on an hdf5 file in the format of ann-benchmarks.com

Usage:
  benchmarker ann-benchmark [flags]

Flags:
  -a, --api string                   The API to use on benchmarks (default "grpc")
  -b, --batchSize int                Batch size for insert operations (default 1000)
      --bq                           Set BQ
      --cache                        Set cache
  -c, --className string             Class name for testing (default "Vector")
      --cleanupIntervalSeconds int   HNSW cleanup interval seconds (default 300) (default 300)
  -d, --distance string              Set distance metric (mandatory)
      --dynamicThreshold int         Threshold to trigger the update in the dynamic index (default 10 000) (default 10000)
      --efArray string               Array of ef parameters as comma separated list (default "16,24,32,48,64,96,128,256,512")
      --efConstruction int           Set Weaviate efConstruction parameter (default 256) (default 256)
      --existingSchema               Leave the schema as-is (default false)
      --filter                       Whether to use filtering for the dataset (default false)
      --filterStrategy               Use a different filter strategy such as "acorn"
      --flatSearchCutoff int         Flat search cut off (default 40 000) (default 40000)
  -f, --format string                Output format, one of [text, json] (default "text")
  -h, --help                         help for ann-benchmark
      --httpOrigin string            The http origin for Weaviate (only used if grpc enabled) (default "localhost:8080")
      --httpScheme string            The http scheme (http or https) (default "http")
      --indexType string             Index type (hnsw or flat) (default "hnsw")
      --labels string                Labels of format key1=value1,key2=value2,...
  -l, --limit int                    Set the query limit / k (default 10) (default 10)
      --maxConnections int           Set Weaviate efConstruction parameter (default 16) (default 16)
      --numTenants int               Number of tenants to use (default 0)
      --offset int                   Offset for uuids (useful to load the same dataset multiple times)
  -u, --origin string                The gRPC origin that Weaviate is running at (default "localhost:50051")
  -o, --output string                Filename for an output file. If none provided, output to stdout only
  -p, --parallel int                 Set the number of parallel threads which send queries (default 8)
      --pq string                    Set PQ (disabled, auto, or enabled) (default disabled) (default "disabled")
      --pqRatio uint                 Set PQ segments = dimensions / ratio (must divide evenly default 4) (default 4)
      --pqSegments uint              Set PQ segments (default 256)
  -q, --query                        Do not import data and only run query tests
      --queryDelaySeconds int        How long to wait before querying (default 30) (default 30)
      --queryDuration int            Instead of querying the test dataset once, query for the specified duration in seconds (default 0)
      --rescoreLimit int             Rescore limit (default 256) for BQ (default 256)
      --shards int                   Set number of Weaviate shards (default 1)
      --skipAsyncReady               Skip async ready (default false)
      --skipTombstonesEmpty          Skip waiting for tombstone to be empty after update (default false)
      --sq string                    Set SQ (disabled, auto, or enabled) (default disabled) (default "disabled")
      --startTenant int              Tenant # to start at if using multiple tenants (default 0)
      --trainingLimit int            Set PQ trainingLimit (default 100000) (default 100000)
      --updateIterations int         Number of iterations to update the dataset if updatePercentage is set (default 1)
      --updatePercentage float       After loading the dataset, update the specified percentage of vectors
      --updateRandomized             Whether to randomize which vectors are updated (default false)
  -v, --vectors string               Path to the hdf5 file containing the vectors


```

### Installation / Running the CLI

#### HDF5 Dependency

The benchmarker requires the hdf5 library for reusing ann-benchmark.com style test datasets
with training vectors, test vectors, and pre-computed neighbors all in the same file.

On Mac you can install via homebrew:

```
brew install hdf5
```

Or on ubuntu:

```
apt install libhdf5-dev
```

#### Option 1: Docker compose

Follow instructions in parent README.md to run in Docker compose.

#### Option 2: With a local Go runtime, compiling on the fly

Ensure you have go and hdf5 installed.

Print the available commands
```
cd benchmarker
go run . help
```

An example command

```
go run . ann-benchmark -v ~/datasets/dbpedia-100k-openai-ada002.hdf5 -d l2-squared

```

or random vectors with long-style flags:

```
go run . \
  random-vectors \
  --className MyClass \
  --dimensions 384 \
  --queries 10000 \
  --parallel 8 \
  --api grpc \
  --limit 10
```

#### Option 3: With a local Go runtime, compile and install just once

Install:

```
cd benchmarker && CGO_ENABLED=1 go install .
```

(Make sure your `PATH` is configured correctly to run go-install-ed binaries)

Run an example command

```
benchmarker random-vectors -c MyClass -d 384 -q 10000 -p 8 -a graphql -l 10
```

or the same command with the long-style flags:

```
benchmarker \
  random-vectors \
  --className MyClass \
  --dimensions 384 \
  --queries 10000 \
  --parallel 8 \
  --api grpc \
  --limit 10
```

### Prometheus Integration

The benchmarker can push metrics to a Prometheus Pushgateway, which allows you to monitor and visualize benchmark results in real-time.

#### Configuration

To enable Prometheus integration, use the following flags:

```
--prometheusEnabled            Enable pushing metrics to Prometheus (default false)
--prometheusPushURL string     URL of the Prometheus pushgateway (e.g., http://localhost:9091)
--prometheusJobName string     Job name for Prometheus metrics (default "weaviate_benchmark")
```

#### Example

```
benchmarker ann-benchmark \
  -v ~/datasets/dbpedia-100k-openai-ada002.hdf5 \
  -d l2-squared \
  --prometheusEnabled \
  --prometheusPushURL http://localhost:9091 \
  --prometheusJobName weaviate_benchmark_dbpedia
```

#### Available Metrics

The following metrics are pushed to Prometheus:

- `weaviate_benchmark_mean_latency_seconds`: Mean latency of benchmark queries in seconds
- `weaviate_benchmark_p99_latency_seconds`: P99 latency of benchmark queries in seconds
- `weaviate_benchmark_queries_per_second`: Queries per second during benchmark
- `weaviate_benchmark_recall`: Recall of benchmark queries
- `weaviate_benchmark_import_time_seconds`: Import time in seconds
- `weaviate_benchmark_heap_alloc_bytes`: Heap allocation in bytes
- `weaviate_benchmark_heap_inuse_bytes`: Heap in use in bytes
- `weaviate_benchmark_heap_sys_bytes`: Heap system in bytes
- `weaviate_benchmark_ef_construction`: EF construction parameter
- `weaviate_benchmark_max_connections`: Max connections parameter
- `weaviate_benchmark_shards`: Number of shards
- `weaviate_benchmark_parallelization`: Parallelization level
- `weaviate_benchmark_limit`: Query limit

#### Labels

All metrics include the following labels:

- `api`: The API used (grpc)
- `ef`: The ef parameter value
- `dataset`: The dataset file name
- `run_id`: A unique ID for the benchmark run
- `timestamp`: The timestamp of the benchmark run

Additionally, any custom labels specified with the `--labels` flag will be included.

#### Setting up Prometheus and Grafana

1. Install and run Prometheus Pushgateway:
   ```
   docker run -d -p 9091:9091 prom/pushgateway
   ```

2. Configure Prometheus to scrape the Pushgateway by adding this to `prometheus.yml`:
   ```yaml
   scrape_configs:
     - job_name: 'pushgateway'
       honor_labels: true
       static_configs:
         - targets: ['localhost:9091']
   ```

3. Install and run Grafana, then create dashboards to visualize the benchmark metrics.

### InfluxDB Integration

The benchmarker can also push metrics to an InfluxDB instance, allowing you to store and analyze benchmark results over time.

#### Configuration

To enable InfluxDB integration, use the following flags:

```
--influxdbEnabled            Enable pushing metrics to InfluxDB (default false)
--influxdbURL string         URL of the InfluxDB instance (e.g., http://localhost:8086)
--influxdbToken string       Token for authenticating with InfluxDB
--influxdbOrg string         Organization name in InfluxDB
--influxdbBucket string      Bucket name in InfluxDB
```

#### Example

```
benchmarker ann-benchmark \
  -v ~/datasets/dbpedia-100k-openai-ada002.hdf5 \
  -d l2-squared \
  --influxdbEnabled \
  --influxdbURL http://localhost:8086 \
  --influxdbToken my-token \
  --influxdbOrg my-org \
  --influxdbBucket my-bucket
```

#### Available Metrics

The same metrics as listed in the Prometheus section are pushed to InfluxDB.

#### Labels

All metrics include the same labels as listed in the Prometheus section.

