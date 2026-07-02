# Multi-threaded weaviate query time benchmarker

Once installed (see-below), the tools tries to be entirely self-documenting. Every command has a `-h` help option that can tell you where to go from there. For example, start with a root help command running `benchmarker -h` and it will print something like the following output to tell you where to go from there:

```
A Weaviate Benchmarker

Usage:
  benchmarker [flags]
  benchmarker [command]

Available Commands:
  ann-benchmark  Benchmark ANN Benchmark style datasets
  bm25-benchmark Benchmark BM25 keyword search on a BEIR-format text corpus
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
      --indexType string             Index type (hnsw, flat, hfresh) (default "hnsw")
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
      --rescoreLimit int             Rescore limit. If not set, Weaviate will configure it automatically when rescoring is enabled
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

## BM25 benchmark

The `bm25-benchmark` command imports a [BEIR](https://github.com/beir-cellar/beir)-format text corpus (`corpus.jsonl` + `queries.jsonl`) into a vectorless collection and benchmarks BM25 keyword-search latency/QPS. Unlike ANN, BM25 top-k is exact, so this is a pure performance test (no recall/ground-truth). It can also generate inverted-index tombstones (via deletes/updates) to measure BM25 performance under tombstone load. Results are written to `./results/<runID>.json`, one row per phase, in the same shape as `ann-benchmark`.

Running `benchmarker bm25-benchmark -h` results in the following output:

```
Import a BEIR-format text corpus (corpus.jsonl) into a vectorless Weaviate
collection and benchmark BM25 keyword-search latency/QPS using queries.jsonl.

Optionally generates inverted-index tombstones (via deletes/updates) to measure
BM25 keyword-search performance under tombstone load.

Usage:
  benchmarker bm25-benchmark [flags]

Flags:
  -a, --api string                     API to use (only grpc is supported) (default "grpc")
  -b, --batchSize int                  Batch size for import (default 1000)
      --bm25Operator string            BM25 search operator: or, and, or empty for server default
      --bm25b float                    BM25 b parameter (default 0.75)
      --bm25k1 float                   BM25 k1 parameter (default 1.2)
  -c, --className string               Class name for the benchmark collection (default "Bm25Bench")
      --corpus string                  Path to the BEIR corpus.jsonl file (required unless --query)
      --existingSchema                 Leave the schema as-is (do not recreate the class)
      --filter                         Combine BM25 with an equality filter on a category bucket
      --filterCount int                Number of category buckets (filter selectivity = 1/filterCount) (default 10)
  -f, --format string                  Output format, one of [text, json] (default "text")
  -u, --grpcOrigin string              The gRPC origin that Weaviate is running at (default "localhost:50051")
  -h, --help                           help for bm25-benchmark
      --httpOrigin string              The HTTP origin for Weaviate (default "localhost:8080")
      --httpScheme string              The HTTP scheme (http or https) (default "http")
      --labels string                  Labels of format key1=value1,key2=value2 merged into each result row
  -l, --limit int                      Query limit (top_k) (default 10)
      --measureBaseline                Run a 0-tombstone baseline before the tombstone phase (default true)
      --memoryMonitoringEnabled        Enable continuous memory monitoring
      --memoryMonitoringFile string    Memory monitoring output file
      --memoryMonitoringInterval int   Memory monitoring interval in seconds (default 5)
      --metricsEndpoint string         Weaviate metrics endpoint (default "http://localhost:2112/metrics")
      --minimumOrTokensMatch int       minimumOrTokensMatch for the OR operator (0 = unset)
      --numTenants int                 Number of tenants; each gets a full corpus copy (0 = single-tenant)
  -o, --output string                  Optional output file for the results JSON
  -p, --parallel int                   Number of parallel query threads (default: number of CPUs)
      --queries int                    Number of query executions per phase (0 = one pass over the query set)
      --queriesFile string             Path to the BEIR queries.jsonl file (required)
      --query                          Do not import; query an existing collection
      --queryDelaySeconds int          How long to wait after import before querying (default 30)
      --queryDuration int              Query for the specified duration in seconds instead of a fixed count
      --queryProperties string         Comma-separated properties to run BM25 against (default "text")
      --replicationFactor int          Replication factor (default 1)
      --searchType string              Search type (bm25; hybrid reserved for a future version) (default "bm25")
      --shards int                     Number of shards (default 1)
      --skipQuery                      Only import data, skip the query phase
      --tokenization string            Tokenization for the text properties (word, lowercase, whitespace, field, trigram) (default "word")
      --tombstoneConcurrent            Churn tombstones in the background during the query run
      --tombstoneIterations int        Number of tombstone-generation iterations (re-measured each time) (default 1)
      --tombstoneMode string           How to create tombstones: update (delete+reinsert) or delete (default "update")
      --tombstonePercentage float      Fraction of docs (0..1) to tombstone per iteration (0 disables)
```

Download a BEIR dataset (e.g. the small `nfcorpus`) and run a pure BM25 benchmark:

```
curl -L -o nfcorpus.zip https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/nfcorpus.zip
unzip nfcorpus.zip

go run . bm25-benchmark \
  --corpus nfcorpus/corpus.jsonl \
  --queriesFile nfcorpus/queries.jsonl \
  -c Bm25Bench -l 10 -p 8 --bm25Operator or
```

To measure BM25 latency under inverted-index tombstone load, run a 0-tombstone baseline followed by one or more delete/reinsert iterations (record the Weaviate version via `--labels`, since tombstone handling varies across versions):

```
go run . bm25-benchmark \
  --corpus nfcorpus/corpus.jsonl --queriesFile nfcorpus/queries.jsonl \
  -c Bm25Bench -l 10 -p 8 \
  --tombstonePercentage 0.5 --tombstoneMode update --tombstoneIterations 2 \
  --labels "weaviateVersion=<commit>"
```

## Memory Monitoring Feature 🆕

The benchmarker now includes comprehensive memory monitoring capabilities that track memory usage throughout benchmark execution.

### Quick Start

Enable memory monitoring with any benchmark:

```bash
./weaviate-benchmarker ann-benchmark \
  --memoryMonitoringEnabled \
  --memoryMonitoringInterval 5 \
  --vectors dataset.hdf5 \
  --distance cosine
```

### Memory Monitoring Flags

- `--memoryMonitoringEnabled`: Enable continuous memory monitoring (default: false)
- `--memoryMonitoringInterval`: Memory monitoring interval in seconds (default: 5)
- `--memoryMonitoringFile`: Custom output filename (default: auto-generated)

### Analysis

Analyze the generated memory metrics:

```bash
# Analysis with simplified plots (memory over time + distribution)
python scripts/python/memory_analysis.py results/memory_metrics_*.json

# Statistics only
python scripts/python/memory_analysis.py results/memory_metrics_*.json --no-plot
```

### Use Cases

- **Performance Analysis**: Monitor memory during different benchmark phases
- **Memory Leak Detection**: Track memory growth over time
- **Resource Planning**: Understand memory requirements for different configurations
- **Optimization**: Compare memory usage between different settings

For detailed documentation, see [MEMORY_MONITORING.md](MEMORY_MONITORING.md).

### Running Tests

```
cd benchmarker
go test ./...
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

