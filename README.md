# Weaviate Benchmarking

This repo contains a tool for benchmarking Weaviate performance.

## Documentation for benchmarker

* 📊 results and context can be found in the [Weaviate documentation](https://weaviate.io/developers/weaviate/current/benchmarks/)
* 💬 discuss the results on our [Slack channel](https://join.slack.com/t/weaviate/shared_invite/zt-goaoifjr-o8FuVz9b1HLzhlUfyfddhw) or [Twitter](https://twitter.com/weaviate_io)

## ANN benchmark

There are two components you will need to run for the benchmarks:

1. `weaviate` the standard Weaviate image
2. `benchmarker` a go based benchmarking tool

You can run both as containers on the same machine via Docker compose.

For replicating our benchmarks we recommend setting the following machine:

| Machine name | CPU type | CPUs | Memory | Disk size | Disk type | Misc. |
| --- | --- | --- | --- | --- | --- | --- |
| `n4-highmem-16` | N4 | 16 | 128GB | 512GB | Hyperdisk Balanced | Debian 12 (bookworm) with [Docker and Compose V2](https://gist.github.com/StefanBogdan/821d18bbc5f18978643adff508749cf0) |

### Run tests

Clone this repo and cd into it `$ git clone https://github.com/weaviate/weaviate-benchmarking && cd weaviate-benchmarking`

Download the files into a datasets folder as outlined below.

```sh
mkdir datasets && \
    curl -o ./datasets/dbpedia-openai-1000k-angular.hdf5 https://storage.googleapis.com/ann-datasets/ann-benchmarks/dbpedia-openai-1000k-angular.hdf5 && \
    curl -o ./datasets/snowflake-msmarco-arctic-embed-m-v1.5-angular.hdf5 https://storage.googleapis.com/ann-datasets/custom/snowflake-msmarco-arctic-embed-m-v1.5-angular.hdf5 && \
    curl -o ./datasets/sift-128-euclidean.hdf5 http://ann-benchmarks.com/sift-128-euclidean.hdf5 && \
    curl -o ./datasets/sphere-10M-meta-dpr.hdf5 https://storage.googleapis.com/ann-datasets/custom/sphere-10M-meta-dpr.hdf5
```

Run a single performance test on an [ann-benchmarks](https://ann-benchmarks.com/) hdf5 dataset.

```sh
DATASET=./datasets/dbpedia-openai-1000k-angular.hdf5 DISTANCE=cosine docker compose up --abort-on-container-exit
```

For more details on additional configuration options see the help options.

```sh
docker compose run benchmarker /app/benchmarker ann-benchmark -h
```

## BM25 benchmark

The `bm25-benchmark` command measures **BM25 keyword-search** performance (latency
percentiles, QPS, throughput under concurrency) on a [BEIR](https://github.com/beir-cellar/beir)-format
text corpus. Unlike ANN, BM25 top-k is computed exactly, so by default this is a
pure performance test — add `--measureQuality` to also compute NDCG/Recall against
the dataset's qrels as a regression gate (see below).

### Get a dataset

BEIR datasets ship as a zip containing `corpus.jsonl` and `queries.jsonl`
(`qrels/` is only read with `--measureQuality`; otherwise ignored).

```sh
# small (smoke): nfcorpus (~3.6K docs), scifact (~5K); large (scale): msmarco (~8.8M)
curl -L -o nfcorpus.zip https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/nfcorpus.zip
unzip nfcorpus.zip
```

### Run

```sh
go run . bm25-benchmark \
    --corpus nfcorpus/corpus.jsonl \
    --queriesFile nfcorpus/queries.jsonl \
    -c Bm25Bench -l 10 -p 8 --bm25Operator or
```

Results are written to `./results/<runID>.json` (one row per phase), in the same
shape as `ann-benchmark` so they plug into the existing reporting. See all options
with `go run . bm25-benchmark -h`.

### Tombstone scenario

BM25 latency can be affected by **inverted-index tombstones** — deleted docIDs that
still occupy posting slots (from updates/deletes) and must be skipped at query time
until compaction reclaims them. This scenario measures that effect: it runs a
0-tombstone baseline, then deletes/re-inserts a fraction of documents and
re-measures, so you can quantify latency/QPS as tombstone density grows.

```sh
go run . bm25-benchmark \
    --corpus nfcorpus/corpus.jsonl --queriesFile nfcorpus/queries.jsonl \
    -c Bm25Bench -l 10 -p 8 \
    --tombstonePercentage 0.5 --tombstoneMode update --tombstoneIterations 3 \
    --labels "weaviateVersion=<commit>,scenario=tombstone"
```

- `--tombstoneMode update` deletes then reinserts the same documents (one tombstone
  per doc per iteration, matching an update-heavy workload); `delete` only deletes.
- `--tombstoneConcurrent` churns tombstones in the background *during* the query run.

### Quality measurement (regression gate)

`--measureQuality` computes **NDCG@10** and **Recall@100 vs the BEIR qrels** for each
phase (filling the `recall`/`ndcg` fields), so a change that degrades what BM25
returns — a tombstone/WAND regression or a new Weaviate version — shows up as a drop
in those metrics. The reference (qrels) ships with the dataset, so nothing is stored
between runs; comparison happens downstream exactly like ANN recall.

```sh
go run . bm25-benchmark \
  --corpus benchmark-data/scifact/corpus.jsonl \
  --queriesFile benchmark-data/scifact/queries.jsonl \
  --measureQuality --queryProperties text,title \
  --tombstonePercentage 0.5 --tombstoneMode update
```

- The qrels file auto-detects at `<corpusdir>/qrels/test.tsv` (then `dev.tsv`); override with `--qrels`.
- Cutoffs are decoupled from `--limit`: tune with `--ndcgCutoff` / `--recallCutoff`.
- In quality mode the update-churn targets the **judged** docs (the rows'
  `tombstoneRatio` then reflects the judged fraction actually churned, not
  `--tombstonePercentage`) and a **retrievability probe** (`tombstoneRetrievability`)
  checks reinserted docs stay findable — the direct tombstone-correctness signal.
- Rows carry a `benchmarkType: bm25-qrels` label. **When comparing runs downstream,
  use negative thresholds** (e.g. `ndcg: -0.02`) so *decreases* are flagged, and never
  mix BM25 and ANN result files (their `recall`/`ndcg` are on different scales).
- Note: absolute scores sit below published Anserini BEIR numbers (Weaviate's default
  tokenization does no stemming/stopword removal). Use `--queryProperties text,title`
  and treat the value as an internally-calibrated band.


