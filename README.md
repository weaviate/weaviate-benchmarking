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