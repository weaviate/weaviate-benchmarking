# Weaviate Benchmarking

This repo contains a tool for benchmarking Weaviate performance.

## Documentation for benchmarker

* 📊 results and context can be found in the [Weaviate documentation](https://weaviate.io/developers/weaviate/current/benchmarks/)
* 💬 discuss the results on our [Slack channel](https://join.slack.com/t/weaviate/shared_invite/zt-goaoifjr-o8FuVz9b1HLzhlUfyfddhw) or [Twitter](https://twitter.com/weaviate_io)

### ANN benchmark

There are two components you will need to run for the benchmarks:

1. `weaviate` the standard Weaviate image
2. `benchmarker` a go based benchmarking tool

You can run both as containers on the same machine via Docker compose.

For replicating our benchmarks we recommend setting up two separate machines.

| Machine description | CPU type | CPUs | Memory | Disk size | Disk type | Misc. |
| --- | --- | --- | --- | --- | --- | --- |
| Machine to run Weaviate | c2 | 30 | 120GB | 500GB | SSD | [Ubuntu 22.04 with Docker-compose](https://gist.github.com/bobvanluijt/04f6d97916244a7de59fead84ef63cd4) |
| Machine to run benchmark script | N2 | 8 | 64GB | 500GB | SSD | [Ubuntu 22.04 with Docker-compose](https://gist.github.com/bobvanluijt/04f6d97916244a7de59fead84ef63cd4) |

#### Prepare the Weaviate machine

Clone this repo and cd into it `$ git clone https://github.com/semi-technologies/weaviate-benchmarking && cd weaviate-benchmarking`

Run the following command to spin up Weaviate: `$ docker-compose up weaviate -d`

Copy the IP address and amount of CPU cores this machine has.

#### Prepare the benchmark machine

Check if the Weaviate machine is available: `curl http://{IP OF WEAVIATE INSTANCE}/v1/meta`. Note that the instance runs on port `8080`, e.g., `http://10.128.15.12:8080/v1/meta`.
You will also need to allow port `50051` for gRPC and `21121` for metrics. You can verify this via `nc -zv localhost 50051`.

Clone this repo and cd into it `$ git clone https://github.com/semi-technologies/weaviate-benchmarking && cd weaviate-benchmarking`

Download the files into a datasets folder as outlined below.

```sh
mkdir datasets && \
    curl -o ./datasets/dbpedia-100k-openai-ada002-angular.hdf5 https://storage.googleapis.com/ann-datasets/custom/dbpedia-100k-openai-ada002-angular.hdf5 \
    curl -o ./datasets/deep-image-96-angular.hdf5 https://ann-benchmarks.com/deep-image-96-angular.hdf5 && \
    curl -o ./datasets/mnist-784-euclidean.hdf5 https://ann-benchmarks.com/mnist-784-euclidean.hdf5 && \
    curl -o ./datasets/gist-960-euclidean.hdf5 https://ann-benchmarks.com/gist-960-euclidean.hdf5
```

Run a single performance test on an [ann-benchmarks](https://ann-benchmarks.com/) hdf5 dataset.

```sh
DATASET=./datasets/dbpedia-100k-openai-ada002-angular.hdf5 DISTANCE=cosine docker compose run benchmarker
```

For more details on additional configuration options see the help options.

```sh
docker compose run benchmarker /app/benchmarker ann-benchmark -h
```