# Weaviate Benchmarking

This repo contains both a library for benchmarking Weaviate e2e as well as a
CLI tool that makes use of the same library

## Documentation for benchmarker

📊 results and context can be found in the [Weaviate documentation](https://weaviate.io).

### ANN benchmark

Spin up two machines:

| Machine description | CPU type | CPUs | Memory | Disk size | Disk type | Misc. |
| --- | --- | --- | --- | --- | --- | --- |
| Machine to run Weaviate | N2 | 32 | 256GB | 500GB | SSD | [Ubuntu 22.04 with Docker-compose](https://gist.github.com/bobvanluijt/04f6d97916244a7de59fead84ef63cd4) |
| Machine to run benchmark script | N2 | 16 | 64GB | 500GB | SSD | [Ubuntu 22.04 with Docker-compose](https://gist.github.com/bobvanluijt/04f6d97916244a7de59fead84ef63cd4) |

#### Prepare the Weaviate machine

Clone this repo and cd into it `$ git clone https://github.com/semi-technologies/weaviate-benchmarking && cd weaviate-benchmarking`

Run the following command to spin up Weaviate: `$ docker-compose up weaviate -d`

Copy the interal IP address and amount of CPU cores this machine has.

#### Prepare the benchmark machine

Check if the Weaviate machine is available: `$ http://{IP OF WEAVIATE INSTANCE}/v1/meta`. Note that the instance runs on port `8080`, e.g., `http://10.128.15.12:8080/v1/meta`

Clone this repo and cd into it `$ git clone https://github.com/semi-technologies/weaviate-benchmarking && cd weaviate-benchmarking`

Download the files into a benchmark-data folder as outlined below.

```sh
$ mkdir benchmark-data && \
    curl -o ./benchmark-data/deep-image-96-angular.hdf5 http://ann-benchmarks.com/deep-image-96-angular.hdf5 && \
    curl -o ./benchmark-data/glove-200-angular.hdf5 http://ann-benchmarks.com/glove-200-angular.hdf5 && \
    curl -o ./benchmark-data/glove-50-angular.hdf5 http://ann-benchmarks.com/glove-50-angular.hdf5 && \
    curl -o ./benchmark-data/nytimes-256-angular.hdf5 http://ann-benchmarks.com/nytimes-256-angular.hdf5 && \
    curl -o ./benchmark-data/glove-100-angular.hdf5 http://ann-benchmarks.com/glove-100-angular.hdf5 && \
    curl -o ./benchmark-data/glove-25-angular.hdf5 http://ann-benchmarks.com/glove-25-angular.hdf5 && \
    curl -o ./benchmark-data/lastfm-64-dot.hdf5 http://ann-benchmarks.com/lastfm-64-dot.hdf5 && \
    curl -o ./benchmark-data/sift-128-euclidean.hdf5 http://ann-benchmarks.com/sift-128-euclidean.hdf5 && \
    curl -o ./benchmark-data/mnist-784-euclidean.hdf5 http://ann-benchmarks.com/mnist-784-euclidean.hdf5
```

Update the following lines in [docker-compose.yml](docker-compose.yml).

```yaml
services:
  benchmark-ann:
      dockerfile: Dockerfile-ann # <== update this line
```

Update the file: `./benchmark-scripts/ann/benchmark.py`. `weaviate_url` should be set to the Weaviate instance and `CPUs` should be set to the amount of CPUs on the machine running Weaviate.

Build the container: `$ docker-compose build --no-cache`

Run the container: `$ docker-compose up benchmark-ann -d`

The benchmark container will ouput files in the format: `results/weaviate_benchmark__{benchmark file}__{ef constructuin}__{max connections}.json`

#### Update the benchmark config

You can update the HNSW build config for this benchmark [here](benchmark-scripts/ann/benchmark.py).

### ANN 1B benchmark

#### Kubernetes cluster

Follow [these steps](https://weaviate.io/developers/weaviate/current/getting-started/installation.html#kubernetes-k8s) in the Weaviate docs to create a Weaviate Kubernetes cluster. 

Our K8s setup:

* 5 pods
* Per pod
  * 320 RAM - 80 CPU
  * SSD 960gb

Update `weaviate_url` in [benchmark-scripts/ann-1B/benchmark.py](./benchmark-scripts/ann-1B/benchmark.py) to reflect the URL of the cluster.

#### Import machine

Create a machine with >= 16 CPUs, 16 GB in memory, and a 200 GB SSD. The import will run from this machine.

Clone this repo and cd into it `$ git clone https://github.com/semi-technologies/weaviate-benchmarking && cd weaviate-benchmarking`

Download the files into a benchmark-data folder as outlined below.

```sh
$ mkdir benchmark-data && \
    curl -o ./benchmark-data/sift-128-euclidean.hdf5 https://storage.googleapis.com/semi-technologies-public-data/sift-1B-128-euclidean.hdf5
```

Update the following lines in [docker-compose.yml](docker-compose.yml).

```yaml
services:
  benchmark-ann:
      dockerfile: Dockerfile-ann1b # <== update this line
```

Build the container: `$ docker-compose build --no-cache`

Run the container: `$ docker-compose up -d`

The benchmark container will ouput files in the format: `results/weaviate_benchmark__{benchmark file}__{ef constructuin}__{max connections}.json`

### Inverted index benchmark

Clone this repo and cd into it `$ git clone https://github.com/semi-technologies/weaviate-benchmarking && cd weaviate-benchmarking`

Spin up a beefy machine, we've used a 32 CPU, 400GB Memory, 1000 GB SSD persistent disk that has Docker installed.

```yaml
services:
  benchmark-ann:
      dockerfile: Dockerfile-ii # <== update this line
```

### ANN + inverted index benchmark

...

### Import transformers module benchmark

...

## Documentation for speed benchmarker

Once installed (see-below), the tools tries to be entirely self-documenting. Every command has a `-h` help option that can tell you where to go from there. For example, start with a root help command running `benchmarker -h` and it will print something like the following output to tell you where to go from there:

```
A Weaviate Benchmarker

Usage:
  benchmarker [flags]
  benchmarker [command]

Available Commands:
  dataset        Benchmark vectors from an existing dataset
  help           Help about any command
  random-text    Benchmark nearText searches
  random-vectors Benchmark nearVector searches

Flags:
  -h, --help   help for benchmarker

Use "benchmarker [command] --help" for more information about a command.
```

Once you picked the command you're interested in, you can again use the help command to learn about the flags, for example running `benchmarker dataset -h` results in the following output:

```
Specify an existing dataset as a list of query vectors in a .json file to parse the query vectors and then query them with the specified parallelism

Usage:
  benchmarker dataset [flags]

Flags:
  -a, --api string         The API to use on benchmarks (default "graphql")
  -c, --className string   The Weaviate class to run the benchmark against
  -f, --format string      Output format, one of [text, json] (default "text")
  -h, --help               help for dataset
  -l, --limit int          Set the query limit (top_k) (default 10)
  -u, --origin string      The origin that Weaviate is running at (default "http://localhost:8080")
  -o, --output string      Filename for an output file. If none provided, output to stdout only
  -p, --parallel int       Set the number of parallel threads which send queries (default 8)
  -q, --queries string     Point to the queries file, (.json)
  -w, --where string       An entire where filter as a string
```

### Installation / Running the CLI

#### Option 1: Download a pre-compiled binary

Not supported yet, there is no CI pipeline yet that pushes artifacts

#### Option 2: With a local Go runtime, compiling on the fly

Print the available commands
```
cd benchmarker
go run . help
```

An example command

```
go run . random-vectors -c MyClass -d 384 -q 10000 -p 8 -a graphql -l 10
```

or the same command with the long-style flags:

```
go run . \
  random-vectors \
  --className MyClass \
  --dimensions 384 \
  --queries 10000 \
  --parallel 8 \
  --api graphql \
  --limit 10
```

#### Option 3: With a local Go runtime, compile and install just once

Install:

```
cd benchmarker && go install .
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
  --api graphql \
  --limit 10
```

### Use benchmarking API programmatically

TODO

### Roadmap

* [x] support random vectors
* [x] support specific vectors from json input file
* [ ] print results as json
* [ ] store results to file
* [ ] take in ground-truth file to calculate recall
* [ ] add versioning
* [ ] pre-build binaries on CI and attach them to releases
