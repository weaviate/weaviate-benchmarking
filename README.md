# weaviate-benchmarking

This repo contains both a library for benchmarking Weaviate e2e as well as a
CLI tool that makes use of the same library

## Documentation

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

## Installation / Running the CLI

### Option 1: Download a pre-compiled binary

Not supported yet, there is no CI pipeline yet that pushes artifacts

### Option 2: With a local Go runtime, compiling on the fly

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

### Option 3: With a local Go runtime, compile and install just once

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

## Use benchmarking API programmatically

TODO
