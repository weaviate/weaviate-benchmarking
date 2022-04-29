# weaviate-benchmarking

This repo contains both a library for benchmarking Weaviate e2e as well as a
CLI tool that makes use of the same library

## Running the CLI

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
