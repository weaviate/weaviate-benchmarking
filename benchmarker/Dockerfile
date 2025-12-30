### BUILDER STAGE ###
FROM golang:1.24-bookworm AS builder
# or: FROM golang:1.24-ubuntu24.04

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libhdf5-dev \
    python3 \
    bash \
    cmake \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

# Enable CGO so HDF5 links correctly
RUN CGO_ENABLED=1 go build -o benchmarker .


### RUNTIME STAGE ###
FROM debian:bookworm-slim
# or: FROM ubuntu:24.04

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libhdf5-103 \
    python3 \
    bash \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/benchmarker /app/benchmarker
COPY --from=builder /app/scripts/ /app/scripts/

CMD ["/app/benchmarker"]