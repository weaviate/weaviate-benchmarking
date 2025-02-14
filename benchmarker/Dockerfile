FROM --platform=$BUILDPLATFORM golang:1.23-alpine AS builder
WORKDIR /app

# Install necessary dependencies
RUN apk add --no-cache musl-dev gcc g++ make glibc-dev gcc-aarch64-linux-gnu

# Conditional installation based on TARGETARCH
RUN if [ "$TARGETARCH" = "arm64" ]; then \
        export CC=aarch64-linux-gnu-gcc;\
    fi

# Set environment variables for CGO
ARG TARGETOS TARGETARCH
ENV CGO_ENABLED=1
ENV CC=gcc

COPY . .
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o benchmarker .

FROM --platform=$TARGETPLATFORM golang:1.23-alpine
RUN apk add --no-cache libstdc++ hdf5-dev python3 bash
WORKDIR /app

COPY --from=builder /app/benchmarker /app/benchmarker
COPY --from=builder /app/scripts/ /app/scripts/
CMD ["/app/benchmarker"]
