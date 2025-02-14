FROM --platform=$BUILDPLATFORM golang:1.23-alpine AS builder
WORKDIR /app

# Install dependencies needed for CGO
RUN apk add --no-cache build-base libc-dev hdf5-dev musl-dev gcc g++ make

# Conditional installation based on TARGETARCH
RUN if [ "$TARGETARCH" = "arm64" ]; then \
        echo "Installing arm64-specific package" && apk add --no-cache gcc-aarch64-linux-gnu binutils-aarch64-linux-gnu \
        export CC=aarch64-linux-gnu-gcc; \
    fi

# Set environment variables for CGO
ARG TARGETOS TARGETARCH
ENV CGO_ENABLED=1

COPY . .
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o benchmarker .

FROM --platform=$TARGETPLATFORM golang:1.23-alpine
RUN apk add --no-cache hdf5-dev python3 bash
WORKDIR /app

COPY --from=builder /app/benchmarker /app/benchmarker
COPY --from=builder /app/scripts/ /app/scripts/
CMD ["/app/benchmarker"]
