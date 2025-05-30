FROM golang:1.22-alpine3.19 AS builder
RUN apk add --no-cache build-base hdf5-dev gcc libc-dev python3 bash g++ musl-dev make cmake
WORKDIR /app
COPY . .
# Set environment variables to reduce memory usage and optimize for stability
ENV CGO_ENABLED=1
ENV GOOS=linux
ENV GOGC=off
# Use less aggressive optimization and add memory limits to avoid GCC internal errors on ARM64
RUN go mod download && \
    go build -ldflags="-s -w" -a -installsuffix cgo -o benchmarker .

FROM golang:1.22-alpine3.19
RUN apk add --no-cache hdf5-dev python3 bash
WORKDIR /app
COPY --from=builder /app/benchmarker /app/benchmarker
COPY --from=builder /app/scripts/ /app/scripts/
CMD ["/app/benchmarker"]
