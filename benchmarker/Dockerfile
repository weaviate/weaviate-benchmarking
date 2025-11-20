FROM golang:1.24-alpine AS builder
RUN apk add --no-cache build-base hdf5-dev gcc libc-dev python3 bash g++ musl-dev make cmake
WORKDIR /app
COPY . .
RUN CGO_ENABLED=1 go build -o benchmarker .

FROM golang:1.24-alpine
RUN apk add --no-cache hdf5-dev python3 bash
WORKDIR /app
COPY --from=builder /app/benchmarker /app/benchmarker
COPY --from=builder /app/scripts/ /app/scripts/
CMD ["/app/benchmarker"]
