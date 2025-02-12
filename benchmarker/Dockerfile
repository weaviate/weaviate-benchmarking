FROM golang:1.22-alpine AS builder
RUN apk add --no-cache hdf5-dev gcc libc-dev python3 bash g++ musl-dev
WORKDIR /app
COPY . .
RUN CGO_ENABLED=1 go build -o benchmarker .

FROM alpine:latest
RUN apk add --no-cache hdf5-dev python3 bash
COPY --from=builder /app/benchmarker /app/benchmarker
CMD ["/app/benchmarker"]
