FROM golang:1.22-alpine AS builder
RUN apk add --no-cache hdf5-dev gcc libc-dev python3 bash g++ musl-dev
ENV CGO_CFLAGS="-I/usr/include"
ENV CGO_LDFLAGS="-L/usr/lib -lhdf5 -lhdf5_hl"
WORKDIR /app
COPY . .
RUN CGO_ENABLED=1 go build -o benchmarker .

FROM golang:1.22-alpine
RUN apk add --no-cache hdf5-dev python3 bash
WORKDIR /app
COPY --from=builder /app/benchmarker /app/benchmarker
COPY --from=builder /app/scripts/ /app/scripts/
CMD ["/app/benchmarker"]
