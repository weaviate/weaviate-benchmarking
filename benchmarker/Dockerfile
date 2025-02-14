FROM --platform=$BUILDPLATFORM golang:1.23-alpine AS builder
WORKDIR /app

# Install dependencies needed for CGO
RUN apk add --no-cache hdf5-dev musl-dev gcc g++ make
# RUN apk add --no-cache build-base libc-dev python3 bash

# Set environment variables for CGO
ARG TARGETOS TARGETARCH

COPY . .
RUN GO_ENABLED=1 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o benchmarker .

FROM --platform=$TARGETPLATFORM golang:1.23-alpine
RUN apk add --no-cache hdf5-dev python3 bash
WORKDIR /app

COPY --from=builder /app/benchmarker /app/benchmarker
COPY --from=builder /app/scripts/ /app/scripts/
CMD ["/app/benchmarker"]
