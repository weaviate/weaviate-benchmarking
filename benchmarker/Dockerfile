FROM golang:alpine
RUN apk add --no-cache hdf5-dev gcc libc-dev python3 bash
WORKDIR /app
COPY . .
RUN CGO_ENABLED=1 go build -o benchmarker .
CMD ["/app/benchmarker"]
