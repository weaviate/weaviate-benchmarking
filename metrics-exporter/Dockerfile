FROM golang:1.23-alpine
WORKDIR /app
COPY . .
RUN go build -o metrics-exporter .
CMD ["/app/metrics-exporter"]
