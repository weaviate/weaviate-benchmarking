FROM golang:alpine as builder
WORKDIR /build
COPY . .
RUN go build -o app .

FROM alpine
COPY --from=builder /build/app /benchmarker
