FROM golang:alpine as builder
RUN apk add --no-cache hdf5-dev gcc libc-dev
WORKDIR /build
COPY . .
RUN CGO_ENABLED=1 go build -o app .

FROM alpine
RUN apk add --no-cache hdf5-dev
COPY --from=builder /build/app /benchmarker
