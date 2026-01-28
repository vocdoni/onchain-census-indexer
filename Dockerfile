# syntax=docker/dockerfile:1
ARG GO_VERSION=1.25.5

FROM golang:${GO_VERSION}-alpine AS build
RUN apk add --no-cache ca-certificates git build-base
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . ./
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags="-s -w" -o /out/onchain-census-indexer ./cmd/onchain-census-indexer

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=build /out/onchain-census-indexer /usr/local/bin/onchain-census-indexer

ENV DB_PATH=/data
ENV LISTEN_ADDR=:8080
ENV LOG_LEVEL=debug

VOLUME ["/data"]
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/onchain-census-indexer"]
