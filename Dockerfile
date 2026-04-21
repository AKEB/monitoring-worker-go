# syntax=docker/dockerfile:1
FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
ARG VERSION=local
ENV CGO_ENABLED=0
RUN go build -buildvcs=false -trimpath \
	-ldflags "-s -w -X monitoring-worker-go/internal/buildinfo.Version=${VERSION}" \
	-o /monitoring-worker ./cmd/worker/

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
WORKDIR /opt/monitoring-worker
COPY --from=build /monitoring-worker /usr/local/bin/monitoring-worker
ENTRYPOINT ["/usr/local/bin/monitoring-worker"]
