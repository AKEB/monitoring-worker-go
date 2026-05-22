# syntax=docker/dockerfile:1

# Cross-compile on the CI host (amd64); no QEMU during go build.
FROM --platform=$BUILDPLATFORM golang:1.22-alpine AS build
RUN apk add --no-cache ca-certificates
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=local
ARG TARGETOS
ARG TARGETARCH
ENV CGO_ENABLED=0
RUN GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -buildvcs=false -trimpath \
	-ldflags "-s -w -X monitoring-worker-go/internal/buildinfo.Version=${VERSION}" \
	-o /monitoring-worker ./cmd/worker/

# CA bundle, zoneinfo, and ping (iputils) for RunPing jobs.
FROM alpine:3.20 AS runtime
RUN apk add --no-cache ca-certificates tzdata iputils \
	&& mkdir -p /deps/bin /deps/lib /deps/usr/lib \
	&& cp -a /bin/ping /deps/bin/ \
	&& cp -a /lib/ld-musl-*.so.1 /deps/lib/ \
	&& cp -a /usr/lib/libcap.so* /deps/usr/lib/

FROM scratch
COPY --from=runtime /deps /
COPY --from=runtime /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=runtime /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=build /monitoring-worker /monitoring-worker
ENTRYPOINT ["/monitoring-worker"]
