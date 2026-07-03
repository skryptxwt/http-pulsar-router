# syntax=docker/dockerfile:1

FROM golang:1.26.4 AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go test ./...

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -buildvcs=false -ldflags="-s -w" \
    -o /out/sr-forwarder ./cmd/sr-forwarder

FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app

COPY --from=builder /out/sr-forwarder /app/sr-forwarder
COPY config.example.json /app/config/config.json

USER nonroot:nonroot
EXPOSE 8080

ENTRYPOINT ["/app/sr-forwarder"]
CMD ["-config", "/app/config/config.json"]
