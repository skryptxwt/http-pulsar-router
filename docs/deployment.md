# Deployment Guide

This project provides a lightweight HTTP-to-Pulsar router for bastion-host or Kubernetes deployment.

## Runtime Flow

```text
HTTP JSON -> dataSet route -> Pulsar topic -> isolated ST service -> SR
```

## HTTP API

Endpoint:

```http
POST /api/v1/events
Content-Type: application/json
```

Payload:

```json
{
  "dataSet": "mss_tag_push_event_test",
  "data": [
    {
      "tenantId": "95842832",
      "uuId": "incident-7e4ab624-cf12-4878-bf4d-4091e62d1f51",
      "alertTag": "{\"alert-53b8beb9-a7b8-44d2-b6c1-6e1777652153\":[\"keyAlert\"]}"
    }
  ]
}
```

Each item in `data` is published as a separate Pulsar message. `uuId` is used as the Pulsar message key.

## Config

`dataSet` routing is configured in JSON:

```json
{
  "server": {
    "addr": ":8080",
    "readTimeout": "5s",
    "writeTimeout": "10s",
    "shutdownTimeout": "15s",
    "maxBodyBytes": 1048576,
    "maxBatchItems": 1000
  },
  "pulsar": {
    "url": "pulsar://127.0.0.1:6650",
    "operationTimeout": "5s",
    "connectionTimeout": "5s"
  },
  "routes": {
    "mss_tag_push_event_test": {
      "topic": "persistent://public/default/mss_tag_push_event_test"
    }
  }
}
```

The service polls the config file and reloads route changes without restarting.

## Local Build

```bash
go test ./...
go build -buildvcs=false -o bin/sr-forwarder ./cmd/sr-forwarder
```

Run:

```bash
./bin/sr-forwarder -config ./config.example.json
```

## Docker

Build:

```bash
docker build -t lengdanlexin/http-pulsar-router:0.1.0 .
```

Run:

```bash
docker run --rm -p 8080:8080 \
  -v "$PWD/config.example.json:/app/config/config.json:ro" \
  lengdanlexin/http-pulsar-router:0.1.0
```

Push:

```bash
docker push lengdanlexin/http-pulsar-router:0.1.0
```

## Helm

Install:

```bash
helm upgrade --install http-pulsar-router ./charts/http-pulsar-router \
  --namespace sr-forwarder \
  --create-namespace
```

Production values example:

```yaml
replicaCount: 2

image:
  repository: lengdanlexin/http-pulsar-router
  tag: "0.1.0"

config:
  pulsar:
    url: pulsar://pulsar-broker.pulsar.svc.cluster.local:6650
  routes:
    mss_tag_push_event_test:
      topic: persistent://public/default/mss_tag_push_event_test
```

Upgrade:

```bash
helm upgrade --install http-pulsar-router ./charts/http-pulsar-router \
  -n sr-forwarder \
  -f values-prod.yaml
```

The chart uses rolling updates with `maxUnavailable: 0` and includes `/healthz` and `/readyz` probes.

## Status Codes

- `202`: all items were published to Pulsar.
- `400`: invalid JSON or invalid request shape.
- `413`: request body or batch item count exceeds the configured limit.
- `422`: `dataSet` has no configured route.
- `503`: publish to Pulsar failed; caller should retry.

## GitHub Sync

```bash
git init
git remote add origin https://github.com/skryptxwt/http-pulsar-router.git
git add .
git commit -m "Initial http pulsar router"
git branch -M main
git push -u origin main
```
