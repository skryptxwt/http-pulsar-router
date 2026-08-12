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

Alias endpoint with identical behavior:

```http
POST /gop/gop-data-service/api/v1/mss/web/alert/outer/add
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
If a Pulsar publish fails after some items were accepted, the response includes `accepted` with the number of items already written. Clients that retry a batch must drop the first `accepted` items or provide their own idempotency/deduplication on `uuId`.

## Config

`dataSet` routing is configured in JSON:

```json
{
  "server": {
    "addr": ":8080",
    "readTimeout": "5s",
    "writeTimeout": "10s",
    "shutdownTimeout": "15s",
    "requestTimeout": "9s",
    "readinessTimeout": "2s",
    "pulsarFailureLivenessThreshold": "5m",
    "maxBodyBytes": 1048576,
    "maxBatchItems": 1000,
    "auth": {
      "enabled": true,
      "bearerToken": "change-me",
      "bearerTokenFile": ""
    },
    "publishRetry": {
      "maxAttempts": 3,
      "initialBackoff": "100ms",
      "maxBackoff": "2s"
    },
    "circuitBreaker": {
      "enabled": true,
      "failureThreshold": 20,
      "openDuration": "10s"
    }
  },
  "pulsar": {
    "url": "pulsar://127.0.0.1:6650",
    "operationTimeout": "5s",
    "connectionTimeout": "5s"
  },
  "routes": {
    "mss_tag_push_event_test": {
      "topic": "persistent://public/default/mss_tag_push_event_test",
      "auth": {
        "bearerToken": "route-specific-token",
        "bearerTokenFile": ""
      },
      "validation": {
        "maxBodyBytes": 1048576,
        "maxBatchItems": 1000,
        "requiredFields": [
          "tenantId",
          "uuId"
        ]
      }
    }
  }
}
```

The service polls the config file and reloads route, request limit, publish retry, circuit breaker, and route validation changes without restarting.
`server.auth.enabled` defaults to `true`. When enabled, event endpoints require `Authorization: Bearer <token>`. The request is decoded only far enough to select the effective route token before authorization errors are returned; unauthorized responses do not expose route details. Health and metrics endpoints are not protected by this option.
Configure `server.auth.bearerToken` or `server.auth.bearerTokenFile` as the global fallback for every dataSet. A route can override the global token with `routes.<dataSet>.auth.bearerToken` or `bearerTokenFile`; once overridden, the global token is not accepted for that route. When auth is enabled without a global token, every route must configure its own token.
`server.publishRetry.maxAttempts` includes the first publish attempt. Set it to `1` to disable retries.
`server.requestTimeout` caps one HTTP publish request before `writeTimeout` is reached. Set it to `0s` to disable this internal deadline.
`server.readinessTimeout` caps the `/readyz` Pulsar readiness check.
`server.pulsarFailureLivenessThreshold` controls when an uninterrupted Pulsar failure also fails `/healthz`; it defaults to `5m`, and `0s` disables Pulsar-driven liveness failures. Readiness failures still remove the pod from Service endpoints immediately.
`server.circuitBreaker` opens per topic after consecutive final publish failures and fails fast with `503` for `openDuration`.
Route `validation` is optional. If a dataSet does not configure it, no per-dataSet body, batch, or required-field validation is applied.

## Metrics

Endpoint:

```http
GET /metrics
```

Important metrics:

```text
sr_forwarder_http_requests_total
sr_forwarder_rejected_requests_total
sr_forwarder_publish_success_total
sr_forwarder_publish_failure_total
sr_forwarder_publish_retry_total
sr_forwarder_publish_circuit_open_total
sr_forwarder_publish_circuit_rejected_total
sr_forwarder_publish_circuit_open
sr_forwarder_accepted_items_total
sr_forwarder_publish_latency_seconds_sum
sr_forwarder_publish_latency_seconds_count
```

## Local Build

```bash
pwsh ./scripts/security-check.ps1
```

The security check runs formatting, vet, unit tests, race tests, build, `govulncheck`, and Helm chart rendering. Use `pwsh ./scripts/security-check.ps1 -SkipHelm` on machines without Helm.

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
  server:
    auth:
      enabled: true
      bearerToken: "change-me"
  pulsar:
    url: pulsar://pulsar-broker.pulsar.svc.cluster.local:6650
  routes:
    mss_tag_push_event_test:
      topic: persistent://public/default/mss_tag_push_event_test
      auth:
        bearerToken: "route-specific-token"
```

The chart exposes the service as `ClusterIP` by default. When `config.server.auth.enabled=true`, Helm requires either a global HTTP token or a token on every route. Enable `ingress.enabled` and set `ingress.tls` when external HTTPS ingress is required. Enable `networkPolicy.enabled` only after providing the ingress and egress rules your cluster needs.

Upgrade:

```bash
helm upgrade --install http-pulsar-router ./charts/http-pulsar-router \
  -n sr-forwarder \
  -f values-prod.yaml
```

The chart uses rolling updates with `maxUnavailable: 0` and includes configurable startup, `/healthz` liveness, and `/readyz` readiness probes. Readiness performs a Pulsar topic metadata lookup. A failed publish or readiness lookup evicts the cached producer so a later retry creates a fresh one; the Pulsar client continues to handle connection reconnection internally.

## Status Codes

- `200`: all items were published to Pulsar.
- `400`: invalid JSON or invalid request shape.
- `401`: request authorization failed when `server.auth.enabled=true`; the response does not include route details.
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
