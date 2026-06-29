# SR Forwarder

Lightweight HTTP-to-Pulsar forwarder for bastion-host deployment.

## Flow

```text
HTTP JSON -> dataSet route -> Pulsar topic -> isolated ST service -> SR
```

## Run

```powershell
go run ./cmd/sr-forwarder -config ./config.example.json
```

## API

```http
POST /api/v1/events
Content-Type: application/json
```

The same forwarding behavior is also available at:

```http
POST /gop/gop-data-service/api/v1/mss/web/alert/outer/add
Content-Type: application/json
```

When `server.auth.enabled` is `true`, include:

```http
Authorization: Bearer <token>
```

If `server.auth.bearerToken` or `server.auth.bearerTokenFile` is configured, it is used globally. Otherwise the token is resolved from the matched `routes.<dataSet>.auth` config.

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

Each item in `data` is published as one Pulsar message. `uuId` is used as the message key when present.
Successful requests return `200 OK`.

## Hot Reload

Route, request limit, publish retry, circuit breaker, and route validation changes in the config file are reloaded without restarting the process. Existing requests continue with the config snapshot available when they are handled.

## Metrics

```http
GET /metrics
```

The endpoint exposes Prometheus text metrics for request results, publish success/failure, retries, circuit breaker opens, accepted items, and publish latency.
