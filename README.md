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

## Hot Reload

Route, request limit, and publish retry changes in the config file are reloaded without restarting the process. Existing requests continue with the config snapshot available when they are handled.
