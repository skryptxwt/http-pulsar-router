# http-pulsar-router Helm Chart

## Install

```bash
helm upgrade --install http-pulsar-router ./charts/http-pulsar-router \
  --namespace sr-forwarder \
  --create-namespace
```

## Configure

Keep only environment-specific values in a small values file.
When `pulsarAuth.enabled=true`, the chart automatically sets `config.pulsar.authTokenFile` to `<pulsarAuth.mountPath>/token` unless it is explicitly overridden.

```yaml
replicaCount: 3

pulsarAuth:
  enabled: true
  create: true
  tokenBase64: "..."

config:
  pulsar:
    url: pulsar://10.72.9.83:32655
  server:
    auth:
      enabled: true
  routes:
    mss_tag_push_event_test:
      auth:
        bearerToken: "..."
```

Apply:

```bash
helm upgrade --install http-pulsar-router ./charts/http-pulsar-router \
  -n sr-forwarder \
  -f values-prod.yaml
```
