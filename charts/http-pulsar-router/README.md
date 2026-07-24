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
The HTTP service defaults to `ClusterIP`. `ingress.enabled`, `ingress.tls`, and `networkPolicy.enabled` are off by default.
`config.server.auth.enabled` defaults to `true`. The server token is the global fallback. A route-level token overrides it for that route; if no global token is configured, every route must define a token.

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
      bearerToken: "..."
  routes:
    mss_tag_push_event_test:
      topic: persistent://public/default/mss_tag_push_event_test
      auth:
        bearerToken: "route-specific-token"

ingress:
  enabled: true
  className: nginx
  hosts:
    - host: router.example.com
      paths:
        - path: /
          pathType: Prefix
  tls:
    - secretName: router-example-com-tls
      hosts:
        - router.example.com
```

Apply:

```bash
helm upgrade --install http-pulsar-router ./charts/http-pulsar-router \
  -n sr-forwarder \
  -f values-prod.yaml
```
