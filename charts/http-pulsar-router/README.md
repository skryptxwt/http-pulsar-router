# http-pulsar-router Helm Chart

## Install

```bash
helm upgrade --install http-pulsar-router ./charts/http-pulsar-router \
  --namespace sr-forwarder \
  --create-namespace
```

## Configure

Keep stable deployment choices in a small values file, and pass environment-specific values such as Pulsar address and temporary tokens with `--set-string`.

```yaml
replicaCount: 3

image:
  repository: lengdanlexin/http-pulsar-router
  tag: latest
  pullPolicy: Always

pulsarAuth:
  enabled: true
  create: true
  secretName: pulsar-auth-secret-key
  secretKey: adminToken
  mountPath: /app/secrets/pulsar

config:
  server:
    auth:
      enabled: true
  pulsar:
    authTokenFile: /app/secrets/pulsar/token
  routes:
    mss_tag_push_event_test:
      topic: persistent://public/default/mss_tag_push_event_test
      auth:
        bearerTokenFile: ""
```

Apply:

```bash
helm upgrade --install http-pulsar-router ./charts/http-pulsar-router \
  -n sr-forwarder \
  -f values-prod.yaml \
  --set-string config.pulsar.url='pulsar://10.72.9.83:32655' \
  --set-string pulsarAuth.tokenBase64='...' \
  --set-string config.routes.mss_tag_push_event_test.auth.bearerToken='...'
```
