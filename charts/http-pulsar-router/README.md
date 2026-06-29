# http-pulsar-router Helm Chart

## Install

```bash
helm upgrade --install http-pulsar-router ./charts/http-pulsar-router \
  --namespace sr-forwarder \
  --create-namespace
```

## Configure

Override only the deployment-specific settings in a values file. Runtime defaults such as timeouts, retry, circuit breaker, and validation do not need to be repeated unless you want to override them.

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
  tokenBase64: ""

config:
  server:
    auth:
      enabled: true
      bearerToken: ""
      bearerTokenFile: ""
  pulsar:
    url: pulsar://10.72.9.83:32655
    authTokenFile: /app/secrets/pulsar/token
  routes:
    mss_tag_push_event_test:
      topic: persistent://public/default/mss_tag_push_event_test
      auth:
        bearerToken: ""
        bearerTokenFile: ""
```

Apply:

```bash
helm upgrade --install http-pulsar-router ./charts/http-pulsar-router \
  -n sr-forwarder \
  -f values-prod.yaml
```
