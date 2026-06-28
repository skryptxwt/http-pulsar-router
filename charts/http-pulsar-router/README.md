# http-pulsar-router Helm Chart

## Install

```bash
helm upgrade --install http-pulsar-router ./charts/http-pulsar-router \
  --namespace sr-forwarder \
  --create-namespace
```

## Configure

Override Pulsar and route settings in a values file:

```yaml
config:
  pulsar:
    url: pulsar://pulsar-broker.pulsar.svc.cluster.local:6650
  routes:
    mss_tag_push_event_test:
      topic: persistent://public/default/mss_tag_push_event_test
```

Apply:

```bash
helm upgrade --install http-pulsar-router ./charts/http-pulsar-router \
  -n sr-forwarder \
  -f values-prod.yaml
```
