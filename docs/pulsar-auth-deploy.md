# Pulsar Auth Deployment

This service publishes HTTP payloads to Pulsar. It does not consume from Pulsar.

Use this values file for the Pulsar cluster exposed through node IP `10.72.9.83` and proxy NodePort `32655`:

```bash
helm upgrade --install http-pulsar-router ./charts/http-pulsar-router \
  -n pulsar \
  -f deploy/pulsar-values.yaml
```

The chart deploys three replicas and mounts `pulsar-auth-secret-key/adminToken` as:

```text
/app/secrets/pulsar/token
```

The app connects with:

```text
pulsar://10.72.9.83:32655
```

If the router is deployed outside the `pulsar` namespace, copy the secret first because Kubernetes secrets are namespace-scoped:

```bash
kubectl get secret pulsar-auth-secret-key -n pulsar -o yaml \
  | sed 's/namespace: pulsar/namespace: sr-forwarder/' \
  | kubectl apply -n sr-forwarder -f -
```

Then install into `sr-forwarder`:

```bash
helm upgrade --install http-pulsar-router ./charts/http-pulsar-router \
  -n sr-forwarder \
  --create-namespace \
  -f deploy/pulsar-values.yaml
```
