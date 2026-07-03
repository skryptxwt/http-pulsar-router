# Pulsar Auth Deployment

This service publishes HTTP payloads to Pulsar. It does not consume from Pulsar.

The chart can create the Kubernetes Secret during `helm upgrade --install`, so you do not need to run `kubectl create secret` first.

## Deploy Three Replicas

Use the Pulsar cluster exposed through node IP `10.72.9.83` and proxy NodePort `32655`:

```bash
helm upgrade --install http-pulsar-router ./charts/http-pulsar-router \
  -n sr-forwarder \
  --create-namespace \
  -f deploy/pulsar-values.yaml \
  --set-string config.server.auth.bearerToken='PASTE_HTTP_BEARER_TOKEN_HERE' \
  --set-string pulsarAuth.tokenBase64='PASTE_ADMIN_TOKEN_BASE64_HERE'
```

Use the base64 value from the Pulsar secret field:

```bash
kubectl get secret pulsar-auth-secret-key -n pulsar -o jsonpath='{.data.adminToken}'
```

Because `pulsarAuth.create=true`, Helm renders a Secret named `pulsar-auth-secret-key` in the release namespace. The pod mounts that Secret as:

```text
/app/secrets/pulsar/token
```

The application reads that file and connects to:

```text
pulsar://10.72.9.83:32655
```

## Values

`deploy/pulsar-values.yaml` configures:

```yaml
replicaCount: 3

pulsarAuth:
  enabled: true
  create: true
  secretName: pulsar-auth-secret-key
  secretKey: adminToken

config:
  server:
    auth:
      enabled: true
      bearerToken: ""
  pulsar:
    url: pulsar://10.72.9.83:32655
    authTokenFile: /app/secrets/pulsar/token
```

Do not commit real HTTP or Pulsar tokens into Git. Pass them at deploy time with `--set-string`, or keep them in an untracked local values file.
