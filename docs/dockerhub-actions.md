# Docker Hub GitHub Actions

The workflow is defined in `.github/workflows/docker-image.yml`.

## Required GitHub Secrets

Configure these repository secrets:

```text
DOCKERHUB_USERNAME
DOCKERHUB_TOKEN
```

Use a Docker Hub access token instead of your account password.

## Trigger Rules

The workflow builds pull requests without pushing images.

It pushes images when:

- `main` is pushed.
- A `v*` tag is pushed, such as `v0.1.0`.
- The workflow is run manually with `workflow_dispatch`.

## Image

The default image is:

```text
lengdanlexin/http-pulsar-router
```

Common pushed tags for `main` pushes:

```text
latest
main
sha-<commit>
```

For `v*` tag pushes, only the original Git tag is pushed:

```text
v0.1.0
```

## Helm

The chart defaults to:

```yaml
image:
  repository: lengdanlexin/http-pulsar-router
  tag: ""
```

When `tag` is empty, Helm uses the chart `appVersion`.
