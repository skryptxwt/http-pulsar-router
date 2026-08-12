# Security Exceptions

The security check normally fails for every vulnerability that `govulncheck`
can trace to a called symbol. The following temporary exceptions are limited to
the Avro implementation imported indirectly by `github.com/apache/pulsar-client-go`:

- `GO-2026-5046`: CPU exhaustion in the Avro decoder.
- `GO-2026-5047`: integer overflow in the Avro decoder.
- `GO-2026-5048`: unbounded map allocation in the Avro decoder.

The Go vulnerability database reports no fixed version for
`github.com/hamba/avro/v2` as of 2026-08-12. Pulsar client `v0.21.0`, the latest
release checked on that date, still depends on this affected module. This
service does not configure or decode application Avro schemas; it publishes
raw byte payloads with the default Pulsar bytes schema. The residual exposure
is therefore accepted temporarily rather than disabling vulnerability checks.

Review these exceptions whenever the Pulsar client or Avro dependency is
upgraded. Remove each ID from `scripts/security-check.ps1` as soon as an
upstream fixed version is available. Any other reachable vulnerability remains
a build failure.
