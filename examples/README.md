# Runnable examples

[Documentation](../docs/README.md) | [简体中文](README.zh-CN.md)

Run all commands below from the repository root.


The `examples/` workspace module contains runnable programs (each needs a local broker at `pulsar://127.0.0.1:6650`):

| Example | Shows |
| --- | --- |
| `examples/producer` | sync `send` and `send_async` |
| `examples/producer_batching` | batching config and manual `flush` |
| `examples/consumer` | exclusive subscription, receive + ack |
| `examples/consumer_shared` | two workers sharing one subscription |
| `examples/consumer_nack` | negative ack and redelivery |
| `examples/roundtrip` | produce-then-consume verification |
| `examples/auth_token` | token authentication (`PULSAR_TOKEN` env var) |
| `examples/oauth2` | OAuth2 client credentials from a token endpoint |
| `examples/athenz` | Athenz ZTS service-key exchange or a sidecar role-token file |
| `examples/athenz_cert` | certificate-authenticated ZTS role-token exchange |
| `examples/auth_challenge` | respond to a Broker-issued authentication challenge |
| `examples/memory_budget` | bounded producer, consumer, chunk and Reader roundtrip on a real broker |
| `examples/chunk_interop` | compressed chunk exchange with the official Go and Java clients |
| `examples/tls` | verified TLS send/receive, optional client certificate/key and broker-restart check |
| `examples/token_rotation` | invalid-token rejection and valid file-token rotation on one client |
| `examples/token_reconnect` | active producer/consumer recovery after file-token and broker signing-key rotation |
| `examples/producer_compression` | Zstd-compressed payloads |
| `examples/reader` | replay a topic from the start with a reader |
| `examples/encryption_interop` | Go/MoonBit encrypted messages, reader replay, retry and dead-letter forwarding |
| `examples/ec_encryption_interop` | Java/MoonBit P-256, P-384 or P-521 ECIES messages in both directions |
| `examples/avro_evolution_interop` | Java v1 writer to MoonBit v2 reader, and MoonBit v1 writer to Java v2 reader |
| `examples/transaction_participants` | register producer and subscription participants in a transaction |
| `examples/transactional_batch_ack` | verify rollback redelivery and commit for whole-batch or per-index transactional ACK (`PULSAR_BATCH_INDEX_ACK=true`) |
| `examples/time_seek` | Consumer and Reader timestamp seek on a real broker |
| `examples/live_capabilities` | real-broker regression for multi-topic, pattern, ack timeout, seek, chunking, transactions, and optional Admin REST |
| `examples/pattern_removal` | delete and recreate active non-partitioned and partitioned pattern sources, then receive again |

Run one with:

```sh
moon run examples/producer --target native
```

For a TLS broker, set `PULSAR_TLS_URL`; the example verifies against system
roots unless `PULSAR_TLS_CA_FILE` supplies a private CA. Set
`PULSAR_TLS_CLIENT_CERT_FILE` and `PULSAR_TLS_CLIENT_KEY_FILE` together for
mutual TLS; `PULSAR_TOKEN` can still select token authentication. Set
`PULSAR_TOPIC` and `PULSAR_SUBSCRIPTION` to isolate the run. To verify an
existing client across a broker restart, also set
`PULSAR_TLS_RECONNECT_CHECK=1` and restart the broker after the first
roundtrip message appears; the example waits 30 seconds before sending again.
`PULSAR_TLS_INSECURE=1` explicitly disables certificate verification for a
disposable local broker.

`examples/token_rotation` reads valid initial and next tokens from
`PULSAR_TOKEN_INITIAL_FILE` and `PULSAR_TOKEN_NEXT_FILE`. It copies them into
a disposable working file, verifies that the broker rejects an invalid rotated
token, then opens a new connection from the same client with the next valid
token. Set `PULSAR_URL`, `PULSAR_TOPIC`, and `PULSAR_SUBSCRIPTION` for an
isolated broker run. The example never changes the supplied token files.

`examples/token_reconnect` uses the same two token-file inputs plus
`PULSAR_RECONNECT_READY_FILE` and `PULSAR_RECONNECT_DONE_FILE`. It sends and
acknowledges one message, copies the next token into its private working file,
then creates the ready file. Switch the isolated broker's signing key from the
initial token's key to the next token's key and restart that broker; create the
done file after it is healthy. The example rejects a new connection with the
old token and verifies that its original producer and consumer send, receive,
and acknowledge after reconnecting. This passed on isolated 4.2.4 and 3.3.9
brokers. The test runner must keep both input token files until the example
exits; this example only changes its private working file.
On a dedicated Linux test host, `scripts/test-token-reconnect-live.sh 4.2.4`
or `scripts/test-token-reconnect-live.sh 3.3.9` runs the full scenario as root
with a disposable authenticated container on ports 7665 and 8086. The script
generates fresh signing keys and tokens, then removes them with the container.

On a dedicated Linux test host, run `scripts/test-chunk-live.sh 4.2.4` or
`scripts/test-chunk-live.sh 3.3.9` to exercise consumer and Reader incomplete-chunk
expiry ACK, shared-budget failure/release, receive-queue overflow and replay,
and a complete chunked roundtrip against a
disposable broker on ports 7665 and 8086. The script cleans up its container.

The extended integration scenario accepts `PULSAR_URL`, `PULSAR_TOKEN` or
`PULSAR_TOKEN_FILE`,
`PULSAR_TEST_PREFIX`, and optional `PULSAR_ADMIN_URL`:

```sh
PULSAR_URL=pulsar://127.0.0.1:6650 PULSAR_TEST_PREFIX=moonpulsar-check \
  moon run examples/live_capabilities --target native
```

A quick local broker via docker:

```sh
docker run --rm -p 6650:6650 -p 8080:8080 apachepulsar/pulsar:4.2.4 bin/pulsar standalone -a 127.0.0.1
```
