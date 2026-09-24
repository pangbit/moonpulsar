# Schema and batch interoperability check

This fixture uses the official `pulsar-client-go` client pinned to
`a83c1519f68d`. It and `examples/schema_interop` share the descriptor in
`testdata/protobuf_native_key_value.proto`.

Choose a fresh test topic and set the broker URL and authentication token when
needed:

```sh
export PULSAR_URL=pulsar://127.0.0.1:6650
export PULSAR_TOPIC=persistent://public/default/moonpulsar-schema-interop-test
# export PULSAR_TOKEN=...
```

From the repository root, run the three steps in order:

```sh
(cd interop/go && go run . send)
moon run examples/schema_interop --target native
(cd interop/go && go run . receive)
```

The Go client first writes typed Protobuf Native, legacy Protobuf, Avro, a
two-message INT64 batch, and INT8, INT16, INT32, FLOAT, DOUBLE, and BYTES
values. MoonBit decodes these and writes matching values in the other direction,
then writes a two-message STRING batch. Go decodes the MoonBit messages. The
legacy Protobuf schema uses an Avro-style JSON definition and ordinary
protobuf message bytes;
INT16/32/64 use the Go client's little-endian integer encoding. The test uses
a new subscription for each read. Delete the dedicated topic and its
type-suffixed companions afterward with your broker's Admin API.

The full three-step exchange passed on Pulsar 4.2.4 and an isolated Pulsar
3.3.9 broker.

## Encryption interoperability

`encryption/main.go` and `examples/encryption_interop` exchange RSA-OAEP-SHA1
wrapped, AES-256-GCM encrypted messages in both directions. The MoonBit example
also checks encrypted Reader replay, retry and dead-letter forwarding, and
errors when no private key is configured. Use a fresh `PULSAR_TOPIC` and the
same `PULSAR_URL` / optional `PULSAR_TOKEN` variables as above:

```sh
(cd interop/go && go run ./encryption send)
moon run examples/encryption_interop --target native
(cd interop/go && go run ./encryption receive)
```

Both senders write an encrypted empty payload. The MoonBit sender also writes
encrypted Zlib and two-message batches; the Go receiver decodes every nonempty payload.
The MoonBit example verifies `Consume` ciphertext delivery and ACK, then
`Discard` automatic ACK and delivery of the following plaintext message.
The PEM files under `testdata/encryption` are
disposable test keys. The full exchange passed on Pulsar 4.2.4 and an isolated
3.3.9 broker.

The pinned Go client's consumer currently exposes the 16-byte GCM tag for an
encrypted empty payload: its decode path only swaps in the decrypted buffer
when `UncompressedSize > 0`. Run `go run ./encryption receive-empty` after the
MoonBit step to reproduce this limitation. MoonBit decodes Go's encrypted empty
payload correctly.
