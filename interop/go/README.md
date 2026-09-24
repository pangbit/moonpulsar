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

The Go client first writes typed Protobuf Native, a two-message INT64 batch,
and INT8, INT16, INT32, FLOAT, DOUBLE, and BYTES values. MoonBit decodes these
and writes matching values in the other direction, then writes a two-message
STRING batch. Go decodes the MoonBit messages. INT16/32/64 use the Go client's
little-endian integer encoding. The test uses a new subscription for each
read. Delete the dedicated topic and its type-suffixed companions afterward
with your broker's Admin API.
