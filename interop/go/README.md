# Protobuf Native interoperability check

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

The Go client first writes a typed Protobuf Native message, MoonBit decodes it
and writes another, and the Go client decodes the MoonBit message. The test
uses a new subscription for each read. Delete the dedicated topic afterward
with your broker's Admin API.
