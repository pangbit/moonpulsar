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

The Go client first writes typed Protobuf Native and a two-message INT64 batch.
MoonBit decodes both, writes its own Protobuf Native and INT64 messages, then
writes a two-message STRING batch. Go decodes all three. The test uses a new
subscription for each read. Delete the dedicated topic and its `-int64` and
`-batch` companions afterward with your broker's Admin API.
