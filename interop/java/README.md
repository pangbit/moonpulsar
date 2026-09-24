# Java ECIES interoperability check

`EncryptionInterop.java` uses the official Java client shipped with Pulsar to
exercise P-256, P-384 or P-521 ECIES message encryption in both directions. Use a disposable
broker and a fresh `PULSAR_TOPIC` in a test namespace. The fixture PEM files
are in `testdata/encryption`.

The Java process needs its client libraries on the classpath and readable copies
of both PEM files. In the Pulsar 4.2.4 or 3.3.9 Docker image, for example:

```sh
javac -proc:none -cp '/pulsar/lib/*' /tmp/EncryptionInterop.java
export PULSAR_URL=pulsar://127.0.0.1:6650
export PULSAR_TOPIC=persistent://public/default/moonpulsar-ec-test
export EC_PUBLIC_KEY=/tmp/ec_public.pem
export EC_PRIVATE_KEY=/tmp/ec_private.pem
java -cp '/pulsar/lib/*:/tmp' EncryptionInterop send
```

With `PULSAR_URL` and `PULSAR_TOPIC` pointing to the same broker and topic from
the MoonBit workspace root, run:

```sh
moon run examples/ec_encryption_interop --target native
```

Set `EC_PUBLIC_KEY` and `EC_PRIVATE_KEY` in the MoonBit environment to the
same curve's PEM files if testing a curve other than the default P-521.

Then run the Java receiver in its container:

```sh
java -cp '/pulsar/lib/*:/tmp' EncryptionInterop receive
```

The Java sender writes to `PULSAR_TOPIC-java`; MoonBit decrypts that message
and writes to `PULSAR_TOPIC-moonbit`; the Java receiver decrypts it. This
three-step exchange passed for P-256, P-384 and P-521 on isolated Pulsar 4.2.4
and 3.3.9 brokers. The
pinned Go client's default crypto implementation accepts only RSA keys, so
Go ECIES interop is not a valid acceptance test for this baseline.

For a broker requiring client certificates, set `PULSAR_URL` to its
`pulsar+ssl://` address and pass `PULSAR_TLS_CA_FILE`,
`PULSAR_TLS_CLIENT_CERT_FILE`, and `PULSAR_TLS_CLIENT_KEY_FILE` to both the Java
process and `examples/ec_encryption_interop`. The Java fixture enables CA and
hostname verification. The same three-step exchange passed over mutually
authenticated TLS on isolated 4.2.4 and 3.3.9 brokers.

## Avro schema evolution

`AvroEvolutionInterop.java` uses the official Java client's Avro generic
schema. It writes v1 records with `id: int` and reads them using a v2 schema
with the alias `identifier`, promoted `long`, and default `active=true`.
Compile it inside either Pulsar image with:

```sh
javac -proc:none -cp '/pulsar/lib/*' /tmp/AvroEvolutionInterop.java
```

Point both processes at the same disposable broker and a fresh
`PULSAR_TOPIC`, then run:

```sh
java -cp '/pulsar/lib/*:/tmp' AvroEvolutionInterop send
moon run examples/avro_evolution_interop --target native
java -cp '/pulsar/lib/*:/tmp' AvroEvolutionInterop receive
```

The MoonBit step resolves the Java writer's schema version using
`Client::decode_avro`, then writes a separate v1 record. The Java step reads
that record with its v2 schema. Both directions passed on isolated 4.2.4 and
3.3.9 brokers in a dedicated namespace.
