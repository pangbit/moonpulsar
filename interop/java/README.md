# Java ECIES interoperability check

`EncryptionInterop.java` uses the official Java client shipped with Pulsar to
exercise P-521 ECIES message encryption in both directions. Use a disposable
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

Then run the Java receiver in its container:

```sh
java -cp '/pulsar/lib/*:/tmp' EncryptionInterop receive
```

The Java sender writes to `PULSAR_TOPIC-java`; MoonBit decrypts that message
and writes to `PULSAR_TOPIC-moonbit`; the Java receiver decrypts it. This
three-step exchange passed on isolated Pulsar 4.2.4 and 3.3.9 brokers. The
pinned Go client's default crypto implementation accepts only RSA keys, so
Go ECIES interop is not a valid acceptance test for this baseline.

For a broker requiring client certificates, set `PULSAR_URL` to its
`pulsar+ssl://` address and pass `PULSAR_TLS_CA_FILE`,
`PULSAR_TLS_CLIENT_CERT_FILE`, and `PULSAR_TLS_CLIENT_KEY_FILE` to both the Java
process and `examples/ec_encryption_interop`. The Java fixture enables CA and
hostname verification. The same three-step exchange passed over mutually
authenticated TLS on isolated 4.2.4 and 3.3.9 brokers.
