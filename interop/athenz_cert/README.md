# Athenz certificate identity check

From the repository root, start the disposable HTTPS ZTS fixture:

```sh
python3 interop/athenz_cert/mock_zts.py --fail-first
```

It prints a `https://localhost:<port>` URL and requires a client certificate
signed by `testdata/athenz_cert/ca.crt`. Set `ZTS_URL` to that URL, then run:

```sh
export ZTS_CLIENT_CERT_FILE=testdata/athenz_cert/client.crt
export ZTS_CLIENT_KEY_FILE=testdata/athenz_cert/client.key
export ZTS_CA_FILE=testdata/athenz_cert/ca.crt
moon run examples/athenz_cert --target native
```

The example validates the role token returned after the fixture's first 503
response. For a negative check, use `server.crt` and `server.key` as the client
identity: the server rejects the certificate because it is issued for server
authentication. Using an unrelated CA also fails certificate verification.

The PEM files here are public test credentials, valid only for this fixture.
The fixture tests the ZTS exchange and mTLS transport; a live Athenz service
and an Athenz-authenticated Pulsar broker are separate integration targets.
