# Disposable certificate fixtures

`ca.crt` signs `server.crt` (SAN: localhost and 127.0.0.1; serverAuth) and
`client.crt` (clientAuth). The matching private keys are public test material.
They are only for `interop/athenz_cert/mock_zts.py` and must never be used for
real authentication. The CA signing key is intentionally not included.
