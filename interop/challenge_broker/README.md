# Broker-issued authentication challenge

Run this test on the disposable Linux host from the repository root:

```sh
bash scripts/test-auth-challenge-live.sh 4.2.4
bash scripts/test-auth-challenge-live.sh 3.3.9
```

The script compiles the local test authentication provider against the
selected official Pulsar image, starts an isolated Broker, and removes the
container and disposable token afterward. The provider sends a real binary
`AUTH_CHALLENGE` during CONNECT. The MoonBit example checks that its auth
supplier is invoked a second time, that the Broker accepts the response, and
that an invalid second response fails the connection.
