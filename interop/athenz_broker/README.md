# Isolated Athenz Broker check

On the disposable Linux test host, run from the repository root:

```sh
bash scripts/test-athenz-broker-live.sh 4.2.4
bash scripts/test-athenz-broker-live.sh 3.3.9
```

The script needs root, Docker, MoonBit, curl, OpenSSL, Python 3 with
`cryptography`, and access to Maven Central. It loads the official optional
Pulsar Athenz Broker plugin for the selected version, generates disposable
ZTS and service keys, then removes its container and credentials on exit.

It checks Broker rejection of a wrong-domain role token, MoonBit send/receive
with a signed role-token file, MoonBit RSA service-NToken exchange through the
local ZTS fixture, and ZTS rejection of an NToken signed with another key.
The fixture verifies the NToken signature and supplies a signed role token;
it is not a deployment of the Athenz ZMS/ZTS services.
