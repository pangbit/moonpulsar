#!/usr/bin/env bash
# Run on the disposable Linux test host with Docker, MoonBit, Python
# (cryptography installed), curl and OpenSSL.
set -euo pipefail
cd "$(dirname "$0")/.."

version=${1:?usage: scripts/test-athenz-broker-live.sh 4.2.4|3.3.9}
case "$version" in
  4.2.4) athenz_version=1.10.62 ;;
  3.3.9) athenz_version=1.10.50 ;;
  *) echo "unsupported Pulsar version: $version" >&2; exit 2 ;;
esac
if [[ $(id -u) -ne 0 ]]; then
  echo "run as root so the isolated broker can read its test keys" >&2
  exit 2
fi
python3 -c 'import cryptography' || {
  echo "Python cryptography is required for the ZTS fixture" >&2
  exit 2
}

name="moonpulsar-athenz-${version//./-}-$$"
dir=$(mktemp -d /tmp/moonpulsar-athenz.XXXXXX)
zts_pid=
cleanup() {
  if [[ -n $zts_pid ]]; then kill "$zts_pid" 2>/dev/null || true; fi
  docker rm -f "$name" >/dev/null 2>&1 || true
  rm -rf "$dir"
}
trap cleanup EXIT

openssl genrsa -out "$dir/zts.key" 2048 2>/dev/null
openssl rsa -in "$dir/zts.key" -pubout -out "$dir/zts.pub" 2>/dev/null
openssl genrsa -out "$dir/service.key" 2048 2>/dev/null
openssl rsa -in "$dir/service.key" -pubout -out "$dir/service.pub" 2>/dev/null
python3 - "$dir" <<'PY'
import base64
import json
from pathlib import Path
import subprocess
import sys
import time

directory = Path(sys.argv[1])
public_key = directory.joinpath("zts.pub").read_bytes()
def ybase64(data):
    return base64.b64encode(data).decode().replace("+", ".").replace("/", "_").replace("=", "-")

config = {
    "ztsUrl": "http://127.0.0.1/",
    "zmsUrl": "http://127.0.0.1/",
    "ztsPublicKeys": [{"id": "0", "key": ybase64(public_key)}],
    "zmsPublicKeys": [],
}
directory.joinpath("athenz.conf").write_text(json.dumps(config))
now = int(time.time())
unsigned = f"v=Z1;d=pulsar;r=reader;p=moonpulsar.test;a=fixture;t={now};e={now + 3600};k=0"
signature = subprocess.check_output(
    ["openssl", "dgst", "-sha256", "-sign", str(directory / "zts.key")],
    input=unsigned.encode(),
)
signed = unsigned + ";s=" + ybase64(signature)
directory.joinpath("role-token").write_text(signed)
directory.joinpath("wrong-domain-token").write_text(signed.replace("d=pulsar", "d=wrong", 1))
PY

image="apachepulsar/pulsar:$version"
mkdir "$dir/jars"
fetch_jar() {
  local group_path=$1 artifact=$2 artifact_version=$3
  curl -fsSL --retry 2 \
    "https://repo.maven.apache.org/maven2/$group_path/$artifact/$artifact_version/$artifact-$artifact_version.jar" \
    -o "$dir/jars/$artifact-$artifact_version.jar"
}
# The official Pulsar image omits its optional Athenz broker plugin.
fetch_jar org/apache/pulsar pulsar-broker-auth-athenz "$version"
for artifact in athenz-zpe-java-client athenz-zts-core athenz-zms-core athenz-auth-core athenz-client-common; do
  fetch_jar com/yahoo/athenz "$artifact" "$athenz_version"
done
fetch_jar com/yahoo/rdl rdl-java 1.5.4
docker run --rm --user 0 -v "$dir:/auth" "$image" \
  bin/pulsar tokens create-secret-key -o /auth/broker-key >/dev/null
docker run --rm --user 0 -v "$dir:/auth" "$image" \
  bin/pulsar tokens create -s broker -sk file:///auth/broker-key >"$dir/broker-token"
chown -R 10000:10000 "$dir"
chmod 700 "$dir"
chmod 700 "$dir/jars"
find "$dir" -type f -exec chmod 600 {} +

docker run -d --name "$name" --network host -v "$dir:/auth:ro" \
  -e PULSAR_PREFIX_brokerServicePort=7665 \
  -e PULSAR_PREFIX_webServicePort=8086 \
  -e PULSAR_PREFIX_advertisedAddress=127.0.0.1 \
  -e PULSAR_PREFIX_authenticationEnabled=true \
  -e PULSAR_PREFIX_authorizationEnabled=false \
  -e PULSAR_PREFIX_authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderAthenz,org.apache.pulsar.broker.authentication.AuthenticationProviderToken \
  -e PULSAR_PREFIX_athenzDomainNames=pulsar \
  -e PULSAR_PREFIX_tokenSecretKey=file:///auth/broker-key \
  -e PULSAR_PREFIX_brokerClientAuthenticationPlugin=org.apache.pulsar.client.impl.auth.AuthenticationToken \
  -e PULSAR_PREFIX_brokerClientAuthenticationParameters=file:///auth/broker-token \
  -e 'PULSAR_EXTRA_CLASSPATH=/auth/jars/*' \
  -e PULSAR_EXTRA_OPTS=-Dathenz.athenz_conf=/auth/athenz.conf \
  "$image" sh -c \
  'bin/apply-config-from-env.py conf/standalone.conf && bin/pulsar standalone --no-functions-worker' \
  >/dev/null

for _ in {1..90}; do
  if [[ $(docker inspect -f '{{.State.Running}}' "$name") != true ]]; then
    echo "Athenz broker exited before readiness" >&2
    docker logs "$name" 2>&1 | tail -n 35 >&2
    exit 1
  fi
  code=$(curl -s -o /dev/null -w '%{http_code}' \
    -H "Athenz-Role-Auth: $(cat "$dir/role-token")" \
    http://127.0.0.1:8086/admin/v2/namespaces/public/default || true)
  if [[ $code == 200 ]]; then break; fi
  sleep 1
done
if [[ $code != 200 ]]; then
  echo "Athenz-authenticated broker did not become ready (HTTP $code)" >&2
  docker logs "$name" 2>&1 | tail -n 35 >&2
  exit 1
fi

wrong_code=$(curl -s -o /dev/null -w '%{http_code}' \
  -H "Athenz-Role-Auth: $(cat "$dir/wrong-domain-token")" \
  http://127.0.0.1:8086/admin/v2/namespaces/public/default)
if [[ $wrong_code != 401 ]]; then
  echo "expected wrong-domain role token to be rejected; got HTTP $wrong_code" >&2
  exit 1
fi

PULSAR_URL=pulsar://127.0.0.1:7665 \
PULSAR_TOPIC="persistent://public/default/$name" \
PULSAR_ATHENZ_ROLE_TOKEN_FILE="$dir/role-token" \
moon run examples/athenz --target native

python3 -u interop/athenz_broker/mock_zts.py "$dir" >"$dir/zts-url" 2>"$dir/zts.log" &
zts_pid=$!
for _ in {1..30}; do
  if [[ -s $dir/zts-url ]]; then break; fi
  if ! kill -0 "$zts_pid" 2>/dev/null; then
    cat "$dir/zts.log" >&2
    exit 1
  fi
  sleep 0.1
done
if [[ ! -s $dir/zts-url ]]; then
  echo "ZTS fixture did not start" >&2
  exit 1
fi
PULSAR_URL=pulsar://127.0.0.1:7665 \
PULSAR_TOPIC="persistent://public/default/$name-zts" \
PULSAR_ATHENZ_ZTS_URL="$(cat "$dir/zts-url")" \
PULSAR_ATHENZ_PROVIDER_DOMAIN=pulsar \
PULSAR_ATHENZ_TENANT_DOMAIN=moonpulsar \
PULSAR_ATHENZ_TENANT_SERVICE=client \
PULSAR_ATHENZ_PRIVATE_KEY_FILE="$dir/service.key" \
moon run examples/athenz --target native

openssl genrsa -out "$dir/wrong-service.key" 2048 2>/dev/null
if PULSAR_URL=pulsar://127.0.0.1:7665 \
  PULSAR_TOPIC="persistent://public/default/$name-rejected" \
  PULSAR_ATHENZ_ZTS_URL="$(cat "$dir/zts-url")" \
  PULSAR_ATHENZ_PROVIDER_DOMAIN=pulsar \
  PULSAR_ATHENZ_TENANT_DOMAIN=moonpulsar \
  PULSAR_ATHENZ_TENANT_SERVICE=client \
  PULSAR_ATHENZ_PRIVATE_KEY_FILE="$dir/wrong-service.key" \
  moon run examples/athenz --target native >"$dir/rejected.log" 2>&1; then
  echo "ZTS accepted an NToken signed with the wrong service key" >&2
  exit 1
fi
if ! grep -q '401' "$dir/rejected.log"; then
  echo "wrong service key did not produce a ZTS HTTP 401" >&2
  cat "$dir/rejected.log" >&2
  exit 1
fi
echo "Athenz Broker accepted signed file and ZTS tokens; rejected wrong domain and service key"
