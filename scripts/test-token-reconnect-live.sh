#!/usr/bin/env bash
# Run on a disposable Linux test host with Docker, MoonBit, curl, and root access.
set -euo pipefail
cd "$(dirname "$0")/.."

version=${1:?usage: scripts/test-token-reconnect-live.sh 4.2.4|3.3.9}
case "$version" in
  4.2.4|3.3.9) ;;
  *) echo "unsupported Pulsar version: $version" >&2; exit 2 ;;
esac
if [[ $(id -u) -ne 0 ]]; then
  echo "run as root so the isolated broker can read its private test keys" >&2
  exit 2
fi

name="moonpulsar-token-reconnect-${version//./-}-$$"
dir=$(mktemp -d /tmp/moonpulsar-token-reconnect.XXXXXX)
client_pid=
cleanup() {
  if [[ -n $client_pid ]]; then
    kill "$client_pid" 2>/dev/null || true
  fi
  docker rm -f "$name" >/dev/null 2>&1 || true
  rm -rf "$dir"
}
trap cleanup EXIT

image="apachepulsar/pulsar:$version"
docker run --rm --user 0 -v "$dir:/auth" "$image" \
  bin/pulsar tokens create-secret-key -o /auth/key-a >/dev/null
docker run --rm --user 0 -v "$dir:/auth" "$image" \
  bin/pulsar tokens create-secret-key -o /auth/key-b >/dev/null
docker run --rm --user 0 -v "$dir:/auth" "$image" \
  bin/pulsar tokens create -s initial -sk file:///auth/key-a >"$dir/token-a"
docker run --rm --user 0 -v "$dir:/auth" "$image" \
  bin/pulsar tokens create -s next -sk file:///auth/key-b >"$dir/token-b"
cp "$dir/key-a" "$dir/key"
cp "$dir/token-a" "$dir/active-token"
chown -R 10000:10000 "$dir"
chmod 700 "$dir"
chmod 600 "$dir"/*

docker run -d --rm --name "$name" --network host -v "$dir:/auth:ro" \
  -e PULSAR_PREFIX_brokerServicePort=7665 \
  -e PULSAR_PREFIX_webServicePort=8086 \
  -e PULSAR_PREFIX_advertisedAddress=127.0.0.1 \
  -e PULSAR_PREFIX_authenticationEnabled=true \
  -e PULSAR_PREFIX_authorizationEnabled=false \
  -e PULSAR_PREFIX_authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderToken \
  -e PULSAR_PREFIX_tokenSecretKey=file:///auth/key \
  -e PULSAR_PREFIX_brokerClientAuthenticationPlugin=org.apache.pulsar.client.impl.auth.AuthenticationToken \
  -e PULSAR_PREFIX_brokerClientAuthenticationParameters=file:///auth/active-token \
  "$image" sh -c \
  'bin/apply-config-from-env.py conf/standalone.conf && bin/pulsar standalone --no-functions-worker' \
  >/dev/null

wait_for_namespace() {
  local token=$1 code
  for _ in {1..60}; do
    code=$(curl -s -o /dev/null -w '%{http_code}' \
      -H "Authorization: Bearer $(cat "$token")" \
      http://127.0.0.1:8086/admin/v2/namespaces/public/default || true)
    if [[ $code == 200 ]]; then return 0; fi
    sleep 1
  done
  echo "authenticated broker did not become ready" >&2
  docker logs "$name" 2>&1 | tail -n 30 >&2
  return 1
}
wait_for_namespace "$dir/token-a"

PULSAR_URL=pulsar://127.0.0.1:7665 \
PULSAR_TOPIC="persistent://public/default/$name" \
PULSAR_TOKEN_INITIAL_FILE="$dir/token-a" \
PULSAR_TOKEN_NEXT_FILE="$dir/token-b" \
PULSAR_RECONNECT_READY_FILE="$dir/ready" \
PULSAR_RECONNECT_DONE_FILE="$dir/done" \
moon run examples/token_reconnect --target native >"$dir/client.log" 2>&1 &
client_pid=$!
for _ in {1..300}; do
  if [[ -s $dir/ready ]]; then break; fi
  if ! kill -0 "$client_pid" 2>/dev/null; then
    cat "$dir/client.log" >&2
    exit 1
  fi
  sleep 0.1
done
if [[ ! -s $dir/ready ]]; then
  echo "client did not reach the pre-restart checkpoint" >&2
  cat "$dir/client.log" >&2
  exit 1
fi

cp "$dir/key-b" "$dir/key"
cp "$dir/token-b" "$dir/active-token"
docker restart "$name" >/dev/null
wait_for_namespace "$dir/token-b"
touch "$dir/done"
if ! wait "$client_pid"; then
  cat "$dir/client.log" >&2
  exit 1
fi
client_pid=
cat "$dir/client.log"
