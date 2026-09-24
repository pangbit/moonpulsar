#!/usr/bin/env bash
# Exercise Broker-issued AUTH_CHALLENGE on an isolated Docker Broker.
set -euo pipefail
cd "$(dirname "$0")/.."

version=${1:?usage: scripts/test-auth-challenge-live.sh 4.2.4|3.3.9}
case "$version" in
  4.2.4|3.3.9) ;;
  *) echo "unsupported Pulsar version: $version" >&2; exit 2 ;;
esac
if [[ $(id -u) -ne 0 ]]; then
  echo "run as root so the isolated broker can read its test token" >&2
  exit 2
fi

name="moonpulsar-challenge-${version//./-}-$$"
dir=$(mktemp -d /tmp/moonpulsar-challenge.XXXXXX)
cleanup() {
  docker rm -f "$name" >/dev/null 2>&1 || true
  rm -rf "$dir"
}
trap cleanup EXIT

image="apachepulsar/pulsar:$version"
docker run --rm --user 0 -v "$PWD/interop/challenge_broker:/src:ro" -v "$dir:/auth" \
  "$image" sh -c \
  'mkdir /auth/classes && javac -proc:none -cp "/pulsar/lib/*" -d /auth/classes /src/ChallengeAuthProvider.java && jar cf /auth/challenge.jar -C /auth/classes .' \
  >/dev/null
docker run --rm --user 0 -v "$dir:/auth" "$image" \
  bin/pulsar tokens create-secret-key -o /auth/broker-key >/dev/null
docker run --rm --user 0 -v "$dir:/auth" "$image" \
  bin/pulsar tokens create -s broker -sk file:///auth/broker-key >"$dir/broker-token"
chown -R 10000:10000 "$dir"
chmod 700 "$dir" "$dir/classes"
find "$dir" -type f -exec chmod 600 {} +

docker run -d --name "$name" --network host -v "$dir:/auth:ro" \
  -e PULSAR_PREFIX_brokerServicePort=7665 \
  -e PULSAR_PREFIX_webServicePort=8086 \
  -e PULSAR_PREFIX_advertisedAddress=127.0.0.1 \
  -e PULSAR_PREFIX_authenticationEnabled=true \
  -e PULSAR_PREFIX_authorizationEnabled=false \
  -e PULSAR_PREFIX_authenticationProviders=moonpulsar.test.ChallengeAuthProvider,org.apache.pulsar.broker.authentication.AuthenticationProviderToken \
  -e PULSAR_PREFIX_tokenSecretKey=file:///auth/broker-key \
  -e PULSAR_PREFIX_brokerClientAuthenticationPlugin=org.apache.pulsar.client.impl.auth.AuthenticationToken \
  -e PULSAR_PREFIX_brokerClientAuthenticationParameters=file:///auth/broker-token \
  -e PULSAR_EXTRA_CLASSPATH=/auth/challenge.jar \
  "$image" sh -c \
  'bin/apply-config-from-env.py conf/standalone.conf && bin/pulsar standalone --no-functions-worker' \
  >/dev/null

for _ in {1..90}; do
  if [[ $(docker inspect -f '{{.State.Running}}' "$name") != true ]]; then
    echo "challenge broker exited before readiness" >&2
    docker logs "$name" 2>&1 | tail -n 35 >&2
    exit 1
  fi
  code=$(curl -s -o /dev/null -w '%{http_code}' \
    -H "Authorization: Bearer $(cat "$dir/broker-token")" \
    http://127.0.0.1:8086/admin/v2/namespaces/public/default || true)
  if [[ $code == 200 ]]; then break; fi
  sleep 1
done
if [[ $code != 200 ]]; then
  echo "challenge broker did not become ready (HTTP $code)" >&2
  docker logs "$name" 2>&1 | tail -n 35 >&2
  exit 1
fi

PULSAR_URL=pulsar://127.0.0.1:7665 moon run examples/auth_challenge --target native
