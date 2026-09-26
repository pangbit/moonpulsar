#!/usr/bin/env bash
# Consumer and Reader timestamp-seek regression on an isolated Broker.
set -euo pipefail
cd "$(dirname "$0")/.."

version=${1:?usage: scripts/test-time-seek-live.sh 4.2.4|3.3.9}
case "$version" in
  4.2.4|3.3.9) ;;
  *) echo "unsupported Pulsar version: $version" >&2; exit 2 ;;
esac

name="moonpulsar-time-seek-${version//./-}-$$"
cleanup() {
  local status=$?
  if (( status != 0 )); then
    docker logs "$name" >&2 || true
  fi
  docker rm -f "$name" >/dev/null 2>&1 || true
  exit "$status"
}
trap cleanup EXIT

docker run -d --name "$name" --network host \
  -e PULSAR_PREFIX_brokerServicePort=7665 \
  -e PULSAR_PREFIX_webServicePort=8086 \
  -e PULSAR_PREFIX_advertisedAddress=127.0.0.1 \
  "apachepulsar/pulsar:$version" sh -c \
  'bin/apply-config-from-env.py conf/standalone.conf && bin/pulsar standalone --no-functions-worker' \
  >/dev/null

for _ in {1..90}; do
  if [[ $(docker inspect -f '{{.State.Running}}' "$name") != true ]]; then
    echo "time-seek Broker exited before readiness" >&2
    docker logs "$name" 2>&1 | tail -n 30 >&2
    exit 1
  fi
  if curl -fsS http://127.0.0.1:8086/admin/v2/namespaces/public/default \
    >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
if ! curl -fsS http://127.0.0.1:8086/admin/v2/namespaces/public/default \
  >/dev/null 2>&1; then
  echo "time-seek Broker did not become ready" >&2
  docker logs "$name" 2>&1 | tail -n 30 >&2
  exit 1
fi

namespace="time-seek-${version//./-}-$$"
curl -fsS -X PUT -H 'Content-Type: application/json' -d '{}' \
  "http://127.0.0.1:8086/admin/v2/namespaces/public/$namespace"
PULSAR_URL=pulsar://127.0.0.1:7665 \
PULSAR_TOPIC="persistent://public/$namespace/seek" \
moon run examples/time_seek --target native
