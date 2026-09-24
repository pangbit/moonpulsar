#!/usr/bin/env bash
# Run chunk and receive-queue memory checks on a dedicated Linux host.
# Requires Docker, MoonBit, and ports 7665/8086 free.
set -euo pipefail
cd "$(dirname "$0")/.."

version=${1:?usage: scripts/test-chunk-live.sh 4.2.4|3.3.9}
case "$version" in
  4.2.4|3.3.9) ;;
  *) echo "unsupported Pulsar version: $version" >&2; exit 2 ;;
esac
name="moonpulsar-chunk-live-${version//./-}-$$"
cleanup() {
  docker rm -f "$name" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker run -d --rm --name "$name" --network host \
  -e PULSAR_PREFIX_brokerServicePort=7665 \
  -e PULSAR_PREFIX_webServicePort=8086 \
  -e PULSAR_PREFIX_advertisedAddress=127.0.0.1 \
  "apachepulsar/pulsar:$version" sh -c \
  'bin/apply-config-from-env.py conf/standalone.conf && bin/pulsar standalone --no-functions-worker' \
  >/dev/null

for _ in {1..60}; do
  if curl -fsS http://127.0.0.1:8086/admin/v2/namespaces/public/default \
    >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
if ! curl -fsS http://127.0.0.1:8086/admin/v2/namespaces/public/default \
  >/dev/null 2>&1; then
  echo "isolated broker did not become ready" >&2
  docker logs "$name" 2>&1 | tail -n 30 >&2
  exit 1
fi

PULSAR_LIVE_CHUNK_URL=pulsar://127.0.0.1:7665 \
PULSAR_LIVE_CHUNK_ADMIN_URL=http://127.0.0.1:8086 \
PULSAR_LIVE_CHUNK_TOPIC="persistent://public/default/$name" \
moon test chunk_live_wbtest.mbt --target native
PULSAR_LIVE_QUEUE_URL=pulsar://127.0.0.1:7665 \
PULSAR_LIVE_QUEUE_TOPIC="persistent://public/default/$name-queue" \
moon test receive_queue_live_wbtest.mbt --target native
PULSAR_URL=pulsar://127.0.0.1:7665 \
PULSAR_NAMESPACE=public/default \
PULSAR_TEST_PREFIX="$name-roundtrip" \
moon run examples/memory_budget --target native
