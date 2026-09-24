#!/usr/bin/env bash
# Official Go/Java and MoonBit bidirectional compressed-chunk exchange.
set -euo pipefail
cd "$(dirname "$0")/.."

version=${1:?usage: scripts/test-chunk-interop-live.sh 4.2.4|3.3.9}
case "$version" in
  4.2.4|3.3.9) ;;
  *) echo "unsupported Pulsar version: $version" >&2; exit 2 ;;
esac

name="moonpulsar-chunk-interop-${version//./-}-$$"
dir=$(mktemp -d /tmp/moonpulsar-chunk-interop.XXXXXX)
cleanup() {
  docker rm -f "$name" >/dev/null 2>&1 || true
  rm -rf "$dir"
}
trap cleanup EXIT

dd if=/dev/urandom of="$dir/payload" bs=262144 count=1 status=none
docker run -d --name "$name" --network host \
  -e PULSAR_PREFIX_brokerServicePort=7665 \
  -e PULSAR_PREFIX_webServicePort=8086 \
  -e PULSAR_PREFIX_advertisedAddress=127.0.0.1 \
  "apachepulsar/pulsar:$version" sh -c \
  'bin/apply-config-from-env.py conf/standalone.conf && bin/pulsar standalone --no-functions-worker' \
  >/dev/null

for _ in {1..90}; do
  if [[ $(docker inspect -f '{{.State.Running}}' "$name") != true ]]; then
    echo "chunk interop Broker exited before readiness" >&2
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
  echo "chunk interop Broker did not become ready" >&2
  docker logs "$name" 2>&1 | tail -n 30 >&2
  exit 1
fi

export PULSAR_URL=pulsar://127.0.0.1:7665
export PULSAR_TOPIC="persistent://public/default/$name"
export PULSAR_PAYLOAD_FILE="$dir/payload"
if ! (cd interop/go && go run ./chunk send) >"$dir/go-send.log" 2>&1; then
  cat "$dir/go-send.log" >&2
  exit 1
fi
tail -n 1 "$dir/go-send.log"
moon run examples/chunk_interop --target native
if ! (cd interop/go && go run ./chunk receive) >"$dir/go-receive.log" 2>&1; then
  cat "$dir/go-receive.log" >&2
  exit 1
fi
tail -n 1 "$dir/go-receive.log"

docker cp interop/java/ChunkInterop.java "$name:/tmp/ChunkInterop.java"
docker cp "$dir/payload" "$name:/tmp/chunk-payload"
docker exec "$name" javac -proc:none -cp '/pulsar/lib/*' \
  /tmp/ChunkInterop.java
java_probe() {
  docker exec \
    -e PULSAR_URL="$PULSAR_URL" \
    -e PULSAR_TOPIC="$PULSAR_TOPIC-java-test" \
    -e PULSAR_PAYLOAD_FILE=/tmp/chunk-payload \
    "$name" java -cp '/pulsar/lib/*:/tmp' ChunkInterop "$1"
}
java_probe send >"$dir/java-send.log" 2>&1 || {
  cat "$dir/java-send.log" >&2
  exit 1
}
tail -n 1 "$dir/java-send.log"
PULSAR_PEER=java PULSAR_TOPIC="$PULSAR_TOPIC-java-test" \
  moon run examples/chunk_interop --target native
java_probe receive >"$dir/java-receive.log" 2>&1 || {
  cat "$dir/java-receive.log" >&2
  exit 1
}
tail -n 1 "$dir/java-receive.log"
