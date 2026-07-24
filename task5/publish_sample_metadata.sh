#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-$ROOT_DIR/docker-compose.task5.yml}"
VERSION="${1:-original}"
SAMPLE_FILE="$ROOT_DIR/task5/samples/${VERSION}_metadata.json"

if [[ ! -f "$SAMPLE_FILE" ]]; then
  echo "Usage: $0 [original|modified]" >&2
  exit 2
fi

payload="$(
  python3 -c \
    'import json,sys; print(json.dumps(json.load(open(sys.argv[1], encoding="utf-8")), separators=(",", ":")))' \
    "$SAMPLE_FILE"
)"
file_id="$(
  python3 -c \
    'import json,sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["file_id"])' \
    "$SAMPLE_FILE"
)"

printf '%s|%s\n' "$file_id" "$payload" |
  docker compose -f "$COMPOSE_FILE" exec -T kafka \
    /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka:9092 \
    --topic cpg.metadata \
    --property parse.key=true \
    --property 'key.separator=|'

echo "Published $VERSION metadata for file_id=$file_id"
