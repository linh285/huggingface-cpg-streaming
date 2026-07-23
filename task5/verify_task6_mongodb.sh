#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-$ROOT_DIR/docker-compose.task5.yml}"
FILE_ID="task6-demo-file"
ORIGINAL_HASH="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
MODIFIED_HASH="bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

compose() {
  docker compose -f "$COMPOSE_FILE" "$@"
}

mongo_eval() {
  compose exec -T mongodb mongosh --quiet cpg --eval "$1"
}

wait_for_hash() {
  local expected_hash="$1"
  local attempt
  local actual

  for attempt in $(seq 1 "${WAIT_ATTEMPTS:-90}"); do
    actual="$(
      mongo_eval \
        "const d=db.source_metadata.findOne({_id:'$FILE_ID'}); print(d ? d.content_sha256 : '')" |
        tr -d '\r'
    )"
    if [[ "$actual" == "$expected_hash" ]]; then
      return 0
    fi
    sleep 2
  done

  echo "Timed out waiting for content_sha256=$expected_hash" >&2
  echo "Check logs: docker compose -f $COMPOSE_FILE logs metadata-stream" >&2
  return 1
}

assert_single_document() {
  local count
  count="$(
    mongo_eval "print(db.source_metadata.countDocuments({_id:'$FILE_ID'}))" |
      tr -d '\r'
  )"
  if [[ "$count" != "1" ]]; then
    echo "Expected exactly 1 document, found $count" >&2
    return 1
  fi
}

echo "[1/6] Starting Kafka, MongoDB and Spark..."
compose up -d

echo "[2/6] Removing only the previous demo document..."
mongo_eval "db.source_metadata.deleteOne({_id:'$FILE_ID'})" >/dev/null

echo "[3/6] Publishing the original event twice..."
"$ROOT_DIR/task5/publish_sample_metadata.sh" original
"$ROOT_DIR/task5/publish_sample_metadata.sh" original
wait_for_hash "$ORIGINAL_HASH"
assert_single_document

echo "[4/6] Publishing the modified event twice with the same file_id..."
"$ROOT_DIR/task5/publish_sample_metadata.sh" modified
"$ROOT_DIR/task5/publish_sample_metadata.sh" modified
wait_for_hash "$MODIFIED_HASH"
assert_single_document

offset_before="$(
  mongo_eval "const d=db.source_metadata.findOne({_id:'$FILE_ID'}); print(d.kafka_offset)" |
    tr -d '\r'
)"

echo "[5/6] Restarting Spark with the existing checkpoint..."
compose restart metadata-stream
sleep 15

offset_after="$(
  mongo_eval "const d=db.source_metadata.findOne({_id:'$FILE_ID'}); print(d.kafka_offset)" |
    tr -d '\r'
)"

if [[ "$offset_before" != "$offset_after" ]]; then
  echo "Unexpected rewrite after restart: offset $offset_before -> $offset_after" >&2
  exit 1
fi
assert_single_document

checkpoint_files="$(
  compose exec -T metadata-stream sh -c \
    "find /opt/spark-checkpoints/cpg-metadata -type f 2>/dev/null | wc -l" |
    tr -d '\r'
)"

echo "[6/6] PASS"
echo "MongoDB documents for file_id : 1"
echo "Final content SHA-256  : $MODIFIED_HASH"
echo "Kafka offset unchanged : $offset_before"
echo "Checkpoint files       : $checkpoint_files"
echo
mongo_eval \
  "db.source_metadata.find({_id:'$FILE_ID'},{_id:1,path:1,content_sha256:1,line_count:1,kafka_offset:1,processed_at:1}).pretty()"
