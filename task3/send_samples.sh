#!/usr/bin/env bash
# Task 3 – Send the four sample JSON events to their respective Kafka topics.
# Each sample is sent as a single-line "key:compact-json" to satisfy the
# kafka-console-producer parse.key=true requirement.
#
# Usage: ./task3/send_samples.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/topics.env"

# Compact a JSON file to a single line using python3
compact_json() {
    python3 -c "import json, sys; print(json.dumps(json.load(open(sys.argv[1])), separators=(',', ':'), ensure_ascii=False))" "$1"
}

send_message() {
    local topic="$1"
    local key="$2"
    local json_file="$3"

    echo "[SEND] topic=${topic}  key=${key}"

    # Combine key and compact JSON on a single line: "key:compactjson"
    local payload
    payload=$(compact_json "${json_file}")

    printf '%s:%s\n' "${key}" "${payload}" | \
        docker compose -f "${PROJECT_DIR}/compose.yml" exec -T \
            "${KAFKA_DOCKER_SERVICE}" \
            "${KAFKA_CONSOLE_PRODUCER_BIN}" \
            --bootstrap-server "${KAFKA_BOOTSTRAP_SERVER}" \
            --topic "${topic}" \
            --property parse.key=true \
            --property key.separator=':'

    echo "  ✔ sent"
}

# 1. Node event → cpg.nodes  (key = node_id)
send_message \
    "${TOPIC_NODES}" \
    "node:FunctionDef:arrow_dataset.py:map:L610" \
    "${SCRIPT_DIR}/samples/node-event.json"

# 2. Edge event → cpg.edges  (key = edge_id)
send_message \
    "${TOPIC_EDGES}" \
    "edge:AST:arrow_dataset.py:Module->FunctionDef:map:L610" \
    "${SCRIPT_DIR}/samples/edge-event.json"

# 3. Metadata event → cpg.metadata  (key = file_id)
send_message \
    "${TOPIC_METADATA}" \
    "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" \
    "${SCRIPT_DIR}/samples/metadata-event.json"

# 4. Error event → cpg.errors  (key = error_id)
send_message \
    "${TOPIC_ERRORS}" \
    "err:SyntaxError:broken_example.py:AST_PARSE:L25:C8" \
    "${SCRIPT_DIR}/samples/error-event.json"

echo
echo "[OK] All four sample events have been sent to Kafka."
