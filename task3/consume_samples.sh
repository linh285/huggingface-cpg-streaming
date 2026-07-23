#!/usr/bin/env bash
# Task 3 – Consume one message from each of the four Kafka topics to verify delivery.
# Prints:  <record-key> | <JSON-value>
#
# Usage: ./task3/consume_samples.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/topics.env"

consume_one() {
    local topic="$1"
    echo "================ ${topic} ================"
    docker compose -f "${PROJECT_DIR}/compose.yml" exec -T \
        "${KAFKA_DOCKER_SERVICE}" \
        "${KAFKA_CONSOLE_CONSUMER_BIN}" \
        --bootstrap-server "${KAFKA_BOOTSTRAP_SERVER}" \
        --topic "${topic}" \
        --from-beginning \
        --max-messages 1 \
        --property print.key=true \
        --property key.separator=' | '
    echo
}

for topic in \
    "${TOPIC_NODES}" \
    "${TOPIC_EDGES}" \
    "${TOPIC_METADATA}" \
    "${TOPIC_ERRORS}"
do
    consume_one "${topic}"
done
