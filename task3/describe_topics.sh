#!/usr/bin/env bash
# Task 3 – Describe all four CPG Kafka topics (partition, replication, config).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/topics.env"

for topic in \
    "${TOPIC_NODES}" \
    "${TOPIC_EDGES}" \
    "${TOPIC_METADATA}" \
    "${TOPIC_ERRORS}"
do
    echo "============================================================"
    echo "Topic: ${topic}"
    docker compose exec -T \
        "${KAFKA_DOCKER_SERVICE}" \
        "${KAFKA_TOPICS_BIN}" \
        --bootstrap-server "${KAFKA_BOOTSTRAP_SERVER}" \
        --describe \
        --topic "${topic}"
    echo
done
