#!/usr/bin/env bash
# Task 3 – List all Kafka topics.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/topics.env"

docker compose exec -T \
    "${KAFKA_DOCKER_SERVICE}" \
    "${KAFKA_TOPICS_BIN}" \
    --bootstrap-server "${KAFKA_BOOTSTRAP_SERVER}" \
    --list
