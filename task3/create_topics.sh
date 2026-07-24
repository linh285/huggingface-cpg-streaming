#!/usr/bin/env bash
# Task 3 – Create four Kafka topics for the CPG streaming pipeline.
# Idempotent: uses --if-not-exists so it is safe to run multiple times.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/topics.env"

run_topics() {
    docker compose exec -T \
        "${KAFKA_DOCKER_SERVICE}" \
        "${KAFKA_TOPICS_BIN}" "$@"
}

create_topic() {
    local topic="$1"
    local cleanup="$2"
    shift 2

    echo "[CREATE] ${topic}"
    run_topics \
        --bootstrap-server "${KAFKA_BOOTSTRAP_SERVER}" \
        --create \
        --if-not-exists \
        --topic "${topic}" \
        --partitions "${KAFKA_PARTITIONS}" \
        --replication-factor "${KAFKA_REPLICATION_FACTOR}" \
        --config "cleanup.policy=${cleanup}" \
        "$@"
}

# cpg.nodes  – compact: keep latest node per node_id  → Neo4j Sink
create_topic "${TOPIC_NODES}"    compact

# cpg.edges  – compact: keep latest edge per edge_id  → Neo4j Sink
create_topic "${TOPIC_EDGES}"    compact

# cpg.metadata – compact: keep latest metadata per file_id → Spark→MongoDB
create_topic "${TOPIC_METADATA}" compact

# cpg.errors – delete with 7-day retention → Debug/monitoring
create_topic "${TOPIC_ERRORS}"   delete \
    --config retention.ms=604800000

echo
echo "[OK] Four Kafka topics are ready."
