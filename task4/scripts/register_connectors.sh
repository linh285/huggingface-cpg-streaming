#!/usr/bin/env bash
# =============================================================================
# Task 4 - Register (or update) the two Neo4j sink connectors on Kafka Connect.
# -----------------------------------------------------------------------------
# Uses PUT on /config so the call is itself idempotent: re-running updates the
# existing connector in place instead of erroring with 409 Conflict.
# =============================================================================
set -euo pipefail

CONNECT_URL="${1:-http://localhost:8083}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONN_DIR="${SCRIPT_DIR}/../connectors"

register() {
  local file="$1"
  local name
  name="$(python -c "import json,sys;print(json.load(open(sys.argv[1]))['name'])" "$file")"
  echo "[connect] Registering ${name} from $(basename "$file")"
  # Strip documentation-only keys (prefixed with '_') before sending.
  python -c "import json,sys;c=json.load(open(sys.argv[1]))['config'];print(json.dumps({k:v for k,v in c.items() if not k.startswith('_')}))" "$file" \
    | curl -sS -X PUT \
        -H "Content-Type: application/json" \
        "${CONNECT_URL}/connectors/${name}/config" \
        -d @- | python -m json.tool
  echo
}

register "${CONN_DIR}/neo4j-sink-nodes.json"
register "${CONN_DIR}/neo4j-sink-edges.json"

echo "[connect] Connector status:"
curl -sS "${CONNECT_URL}/connectors?expand=status" | python -m json.tool
