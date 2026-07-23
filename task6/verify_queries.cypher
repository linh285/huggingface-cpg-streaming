// =============================================================================
// Task 6 - Idempotent replay verification queries (Neo4j Browser)
// -----------------------------------------------------------------------------
// Replace <FILE_ID> with the file_id printed by replay_single_file.py.
// =============================================================================

// 1. Node/edge counts for the replayed file (compare BEFORE vs AFTER) -------
MATCH (n:CPGNode {file_id: '<FILE_ID>'}) RETURN count(n) AS file_nodes;
MATCH ()-[r:CPG_EDGE {file_id: '<FILE_ID>'}]->() RETURN count(r) AS file_edges;

// 2. IDEMPOTENCY PROOF: still no duplicated node_id after replay (0 rows) ---
MATCH (n:CPGNode)
WITH n.node_id AS node_id, count(*) AS c
WHERE c > 1
RETURN node_id, c;

// 3. IDEMPOTENCY PROOF: still no duplicated edge_id after replay (0 rows) ---
MATCH ()-[r:CPG_EDGE]->()
WITH r.edge_id AS edge_id, count(*) AS c
WHERE c > 1
RETURN edge_id, c;

// 4. Confirm the replayed nodes carry the fresh event_time (updated in place)
MATCH (n:CPGNode {file_id: '<FILE_ID>'})
RETURN n.event_time AS event_time, count(*) AS c
ORDER BY event_time DESC;

// 5. Global totals (unchanged files must be untouched by the single-file replay)
MATCH (n:CPGNode) RETURN count(n) AS total_nodes;
MATCH ()-[r:CPG_EDGE]->() RETURN count(r) AS total_edges;
