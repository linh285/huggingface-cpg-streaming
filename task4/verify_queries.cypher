// =============================================================================
// Task 4 - Verification queries (paste into the Neo4j Browser at :7474)
// =============================================================================

// 1. Total topology counts -------------------------------------------------
MATCH (n:CPGNode) RETURN count(n) AS total_nodes;
MATCH ()-[r:CPG_EDGE]->() RETURN count(r) AS total_edges;

// 2. Node breakdown by AST node_type --------------------------------------
MATCH (n:CPGNode)
RETURN coalesce(n.node_type, '<placeholder>') AS node_type, count(*) AS c
ORDER BY c DESC;

// 3. Edge breakdown by category (AST / CFG / DFG / CALL) --------------------
MATCH ()-[r:CPG_EDGE]->()
RETURN r.edge_type AS edge_type, count(*) AS c
ORDER BY c DESC;

// 4. IDEMPOTENCY PROOF: no duplicated node_id (expect: 0 rows) --------------
MATCH (n:CPGNode)
WITH n.node_id AS node_id, count(*) AS c
WHERE c > 1
RETURN node_id, c;

// 5. IDEMPOTENCY PROOF: no duplicated edge_id (expect: 0 rows) --------------
MATCH ()-[r:CPG_EDGE]->()
WITH r.edge_id AS edge_id, count(*) AS c
WHERE c > 1
RETURN edge_id, c;

// 6. Sample subgraph around one function definition (visual sanity check) --
MATCH (f:CPGNode {node_type: 'FunctionDef'})
WITH f LIMIT 1
MATCH p = (f)-[:CPG_EDGE*1..2]->(m)
RETURN p LIMIT 100;

// 7. Per-file node counts (which file contributed what) --------------------
MATCH (n:CPGNode)
RETURN n.file_id AS file_id, count(*) AS nodes
ORDER BY nodes DESC
LIMIT 20;
