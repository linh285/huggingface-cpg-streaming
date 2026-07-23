// =============================================================================
// Task 4 - Neo4j schema bootstrap
// -----------------------------------------------------------------------------
// Run ONCE before registering the sink connectors. The node uniqueness
// constraint is the second guardrail behind idempotency: even if two node
// events with the same node_id were processed concurrently, Neo4j refuses to
// create a duplicate. The relationship index keeps the edge connector's
// MERGE-on-edge_id fast at scale.
// =============================================================================

// Uniqueness constraint on CPGNode.node_id (also creates a backing index).
CREATE CONSTRAINT cpgnode_node_id_unique IF NOT EXISTS
FOR (n:CPGNode)
REQUIRE n.node_id IS UNIQUE;

// Relationship-property index on edge_id to speed up MERGE (src)-[:CPG_EDGE {edge_id}]->(dst).
CREATE INDEX cpg_edge_id_index IF NOT EXISTS
FOR ()-[r:CPG_EDGE]-()
ON (r.edge_id);
