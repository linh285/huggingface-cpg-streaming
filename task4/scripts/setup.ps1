<#
=============================================================================
 Task 4 - One-shot Windows setup orchestrator
-----------------------------------------------------------------------------
 Assumes the merged stack is already up from the repository root:
     docker compose -f compose.yml -f task4/docker-compose.yml up -d

 Brings the Kafka -> Neo4j path online:
   1. Waits for the Kafka Connect REST API + Neo4j plugin to be ready.
   2. Ensures the four Kafka topics exist (Task 3 owns them; this is an
      idempotent safety net using --if-not-exists on the Task 3 broker).
   3. Applies the Neo4j schema (uniqueness constraint + edge index).
   4. Registers the two Neo4j sink connectors (idempotent PUT).

 Usage:  powershell -File scripts/setup.ps1
=============================================================================
#>
[CmdletBinding()]
param(
    [string]$ConnectUrl = "http://localhost:8083",
    [string]$KafkaContainer = "huggingface-cpg-kafka",   # container from Task 3 compose.yml
    [string]$KafkaTopicsBin = "/opt/kafka/bin/kafka-topics.sh",
    [string]$Neo4jContainer = "cpg-neo4j",
    [string]$Neo4jUser = "neo4j",
    [string]$Neo4jPassword = "cpgpassword"
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Task4Dir  = Split-Path -Parent $ScriptDir

function Wait-ForConnect {
    Write-Host "[setup] Waiting for Kafka Connect at $ConnectUrl ..." -ForegroundColor Cyan
    for ($i = 0; $i -lt 60; $i++) {
        try {
            $resp = Invoke-RestMethod -Uri "$ConnectUrl/connector-plugins" -TimeoutSec 5
            $hasNeo4j = $resp | Where-Object { $_.class -like "*Neo4jConnector*" }
            if ($hasNeo4j) {
                Write-Host "[setup] Kafka Connect is up and the Neo4j plugin is loaded." -ForegroundColor Green
                return
            }
            Write-Host "[setup] Connect up but Neo4j plugin not loaded yet... ($i)"
        } catch {
            Write-Host "[setup] Connect not ready yet... ($i)"
        }
        Start-Sleep -Seconds 5
    }
    throw "Kafka Connect / Neo4j plugin did not become ready in time."
}

# --- Step 1: wait for Connect + Neo4j plugin ---
Wait-ForConnect

# --- Step 2: ensure topics exist (Task 3 is the canonical owner) ---
Write-Host "[setup] Ensuring Kafka topics exist (idempotent)..." -ForegroundColor Cyan
$topics = @(
    @{ name = "cpg.nodes";    partitions = 1; cleanup = "compact" },
    @{ name = "cpg.edges";    partitions = 1; cleanup = "compact" },
    @{ name = "cpg.metadata"; partitions = 1; cleanup = "compact" },
    @{ name = "cpg.errors";   partitions = 1; cleanup = "delete"  }
)
foreach ($t in $topics) {
    Write-Host "  - $($t.name)"
    docker exec $KafkaContainer $KafkaTopicsBin --bootstrap-server localhost:9092 `
        --create --if-not-exists --topic $t.name `
        --partitions $t.partitions --replication-factor 1 `
        --config "cleanup.policy=$($t.cleanup)" | Out-Host
}

# --- Step 3: apply Neo4j schema ---
Write-Host "[setup] Applying Neo4j schema (constraints + indexes)..." -ForegroundColor Cyan
$initCypher = Get-Content -Raw "$Task4Dir/neo4j/init.cypher"
$initCypher | docker exec -i $Neo4jContainer cypher-shell -u $Neo4jUser -p $Neo4jPassword --format plain | Out-Host

# --- Step 4: register connectors ---
Write-Host "[setup] Registering Neo4j sink connectors..." -ForegroundColor Cyan
$connectorFiles = @("neo4j-sink-nodes.json", "neo4j-sink-edges.json")
foreach ($f in $connectorFiles) {
    $doc  = Get-Content -Raw "$Task4Dir/connectors/$f" | ConvertFrom-Json
    $name = $doc.name
    # Strip documentation-only keys (prefixed with '_') before sending.
    $cfg = @{}
    foreach ($p in $doc.config.PSObject.Properties) {
        if (-not $p.Name.StartsWith("_")) { $cfg[$p.Name] = $p.Value }
    }
    $body = $cfg | ConvertTo-Json -Depth 10
    Write-Host "  - PUT connector '$name'"
    $result = Invoke-RestMethod -Method Put -Uri "$ConnectUrl/connectors/$name/config" `
        -ContentType "application/json" -Body $body
    Write-Host "    registered (tasks.max=$($result.config.'tasks.max'))" -ForegroundColor Green
}

Write-Host ""
Write-Host "[setup] Connector status:" -ForegroundColor Cyan
$status = Invoke-RestMethod -Uri "$ConnectUrl/connectors?expand=status"
$status.PSObject.Properties | ForEach-Object {
    $c = $_.Value.status
    Write-Host ("  {0,-24} connector={1} tasks={2}" -f $_.Name, $c.connector.state, (($c.tasks | ForEach-Object { $_.state }) -join ","))
}
Write-Host ""
Write-Host "[setup] Done. Now publish events with:  python publish_jsonl_to_kafka.py" -ForegroundColor Green
