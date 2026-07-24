#!/usr/bin/env pwsh
<#
Bring the Kafka -> Neo4j path online using Compose service names.
Run from any directory after the root + Task 4 Compose stack is up.
#>
[CmdletBinding()]
param(
    [string]$ConnectUrl = "http://localhost:8083",
    [string]$Neo4jUser = "neo4j",
    [string]$Neo4jPassword = "cpgpassword"
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Task4Dir = Split-Path -Parent $ScriptDir
$ProjectRoot = Split-Path -Parent $Task4Dir
$ComposeFiles = @(
    "-f", (Join-Path $ProjectRoot "compose.yml"),
    "-f", (Join-Path $Task4Dir "docker-compose.yml")
)

Write-Host "[setup] Waiting for Kafka Connect and the Neo4j plugin..."
$ready = $false
for ($attempt = 1; $attempt -le 60; $attempt++) {
    try {
        $plugins = Invoke-RestMethod -Uri "$ConnectUrl/connector-plugins" -TimeoutSec 5
        if ($plugins | Where-Object { $_.class -like "*Neo4jConnector*" }) {
            $ready = $true
            break
        }
    } catch {
        # The service may still be starting.
    }
    Start-Sleep -Seconds 5
}
if (-not $ready) {
    throw "Kafka Connect or the Neo4j connector plugin is not ready."
}

Write-Host "[setup] Ensuring all four topics exist..."
$topics = @(
    @{ Name = "cpg.nodes"; Cleanup = "compact" },
    @{ Name = "cpg.edges"; Cleanup = "compact" },
    @{ Name = "cpg.metadata"; Cleanup = "compact" },
    @{ Name = "cpg.errors"; Cleanup = "delete" }
)
foreach ($topic in $topics) {
    & docker compose @ComposeFiles exec -T kafka `
        /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 `
        --create --if-not-exists --topic $topic.Name --partitions 1 `
        --replication-factor 1 --config "cleanup.policy=$($topic.Cleanup)"
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to create topic $($topic.Name)."
    }
}

Write-Host "[setup] Applying Neo4j constraints and indexes..."
Get-Content -Raw (Join-Path $Task4Dir "neo4j/init.cypher") |
    & docker compose @ComposeFiles exec -T neo4j cypher-shell `
        -u $Neo4jUser -p $Neo4jPassword --format plain
if ($LASTEXITCODE -ne 0) {
    throw "Failed to apply the Neo4j schema."
}

Write-Host "[setup] Registering the two sink connectors..."
foreach ($filename in @("neo4j-sink-nodes.json", "neo4j-sink-edges.json")) {
    $document = Get-Content -Raw (Join-Path $Task4Dir "connectors/$filename") |
        ConvertFrom-Json
    $config = @{}
    foreach ($property in $document.config.PSObject.Properties) {
        if (-not $property.Name.StartsWith("_")) {
            $config[$property.Name] = $property.Value
        }
    }
    $body = $config | ConvertTo-Json -Depth 10
    Invoke-RestMethod -Method Put `
        -Uri "$ConnectUrl/connectors/$($document.name)/config" `
        -ContentType "application/json" -Body $body | Out-Null
}

$statuses = Invoke-RestMethod -Uri "$ConnectUrl/connectors?expand=status"
$statuses.PSObject.Properties | ForEach-Object {
    $status = $_.Value.status
    $tasks = ($status.tasks | ForEach-Object { $_.state }) -join ","
    Write-Host ("{0}: connector={1}, tasks={2}" -f $_.Name, $status.connector.state, $tasks)
}
