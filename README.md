![System Diagram](./Report/images/diagram.png)
![System Diagram](./Report/images/upper_layer.png) ![System Diagram](./Report/images/middle_layer.png) ![System Diagram](./Report/images/lower_layer.png)




# Project Structure

```
team-gurumi/mc/
├── Makefile                     # Build and installation commands
├── go.mod / go.sum              # Go module dependency management
├── run_experiments1.sh          # Experiment execution script
├── run_fault_tolerance_benchmark.sh
├── analyze_metrics.py           # Script for analyzing experimental results
├── agent / control / seeder     # Compiled executable binaries
│
├── cmd/                         # Main application entry points
│ ├── agent/                     # Agent node entry point (task execution and status reporting)
│ ├── control/                   # Control server entry point (central manager & HTTP API)
│ ├── seeder/                    # Seeder node entry point (P2P file delivery)
│ └── dhtget/                    # DHT lookup utility (for debugging)
│
├── pkg/                         # Core shared libraries
│ ├── agent/                     # Agent logic: task execution, lease management, status updates
│ ├── dht/                       # DHT node and XOR routing implementation
│ ├── p2p/                       # Peer-to-peer communication protocol
│ ├── task/                      # Task structures and resource-claim logic
│ ├── demand/                    # PostgreSQL-backed job storage and management
│ └── seeder/                    # Seeder request handling and data distribution logic
│
├── Report/                      # Project reports and diagrams
│ ├── 09-구르미-1차보고서-금채원.pdf
│ ├── 09-구르미-2차보고서.md
│ └── images/                    # Diagrams and images used in reports
│
├── experiment_logs/             # Logs from experiment runs
│ ├── exp2_agents_100.log
│ └── exp3_kill_10.log
│
└── vendor/                      # External Go dependencies (vendored)
```

## Folder Overview

cmd/ — Entry points for executable components.  
pkg/ — Core logic shared across all executables.  
Report/ — Reports and related visual assets.  
experiment_logs/ — Experiment output logs.  
vendor/ — Vendored Go module dependencies.

## Requirements

PostgreSQL **must** be installed before running the project or executing experiment scripts.

## Required Environment Variables for Experiments

Before running any experiment scripts, the following environment variables must be set:

```bash
# PostgreSQL DB Connection String(etc)
export MC_DB_DSN='postgres://mcuser:mcpw@127.0.0.1:5432/mc?sslmode=disable'

# Disable authentication
export MC_DISABLE_AUTH=1

# Docker socket permissions
export DOCKER_HOST=unix:///var/run/docker.sock
```

# Remote Installation and Task Execution Guide

The server must already have **Go**, **Docker**, and **systemd** installed.

## 1. Installation

### Install Control Server (Remote)

```bash
make install-control HOST=ubuntu@control-box PORT=8080 TIMEOUT=180
````

### Install Agent (Remote)

Pass the Control server's address (local or Tailscale IP).

```bash
make install-agent HOST=ubuntu@node-1 CONTROL_URL=http://100.70.1.2:8080
```

Install another Agent on a different machine and enable `kata-runtime`:

```bash
make install-agent HOST=ubuntu@B CONTROL_URL=http://100.70.1.2:8080 DOCKER_RUNTIME=kata-runtime
```

## 2. Restarting and Checking Logs

```bash
make agent-restart HOST=ubuntu@node-1
make logs-agent    HOST=ubuntu@node-1
```

## 3. Checking the Agent's NODE_ID

```bash
ssh ubuntu@B 'hostname'          # example output: "node-b"
```

or check systemd environment variables:

```bash
ssh ubuntu@B 'systemctl show -p Environment mc-agent'
```

## 4. User Submits a Task (Specify target_node)

```bash
curl -X POST http://100.70.1.2:8080/api/tasks/push \
  -H 'Content-Type: application/json' \
  -d '{
    "image": "alpine:3.20",
    "cmd": ["sh","-c","echo hi-from-A && sleep 2 && uname -a"],
    "timeout_sec": 60,
    "labels": {
      "job": "demo",
      "target_node": "node-b"
    },
    "resources": {
      "nano_cpus": 500000000,
      "memory_bytes": 268435456
    }
  }'
```

## 5. Checking Results

```bash
curl http://<control-host>:8080/api/tasks
curl http://<control-host>:8080/api/tasks/<taskId>
```



# Local Multi-Node Demo

This document explains how to test the entire flow: Seeder → Control → Agent → Job Submitter in a local environment.

# 0. Running the Seeder Node

The Seeder node serves files to Agents.  
Start the Seeder with local bootstrap disabled (it will generate its own peer ID and listen address).

```bash
go run ./cmd/seeder \
  -ns mc \
  -root ./data \

  -listen /ip4/0.0.0.0/tcp/0
````

The Seeder will print output similar to:

```
seeder peer: 12D3KooWJ9sseudcqrkD1YeiuCtvxmG5UZn6TXfZM2HvyMv28GsK
seeder address: /ip4/127.0.0.1/tcp/36053/p2p/12D3KooWJ9sseudcqrkD1YeiuCtvxmG5UZn6TXfZM2HvyMv28GsK
```

Copy these values:

* The peer ID is used in `peer_id`
* The multi-address is used in `addrs`
* The file name inside `./data/` becomes the `root_cid` (for example `input.jpg`)

## 1. Running the Control Node

Use the exact multi-address output by the Seeder node as the bootstrap address.

```bash

# PostgreSQL DB Connection String (etc)
export MC_DB_DSN='postgres://mcuser:mcpw@127.0.0.1:5432/mc?sslmode=disable'

# Disable authentication
export MC_DISABLE_AUTH=1

# 0) Docker socket permissions
export DOCKER_HOST=unix:///var/run/docker.sock


export CTRL_BOOT="/ip4/127.0.0.1/tcp/37317/p2p/12D3KooWRR5VuMnELgFdjn6sH1RiriRGdX52JssN1hEdUTU8miG1"

MC_DISABLE_AUTH=1 \
go run ./cmd/control \
  -ns mc \
  -bootstrap "$CTRL_BOOT"
````

Note: Successful execution is confirmed when the Control server logs indicate it has joined the DHT.

## 2. Running the Agent Node

The Agent connects to the Control server, performs DHT discovery, and automatically claims registered jobs.

```bash
BOOTSTRAP="/ip4/127.0.0.1/tcp/37317/p2p/12D3KooWRR5VuMnELgFdjn6sH1RiriRGdX52JssN1hEdUTU8miG1"

go run ./cmd/agent \
  -ns mc \
  -control-url http://127.0.0.1:8080 \
  -auth-token dev \
  -bootstrap "$BOOTSTRAP"
```

## 3. Job Creation & Manifest Registration

### 3-1. Job Creation (Job ID Auto-Generated)

If the id field is omitted, the Control server automatically generates a job ID.

```bash
# Create a new job (server auto-generates the 'id' field if left empty)
TASK_JSON=$(curl -sS -X POST http://127.0.0.1:8080/api/tasks \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer dev' \
  -d "{
    \"image\": \"dpokidov/imagemagick:7.1.1-17-ubuntu\",
    \"command\": [\"convert\", \"input/input.jpg\", \"-colorspace\", \"Gray\", \"output_gray.jpg\"]
  }")

echo "$TASK_JSON" | jq

# Extract the generated JOB ID
JOB=$(echo "$TASK_JSON" | jq -r '.id')
echo "JOB ID: $JOB"
```

### 3-2. Manifest Registration (File Transfer Info from Seeder to Agent)

Register the file location and Seeder provider info so the Agent can fetch the input file.

```bash
SEEDER_PEER="12D3KooWJ9sseudcqrkD1YeiuCtvxmG5UZn6TXfZM2HvyMv28GsK"
SEEDER_ADDR="/ip4/127.0.0.1/tcp/36053/p2p/12D3KooWJ9sseudcqrkD1YeiuCtvxmG5UZn6TXfZM2HvyMv28GsK"

curl -s -X POST http://127.0.0.1:8080/jobs/$JOB/manifest \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer dev' \
  -d "{
    \"root_cid\": \"input.jpg\",
    \"providers\": [{
      \"peer_id\": \"$SEEDER_PEER\",
      \"addrs\": [\"$SEEDER_ADDR\"]
    }]
  }"
```

## 4. Checking Job Status

```bash
curl -s -H 'Authorization: Bearer dev' \
  http://127.0.0.1:8080/api/tasks/$JOB | jq
```

Note: The Agent logs should also include a message indicating that the job was successfully claimed.


