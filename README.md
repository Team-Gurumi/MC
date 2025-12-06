

# 🌀 MC — Mutual Cloud

### A P2P Decentralized Control Plane Framework for SPOF-Resilient Distributed Task Execution

![System Diagram](./Report/images/infographic1.png)

Mutual Cloud (MC) is a **lightweight distributed task execution platform** that removes the inherent limitations of centralized schedulers such as Kubernetes and Nomad.
MC eliminates Single Points of Failure (SPOF) by decentralizing resource discovery, task claiming, and failure recovery using a peer-to-peer control plane.

MC is built on:

* **libp2p Kad-DHT** for decentralized discovery & coordination
* **PostgreSQL** for durable task state
* **Docker** (or Kata) for containerized execution

It is composed of three main runtime components:

* **`cmd/control`** — Stateless control server: HTTP API + job store + DHT announcer
* **`cmd/agent`** — Worker node: discovers tasks via DHT, claims leases, executes containers
* **`cmd/seeder`** — Seeder node: provides input files to agents via P2P transport

---

# ✨ Core Ideas

## 1. Decentralized Orchestration

* Control server *does not schedule tasks*.
* Agents autonomously discover executable tasks by querying the DHT.

## 2. DHT-Based Discovery

* Task manifests and state are published into a Distributed Hash Table.
* Agents query `task-index/{ns}` to find pending tasks.

## 3. Atomic Claim & Lease

* Agents claim tasks via a CAS-based lease protocol (`try-claim`).
* Fencing tokens prevent duplicate/stale execution.

## 4. P2P Artifact Delivery

* Seeder exposes input files via libp2p multiaddrs.
* Agents fetch them directly without involving the Control server.

## 5. Self-Healing Execution

* If an Agent dies or loses its lease, tasks re-enter the pending set and are reclaimed by others.

---

# 🧩 Architecture Overview


![System Diagram](./Report/images/diagramEg.png)

![System Diagram](./Report/images/upper_layer.png)
![System Diagram](./Report/images/middle_layer.png)
![System Diagram](./Report/images/lower_layer.png)


### High-level flow

1. User submits a task via HTTP → written to PostgreSQL
2. Control publishes metadata to DHT (`task/{id}/state`, `task/{id}/manifest`)
3. Agents discover tasks through Kad-DHT
4. Agent atomically acquires lease → fetches input files from Seeder
5. Agent executes container → streams logs → reports completion
6. If failure occurs, task is instantly recoverable by other Agents

### Key code locations

* **`cmd/control/control_http.go`** — task creation, try-claim, finish, manifest
* **`cmd/agent/main.go`** — discover → claim → fetch → run → report
* **`pkg/task/types.go`** — task & manifest type definitions, DHT key builders
* **`pkg/dht/node.go`** — libp2p + Kad-DHT bootstrap
* **`pkg/agent`** — container execution, fetch logic

---

# 📊 Experimental Results

MC has been evaluated under heavy failure and large-scale scenarios.

### **SPOF Test — Control Node Kill**

* Control server killed for 30 seconds
* Agents continued executing tasks with **zero interruption**
* Kubernetes (even HA mode) introduces blackout via leader election (3–8s) + pod recovery (10–35s)

### **Failure Ratio Test (10% → 60% agent kill)**

* Success rate maintained at **100%**
* p95 MTTR ≈ **0.02s**
* No duplicate execution due to fencing tokens

### **Scalability Test (50 → 500 agents)**

* MTTR remained in **millisecond range**
* No central bottleneck
* Kubernetes unable to scale past ~200 agents in identical conditions

### **Recovery Speed**

MC achieved:

* **96% faster recovery** than Kubernetes
* Up to **400× faster resumption time**
* **100% availability** even when 60% of nodes failed simultaneously

---

# 📁 Project Structure

```
team-gurumi/mc/
├── Makefile
├── go.mod / go.sum
├── run_experiments1.sh
├── run_fault_tolerance_benchmark.sh
├── analyze_metrics.py
├── agent / control / seeder           # Compiled binaries (to be removed in future cleanup)
│
├── cmd/
│ ├── agent/
│ ├── control/
│ ├── seeder/
│ └── dhtget/
│
├── pkg/
│ ├── agent/
│ ├── dht/
│ ├── p2p/
│ ├── task/
│ ├── demand/
│ └── seeder/
│
├── Report/
│ ├── 09-구르미-1차보고서-금채원.pdf
│ ├── 09-구르미-2차보고서.md
│ └── images/
│
├── experiment_logs/
│ ├── exp2_agents_100.log
│ └── exp3_kill_10.log
│
└── vendor/
```

---

# ⚙️ Requirements

* PostgreSQL
* Docker or Kata Runtime
* Go 1.21+
* systemd (for remote installation scripts)

### Required Environment Variables

```bash
export MC_DB_DSN='postgres://mcuser:mcpw@127.0.0.1:5432/mc?sslmode=disable'
export MC_DISABLE_AUTH=1
export DOCKER_HOST=unix:///var/run/docker.sock
```

---

# 🌐 Remote Installation Guide

## Install Control Node

```bash
make install-control HOST=ubuntu@control-box PORT=8080 TIMEOUT=180
```

## Install Agent Nodes

```bash
make install-agent HOST=ubuntu@node-1 CONTROL_URL=http://100.70.1.2:8080
```

With Kata runtime:

```bash
make install-agent HOST=ubuntu@B CONTROL_URL=http://100.70.1.2:8080 DOCKER_RUNTIME=kata-runtime
```

## Logs & Service Control

```bash
make agent-restart HOST=ubuntu@node-1
make logs-agent    HOST=ubuntu@node-1
```

---

# 🧪 Local Multi-Node Demo

The following walkthrough launches Seeder → Control → Agent fully on localhost.

## 0. Start Seeder

```bash
go run ./cmd/seeder \
  -ns mc \
  -root ./data \
  -listen /ip4/0.0.0.0/tcp/0
```

Copy:

* Seeder Peer ID
* Seeder Multiaddr
* File name = `root_cid`

## 1. Start Control

```bash
export MC_DB_DSN='postgres://mcuser:mcpw@127.0.0.1:5432/mc?sslmode=disable'
export MC_DISABLE_AUTH=1
export DOCKER_HOST=unix:///var/run/docker.sock

MC_DISABLE_AUTH=1 \
go run ./cmd/control \
  -ns mc \
  -bootstrap "$CTRL_BOOT"
```

## 2. Start Agent

```bash
go run ./cmd/agent \
  -ns mc \
  -control-url http://127.0.0.1:8080 \
  -auth-token dev \
  -bootstrap "$BOOTSTRAP"
```

## 3. Create Job

```bash
TASK_JSON=$(curl -sS -X POST http://127.0.0.1:8080/api/tasks \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer dev' \
  -d '{
    "image": "dpokidov/imagemagick:7.1.1-17-ubuntu",
    "command": ["convert","input/input.jpg","-colorspace","Gray","output_gray.jpg"]
  }')
```

Extract job ID:

```bash
JOB=$(echo "$TASK_JSON" | jq -r '.id')
```

## 3-2. Register Manifest

```bash
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

## 4. Check Task Status

```bash
curl -s \
  -H 'Authorization: Bearer dev' \
  http://127.0.0.1:8080/api/tasks/$JOB | jq
```

---

# 🔚 Summary

Mutual Cloud brings:

* **SPOF-free task orchestration**
* **Fast, autonomous failure recovery**
* **DHT-based discovery instead of centralized scheduling**
* **P2P data distribution**
* **Scalability to hundreds of agents without control-plane load**

It demonstrates an alternative model to conventional orchestrators, suitable for research in **edge computing, federated clusters, and decentralized control planes**
 
 
 
 
 
