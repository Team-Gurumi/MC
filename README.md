

# 🌀 Mutual Cloud (MC)

### A Decentralized Control Plane for SPOF-Free Distributed Task Execution

*Research-driven system design with a fully working distributed implementation*

![System Diagram](./Report/images/infographic1.png)

Mutual Cloud (MC) is a **P2P-based distributed task execution framework** that eliminates the structural limitations of centralized schedulers such as Kubernetes, Nomad, and Ray.

We show that **task orchestration can be fully decentralized**—
no leader election, no global state sync, no control-plane bottleneck—
while maintaining **correctness, fault tolerance, and high scalability**.

MC is both:

* 💡 **A research project** (theory, motivation, experiments, contribution)
* 🧱 **A full working system** (real DHT, real containers, real agents)

---

# 1. Motivation

Traditional orchestrators rely on **logical centralization**:

* Leader election → unavoidable blackout
* Global synchronization → high tail latency
* Central writes → scalability ceilings

Even HA Kubernetes clusters cannot escape these constraints.

Our research question:

> **Can a distributed execution system function with *no logical center* while still ensuring correctness and availability?**

Mutual Cloud is our answer—backed by a real implementation and reproducible experiment results.

---

# 2. Research Contributions

### 1) *Decentralized control plane via libp2p Kad-DHT*

Metadata is stored in the DHT; no scheduler, no central cluster state.

### 2) *Agent-driven autonomous scheduling*

Agents proactively search, claim, fetch, execute, and finalize tasks.

### 3) *Correctness through Atomic Lease + Fencing Tokens*

Ensures safe execution and prevents duplicate or stale work.

### 4) *P2P-based artifact distribution*

Input datasets are fetched directly via multiaddrs.

### 5) *Extensive evaluation under heavy failure*

We validated MC under 50–500 nodes and up to 60% agent failures.

---

# 3. Architecture Overview

![Diagram](./Report/images/diagramEg.png)

## 📚 Layered Components



![Upper](./Report/images/upper_layer.png)
![Middle](./Report/images/middle_layer.png)
![Lower](./Report/images/lower_layer.png)

---

## Components

| Component      | Description                                                               |
| -------------- | ------------------------------------------------------------------------- |
| **Control**    | Stateless task entrypoint, publishes metadata to PostgreSQL + DHT.        |
| **Agent**      | Discovers tasks, acquires leases, fetches artifacts, executes containers. |
| **Seeder**     | Provides input datasets via libp2p P2P transport.                         |
| **PostgreSQL** | Durable backing store for lease verification.                             |
| **Kad-DHT**    | Decentralized index for discovery and coordination.                       |

---

# 4.Design Highlights

### 🔍 DHT-Based Task Discovery

Control publishes:

* `task/{id}/state`
* `task/{id}/manifest`

Agents query namespace indices:

* `task-index/{ns}`

**Why it matters:** enables *fully decentralized scheduling* with O(log N) lookup cost.

---

### 🔐 Atomic Lease + Fencing Token

Guarantees:

* Single legitimate executor
* No zombie re-claims
* Safe failover

**Why it matters:** ensures *strong consistency* even under churn or partial failures.

---

### 🔄 Self-Healing Execution

If an agent:

* crashes
* loses heartbeat
* or lease expires

→ work is instantly reassigned **without any central coordinator**.

**Why it matters:** enables *millisecond-level failover* and no blackout periods.

---

### 📦 P2P Artifact Delivery

Agents fetch data via:

```
/ip4/<host>/tcp/<port>/p2p/<peer-id>
```

**Why it matters:** avoids central bottlenecks; ideal for edge or federated clusters.

---

# 5. Experimental Results

MC was evaluated to answer a central question: Can a fully decentralized execution system outperform HA leader-based systems in reliability and recovery speed?

MC was evaluated under failure-heavy and large-scale environments.



##  SPOF Test — Control Node Kill
<p align="center">
  <img src="https://github.com/user-attachments/assets/e66a9abf-addc-4f8c-b890-f9ab83410bfd" width="480"/>
  <br/>
  <em>Figure 1. Control node kill experiment (30s outage, 0 task loss)</em>
</p>

* Control killed for **30 seconds**
* MC agents continued execution **with 0 interruption**
* Kubernetes HA requires:

  * 3–8s leader election
  * 10–35s pod recovery

---

## Failure Ratio Test (10 → 60% agent kill)
<a href="https://github.com/user-attachments/assets/0e304e1e-1650-473f-ad7e-93c91f953451">
  <img src="https://github.com/user-attachments/assets/0e304e1e-1650-473f-ad7e-93c91f953451" width="450"/>
</a>

* 100% task completion
* p95 MTTR ≈ **0.02s**
* No duplicate execution

---

##  Scalability Test (50 → 500 agents)
<p align="center">
  <img src="https://github.com/user-attachments/assets/72a07d0e-f5f2-4726-97f6-c1a3bb0c169b" width="480"/>
  <br/>
</p>

* Millisecond-level reassignment
* No central bottleneck
---

##  Summary Table

| Experiment                  | Result                                      |
| --------------------------- | ------------------------------------------- |
| **Control node kill (30s)** | *0 task loss*, uninterrupted execution      |
| **Agent failure 60%**       | 100% completion, p95 MTTR ≈ 0.02s           |
| **Scalability (50 → 500)**  | Millisecond-class reassignment              |

> **Key Insight:**
> *A fully decentralized architecture can outperform centralized HA systems in both recovery latency and scalability.*

---

# 6. Quickstart (Minimal Demo)

### Run system

```bash
git clone https://github.com/team-gurumi/mc
cd mc
docker-compose up -d
```

### Expected output (sample)

```
✔ control started
✔ seeder started
✔ agent started
```

### Submit a task

```bash
curl -X POST http://localhost:8080/api/tasks \
  -H "Content-Type: application/json" \
  -d '{"image":"busybox","command":["echo","hello from MC"]}'
```

### List tasks

```bash
curl http://localhost:8080/api/tasks
```
---

# 7. Requirements

* Go 1.21+
* Docker / Kata Runtime
* PostgreSQL
* systemd (for remote install scripts)

---

# 8. Remote Installation & Execution


### Install Control

```bash
make install-control HOST=ubuntu@control-box PORT=8080 TIMEOUT=180
```

### Install Agent

```bash
make install-agent HOST=ubuntu@node-1 CONTROL_URL=http://100.70.1.2:8080
```

With Kata:

```bash
make install-agent HOST=ubuntu@B CONTROL_URL=http://100.70.1.2:8080 DOCKER_RUNTIME=kata-runtime
```

### Logs

```bash
make agent-restart HOST=ubuntu@node-1
make logs-agent    HOST=ubuntu@node-1
```


---

# 9. Local Multi-Node Demo


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

# 10. Limitations & Future Work

* Research prototype; not production-hardened
* No RBAC / multi-tenancy
* DHT churn optimization
* Full DAG scheduler planned
* Multi-DHT federation & malicious-node detection
---

# 🏁 Closing Note

Mutual Cloud demonstrates:

* ✔ Failover without leader election
* ✔ Correctness without global synchronization
* ✔ Scalability without a central scheduler

It shows that **decentralized control-plane architectures are not only theoretically sound, but practically implementable and empirically superior in failure-heavy environments.**
---
