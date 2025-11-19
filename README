# 프로젝트 구조

team-gurumi/mc/
├── Makefile # 빌드 및 설치 명령 정의
├── go.mod / go.sum # Go 모듈 의존성 관리
├── run_experiments1.sh # 실험 실행 스크립트
├── run_fault_tolerance_benchmark.sh
├── analyze_metrics.py # 실험 결과 분석 스크립트
├── agent / control / seeder # 컴파일된 실행 바이너리
│
├── cmd/ # 실행 진입점 (Main Applications)
│ ├── agent/ # 에이전트 노드 실행 진입점 - 작업 실행·상태 관리
│ ├── control/ # 컨트롤 서버 실행 진입점 - 중앙 관리 및 API 서버
│ ├── seeder/ # 시더 노드 실행 진입점 - P2P로 파일 전송
│ └── dhtget/ # DHT 네트워크 데이터 조회 유틸리티 (디버깅용)
│
├── pkg/ # 공통 라이브러리 (Core Packages)
│ ├── agent/ # 에이전트 로직: 작업 실행, 리스 관리, 상태 보고
│ ├── dht/ # DHT 노드 및 XOR 라우팅 구현
│ ├── p2p/ # 피어 간 P2P 통신 프로토콜
│ ├── task/ # 작업(Task) 데이터 구조 및 자원 점유 로직
│ ├── demand/ # PostgreSQL 기반 작업(Job) 저장 및 관리
│ └── seeder/ # 시더 요청 처리 및 데이터 분배 로직
│
├── Report/ # 프로젝트 보고서 및 다이어그램
│ ├── 09-구르미-1차보고서-금채원.pdf
│ ├── 09-구르미-2차보고서.md
│ └── images/ # 보고서용 다이어그램, 그래프 이미지
│
├── experiment_logs/ # 실험 결과 로그 파일
│ ├── exp2_agents_100.log
│ └── exp3_kill_10.log
│
└── vendor/ # 외부 의존성 모듈 (Go vendor)

- **cmd/** — 프로젝트의 실행 진입점으로, 각 하위 폴더가 독립 실행 프로그램  
- **pkg/** — `cmd`에서 공통으로 사용하는 핵심 로직 패키지  
- **Report/** — 보고서 및 이미지 리소스 저장 폴더  
- **experiment_logs/** — 실험 로그 및 결과 기록  
- **vendor/** — 외부 Go 패키지 의존성  

Postgres SQL이 꼭 설치되어 있어야 함.

실험 스크립트 실행할 때 반드시

```
# PostgreSQL DB Connection String (etc)
export MC_DB_DSN='postgres://mcuser:mcpw@127.0.0.1:5432/mc?sslmode=disable'

# Disable authentication
export MC_DISABLE_AUTH=1

# 0) Docker socket permissions
export DOCKER_HOST=unix:///var/run/docker.sock
```
하고 실행시키기.


#서버에 Go, Docker, systemd 는 이미 설치되어 있어야 함

#설치
# 컨트롤 서버 설치(원격)
make install-control HOST=ubuntu@control-box PORT=8080 TIMEOUT=180

# 에이전트 설치(원격) - 컨트롤 서버 주소 넘김(로컬/테일스케일 IP)
make install-agent  HOST=ubuntu@node-1 CONTROL_URL=http://100.70.1.2:8080
# 작업 지정할 노드 (에이전트) - 같은 컨트롤에 붙이고 kata 사용
make install-agent  HOST=ubuntu@B CONTROL_URL=http://100.70.1.2:8080 DOCKER_RUNTIME=kata-runtime

# 이후 재시작/로그
make agent-restart  HOST=ubuntu@node-1
make logs-agent     HOST=ubuntu@node-1

# 작업 처리할 agent의 NODE_ID 확인
ssh ubuntu@B 'hostname'      # 예: "node-b"
# 또는
ssh ubuntu@B 'systemctl show -p Environment mc-agent'

# 사용자가 작업 push (target_node 지정)
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

# 결과 확인
curl http://<control-host>:8080/api/tasks
curl http://<control-host>:8080/api/tasks/<taskId>



````md
# Local Multi-Node Demo

This document explains how to test the entire flow: Seeder → Control → Agent → Job Submitter in a local environment.

# 1. Running the Control Node

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

# 2. Running the Agent Node

The Agent connects to the Control server, performs DHT discovery, and automatically claims registered jobs.

```bash
BOOTSTRAP="/ip4/127.0.0.1/tcp/37317/p2p/12D3KooWRR5VuMnELgFdjn6sH1RiriRGdX52JssN1hEdUTU8miG1"

go run ./cmd/agent \
  -ns mc \
  -control-url http://127.0.0.1:8080 \
  -auth-token dev \
  -bootstrap "$BOOTSTRAP"
```

# 3. Job Creation & Manifest Registration

## 3-1. Job Creation (Job ID Auto-Generated)

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

## 3-2. Manifest Registration (File Transfer Info from Seeder to Agent)

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

# 4. Checking Job Status

```bash
curl -s -H 'Authorization: Bearer dev' \
  http://127.0.0.1:8080/api/tasks/$JOB | jq
```

Note: The Agent logs should also include a message indicating that the job was successfully claimed.


