package task

import "time"

// 1) 상태 타입을 먼저 선언
type TaskStatus string

// 상태값
const (
    StatusQueued    TaskStatus = "queued"
    StatusAssigned  TaskStatus = "assigned"
    StatusRunning   TaskStatus = "running"
StatusFinished TaskStatus = "finished" 
    StatusFailed    TaskStatus = "failed"
)

type Lease struct {
    Owner   string    `json:"owner"`
    Nonce   string    `json:"nonce"`
    Expires time.Time `json:"expires"` // UTC 권장
}

type Artifact struct {
    Name string `json:"name,omitempty"`
    Size int64  `json:"size,omitempty"`
    CID  string `json:"cid,omitempty"`
    URL  string `json:"url,omitempty"` // optional gateway/proxy
}

type TaskMeta struct {
ID         string    `json:"id,omitempty"`
    CreatedAt  time.Time `json:"created_at,omitempty"`
    Image      string   `json:"image"`
    Command    []string `json:"command"`
    Env        []string `json:"env,omitempty"`
    Workdir    string   `json:"workdir,omitempty"`
    TimeoutSec int      `json:"timeout_sec,omitempty"`
    Resources struct {
        CPU    string `json:"cpu,omitempty"`
        Memory string `json:"memory,omitempty"`
    } `json:"resources,omitempty"`
}
type Provider struct {
    PeerID string   `json:"peer_id"`              // 12D3Koo... 형태
    Addrs  []string `json:"addrs"`                // libp2p 멀티어드레스들 (/ip4/.../udp/.../quic-v1 등)
    Relays []string `json:"relays,omitempty"`     // (옵션) 미리 붙은 릴레이 주소들
    Caps   []string `json:"caps,omitempty"`       // (옵션) "mc-get/1.0.0", "graphsync", "bitswap" 등
}


// 작업 입력 좌표(순수 P2P)
type Manifest struct {
    RootCID    string     `json:"root_cid"`                 // Merkle-DAG root
    Providers  []Provider `json:"providers"`                // 시더 피어 좌표들
    EncMeta    string     `json:"enc_meta,omitempty"`       // AES-GCM 메타(Agent만 복호화)
    Rendezvous string     `json:"rendezvous,omitempty"`     // DHT 토픽/룸
    Transports []string   `json:"transports,omitempty"`     // 예: ["quic-v1","webrtc"]
    Version    int64      `json:"version"`
    UpdatedAt  time.Time  `json:"updated_at"`

    // ↓ 임시 호환 필드: 기존 코드가 참조 중이면 컴파일 유지용(실제 사용 금지)
    //    다음 단계에서 핸들러/I-O를 모두 providers 기반으로 바꾼 뒤 제거.
    Seeders []string `json:"seeders,omitempty"` //  사용금지
}

type TaskState struct {
    ID        string     `json:"id"`
    Status    TaskStatus `json:"status"`
    Version   int64      `json:"version"`
    UpdatedAt time.Time  `json:"updated_at"`

AssignedTo  string     `json:"assigned_to,omitempty"`
    Lease      *Lease     `json:"lease,omitempty"`
    StartedAt  *time.Time `json:"started_at,omitempty"`
    FinishedAt *time.Time `json:"finished_at,omitempty"`

    ExitCode      *int       `json:"exit_code,omitempty"`
    Error         string     `json:"error,omitempty"`
    ResultRootCID string     `json:"result_root_cid,omitempty"`
    Artifacts     []Artifact `json:"artifacts,omitempty"`
    Metrics struct {
        CPUAvgPct    float64 `json:"cpu_avg_pct,omitempty"`   // 0.0~100.0
        MemPeakBytes int64   `json:"mem_peak_bytes,omitempty"`
        WallSec      int     `json:"wall_sec,omitempty"`
    } `json:"metrics,omitempty"`

    LogTail string `json:"log_tail,omitempty"`
}

// WS/QUIC endpoint (snake_case로 통일)
type TaskEndpoint struct {
    TaskID   string    `json:"task_id"`
    Proto    string    `json:"proto"`    // "ws" | "wss" | "quic"
    Endpoint string    `json:"endpoint"` // e.g. wss://... or multiaddr
    Updated  time.Time `json:"updated"`
}

func KeyMeta(id string) string     { return "task/" + id + "/meta" }
func KeyState(id string) string    { return "task/" + id + "/state" }
func KeyWS(id string) string       { return "task/" + id + "/ws" }
func KeyManifest(id string) string { return "task/" + id + "/manifest" }

// 전역 인덱스 (CAS용 Version 포함)
type TaskIndex struct {
    IDs       []string  `json:"ids"`
    UpdatedAt time.Time `json:"updated_at"`
    Version   int64     `json:"version"`
}

const IndexKey = "task/index"
func KeyIndex(ns string) string        { return "ns/" + ns + "/task/index" }
func KeyFinishMirror(id string) string { return "task/" + id + "/finish" }

