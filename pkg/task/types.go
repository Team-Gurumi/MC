package task

import "time"

// 1) 상태 타입을 먼저 선언
type TaskStatus string

// 상태값
const (
    StatusQueued    TaskStatus = "queued"
    StatusAssigned  TaskStatus = "assigned"
    StatusRunning   TaskStatus = "running"
     StatusSucceeded = "succeeded" 
StatusFinished TaskStatus = "finished" 
    StatusFailed    TaskStatus = "failed"
)

type Lease struct {
    Owner   string    `json:"owner"`
    Nonce   string    `json:"nonce"`
    Expires time.Time `json:"expires"`
    Version int64     `json:"version"` // 펜싱 카운터
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
    PeerID string   `json:"peer_id"`             
    Addrs  []string `json:"addrs"`                
    Relays []string `json:"relays,omitempty"`
    Caps   []string `json:"caps,omitempty"`       
}


// 작업 입력 좌표(순수 P2P)
type Manifest struct {
    RootCID    string     `json:"root_cid"`                 
    Providers  []Provider `json:"providers"`                
    EncMeta    string     `json:"enc_meta,omitempty"`       
    Rendezvous string     `json:"rendezvous,omitempty"`     
    Transports []string   `json:"transports,omitempty"`    
    Version    int64      `json:"version"`
    UpdatedAt  time.Time  `json:"updated_at"`

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
        CPUAvgPct    float64 `json:"cpu_avg_pct,omitempty"`   
        MemPeakBytes int64   `json:"mem_peak_bytes,omitempty"`
        WallSec      int     `json:"wall_sec,omitempty"`
    } `json:"metrics,omitempty"`

    LogTail string `json:"log_tail,omitempty"`
}


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

type TaskIndex struct {
    IDs       []string  `json:"ids"`
    UpdatedAt time.Time `json:"updated_at"`
    Version   int64     `json:"version"`
}

const IndexKey = "task/index"
func KeyIndex(ns string) string        { return "ns/" + ns + "/task/index" }
func KeyFinishMirror(id string) string { return "task/" + id + "/finish" }

func KeyLease(id string) string { return "task/" + id + "/lease" }


