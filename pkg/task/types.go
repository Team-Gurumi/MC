package task

import "time"

type TaskMeta struct {
	ID        string    `json:"id"`
	Image     string    `json:"image"`
	Command   []string  `json:"command"`
	CreatedAt time.Time `json:"createdAt"`
}

type TaskStatus string

const (
	StatusQueued   TaskStatus = "queued"
	StatusAssigned TaskStatus = "assigned"
	StatusRunning  TaskStatus = "running"
	StatusFinished TaskStatus = "finished"
	StatusFailed   TaskStatus = "failed"
)

type TaskState struct {
	ID         string     `json:"id"`
	Status     TaskStatus `json:"status"`
	AssignedTo string     `json:"assignedTo,omitempty"` // PeerID
	UpdatedAt  time.Time  `json:"updatedAt"`
	StartedAt  *time.Time `json:"startedAt,omitempty"`
	FinishedAt *time.Time `json:"finishedAt,omitempty"`
	ExitCode   *int       `json:"exitCode,omitempty"`
	Error      string     `json:"error,omitempty"`
	LogURL     string     `json:"logUrl,omitempty"`
	Version    int64      `json:"version"` // 아이템포턴트/순서 보장을 위한 단조 증가값
}

// WS/QUIC 엔드포인트 표준 포맷
type TaskEndpoint struct {
	TaskID   string    `json:"taskId"`
	Proto    string    `json:"proto"`    // "ws" | "wss" | "quic"
	Endpoint string    `json:"endpoint"` // 예: "wss://agent.example/ws/abc" 또는 libp2p 멀티addr
	Updated  time.Time `json:"updated"`
}

func KeyMeta(id string) string  { return "task/" + id + "/meta" }
func KeyState(id string) string { return "task/" + id + "/state" }
func KeyWS(id string) string    { return "task/" + id + "/ws" }

// 전역 인덱스 (CAS용 Version 포함)
type TaskIndex struct {
	IDs       []string  `json:"ids"`
	UpdatedAt time.Time `json:"updatedAt"`
	Version   int64     `json:"version"`
}

const IndexKey = "task/index"
