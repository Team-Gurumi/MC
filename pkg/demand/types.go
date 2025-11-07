package demand

import "time"

// 사용자가 POST /api/tasks 로 넘긴 메타 + 생성시각
type JobMeta struct {
	ID        string    `json:"id"`
	Image     string    `json:"image"`
	Command   []string  `json:"command"`
	CreatedAt time.Time `json:"createdAt"`
		RetryCount int
}

// 현재 상태 (네가 이미 응답에서 보고 있는 필드들)
type JobState struct {
	ID        string    `json:"id"`
	Status    string    `json:"status"` // queued|assigned|running|finished|failed
	UpdatedAt time.Time `json:"updatedAt"`
	Version   int64     `json:"version"`
	// 아래 3개는 다음 단계에서 쓸 예정(지금은 비워둬도 OK)
	AssignedTo     string    `json:"assignedTo,omitempty"`
	LeaseExpiresAt time.Time `json:"leaseExpiresAt,omitempty"`
	FencingToken   string    `json:"fencingToken,omitempty"`
}

// 최종 결과 스키마 (지금은 아직 저장 안 하지만 타입만 준비)
type MetricKV map[string]any

type Artifact struct {
	Name string `json:"name"`
	CID  string `json:"cid"`
}

type JobResult struct {
	Status        string    `json:"status"` // succeeded|failed
	FinalMetrics  MetricKV  `json:"final_metrics,omitempty"`
	ResultRootCID string    `json:"result_root_cid,omitempty"`
	Artifacts     []Artifact `json:"artifacts,omitempty"`
}

// 전체 Job 뷰
type Job struct {
	Meta   *JobMeta  `json:"meta"`
	State  *JobState `json:"state"`
	Result *JobResult `json:"result,omitempty"`
}
