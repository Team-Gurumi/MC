package task

import "time"

type ClaimRecord struct { // 이름을 변경
    TaskID    string    `json:"taskId"`
    Owner     string    `json:"owner"`     
    Nonce     string    `json:"nonce"`
    Expires   time.Time `json:"expires"`   
    UpdatedAt time.Time `json:"updatedAt"`
}

func KeyLease(id string) string { return "task/" + id + "/lease" }
// TTL/하트비트 간격
const (
	DefaultLeaseTTL     = 15 * time.Second
	DefaultHeartbeatGap = 7 * time.Second
)
