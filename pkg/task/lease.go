package task

import "time"

type ClaimRecord struct {
    TaskID    string    `json:"taskId"`
    Owner     string    `json:"owner"`     
    Nonce     string    `json:"nonce"`
    Expires   time.Time `json:"expires"`   
    UpdatedAt time.Time `json:"updatedAt"`
}


// TTL/하트비트 간격
const (
	DefaultLeaseTTL     = 15 * time.Second
	DefaultHeartbeatGap = 7 * time.Second
)
