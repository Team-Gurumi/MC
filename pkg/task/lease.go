package task

import "time"

// Lease 키: task/<id>/lease
func KeyLease(id string) string {
	return "task/" + id + "/lease"
}

// 작업 선점(Claim) 레코드
type Lease struct {
	TaskID    string    `json:"taskId"`
	OwnerPeer string    `json:"ownerPeer"` // 선점한 에이전트 PeerID
	Nonce     string    `json:"nonce"`     // 충돌 방지용 랜덤 토큰
	ExpiresAt time.Time `json:"expiresAt"` // 이 시각 이후 만료
	UpdatedAt time.Time `json:"updatedAt"` // 최근 하트비트 시각
}

// TTL/하트비트 간격
const (
	DefaultLeaseTTL     = 15 * time.Second
	DefaultHeartbeatGap = 7 * time.Second
)
