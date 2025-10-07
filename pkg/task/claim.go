package task

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	dhtnode "github.com/Team-Gurumi/MC/pkg/dht"
)

var (
	ErrLeaseBusy   = errors.New("lease busy")
	ErrLeaseStolen = errors.New("lease stolen by another peer")
)

func randNonce() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// Claim: 리스가 없거나 만료된 경우에만 내가 선점
func Claim(d *dhtnode.Node, taskID, myPeer string, ttl time.Duration) (string, error) {
	if ttl <= 0 {
		ttl = DefaultLeaseTTL
	}
	nonce := randNonce()
	key := KeyLease(taskID)

	check := func(prev []byte) (bool, []byte, error) {
		now := time.Now().UTC()

		var old ClaimRecord
		if len(prev) > 0 {
			if err := json.Unmarshal(prev, &old); err != nil {
				old = ClaimRecord{}
			}
		}

		// 이전 리스가 유효하면 점유 중
		// 이전 리스가 유효하면 점유 중
if !old.Expires.IsZero() && old.Expires.After(now) && old.Owner != "" {
    return false, nil, nil
}

newL := ClaimRecord{
    TaskID:    taskID,
    Owner:     myPeer,
    Nonce:     nonce,
    Expires:   now.Add(ttl),
    UpdatedAt: now,
}
next, err := json.Marshal(newL)
        if err != nil {
            return false, nil, err
        }
        return true, next, nil
    } 

	if err := d.PutJSONCAS(key, check); err != nil {
		return "", err
	}
	return nonce, nil
}

// Heartbeat: 내가 점유 중일 때만 연장
func Heartbeat(d *dhtnode.Node, taskID, myPeer, myNonce string, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = DefaultLeaseTTL
	}
	key := KeyLease(taskID)
	return d.PutJSONCAS(key, func(prev []byte) (bool, []byte, error) {
    now := time.Now().UTC()
    var cur ClaimRecord // 타입을 ClaimRecord로 변경
    if len(prev) > 0 {
        if err := json.Unmarshal(prev, &cur); err != nil {
            return false, nil, err
        }
    }
    if cur.Owner != myPeer || cur.Nonce != myNonce { // Owner로 변경
        return false, nil, ErrLeaseStolen
    }
    cur.UpdatedAt = now // 이제 정상 동작
    cur.Expires = now.Add(ttl) // Expires로 변경
    next, _ := json.Marshal(cur)
    return true, next, nil
})

}

// Release: 삭제 대신 즉시 만료 표식
func Release(d *dhtnode.Node, taskID, myPeer, myNonce string) error {
	key := KeyLease(taskID)
	return d.PutJSONCAS(key, func(prev []byte) (bool, []byte, error) {
    now := time.Now().UTC()
    var cur ClaimRecord
    if len(prev) > 0 {
        if err := json.Unmarshal(prev, &cur); err != nil {
            return false, nil, err
        }
    }
    if cur.Owner != myPeer || cur.Nonce != myNonce { // Owner로 변경
        return false, nil, ErrLeaseStolen
    }
    cur.Expires = now.Add(-1 * time.Second) // Expires로 변경
    cur.UpdatedAt = now // 이제 정상 동작
    next, _ := json.Marshal(cur)
    return true, next, nil
})
}
