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

		var old Lease
		if len(prev) > 0 {
			if err := json.Unmarshal(prev, &old); err != nil {
				old = Lease{}
			}
		}

		// 이전 리스가 유효하면 점유 중
		if !old.ExpiresAt.IsZero() && old.ExpiresAt.After(now) && old.OwnerPeer != "" {
			return false, nil, nil
		}

		newL := Lease{
			TaskID:    taskID,
			OwnerPeer: myPeer,
			Nonce:     nonce,
			ExpiresAt: now.Add(ttl),
			UpdatedAt: now,
		}
		next, _ := json.Marshal(newL)
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
		var cur Lease
		if len(prev) > 0 {
			if err := json.Unmarshal(prev, &cur); err != nil {
				return false, nil, err
			}
		}
		if cur.OwnerPeer != myPeer || cur.Nonce != myNonce {
			return false, nil, ErrLeaseStolen
		}
		cur.UpdatedAt = now
		cur.ExpiresAt = now.Add(ttl)
		next, _ := json.Marshal(cur)
		return true, next, nil
	})
}

// Release: 삭제 대신 즉시 만료 표식
func Release(d *dhtnode.Node, taskID, myPeer, myNonce string) error {
	key := KeyLease(taskID)
	return d.PutJSONCAS(key, func(prev []byte) (bool, []byte, error) {
		now := time.Now().UTC()
		var cur Lease
		if len(prev) > 0 {
			if err := json.Unmarshal(prev, &cur); err != nil {
				return false, nil, err
			}
		}
		if cur.OwnerPeer != myPeer || cur.Nonce != myNonce {
			return false, nil, ErrLeaseStolen
		}
		cur.ExpiresAt = now.Add(-1 * time.Second) // 즉시 만료
		cur.UpdatedAt = now
		next, _ := json.Marshal(cur)
		return true, next, nil
	})
}
