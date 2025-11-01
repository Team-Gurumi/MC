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

const DefaultLeaseTTL = 20 * time.Second

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
				// 깨진 값이면 새로 쓰도록 한다
				old = Lease{}
			}
		}

		// 이전 리스가 아직 살아 있으면 점유 중
		if old.Owner != "" && !old.Expires.IsZero() && old.Expires.After(now) {
			return false, nil, ErrLeaseBusy
		}

		newL := Lease{
			Owner:   myPeer,
			Nonce:   nonce,
			Expires: now.Add(ttl),
			Version: old.Version + 1, // 펜싱 토큰
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

		var cur Lease
		if len(prev) == 0 {
			// 리스가 없으면 갱신 안 함
			return false, nil, nil
		}
		if err := json.Unmarshal(prev, &cur); err != nil {
			return false, nil, err
		}

		// 내 리스가 아니면 연장 금지
		if cur.Owner != myPeer || cur.Nonce != myNonce {
			return false, nil, ErrLeaseStolen
		}

	
		cur.Expires = now.Add(ttl)
		cur.Version++

		next, _ := json.Marshal(cur)
		return true, next, nil
	})
}

// Release: 내가 가진 리스를 의도적으로 내려놓기
func Release(d *dhtnode.Node, taskID, myPeer, myNonce string) error {
	key := KeyLease(taskID)
	return d.PutJSONCAS(key, func(prev []byte) (bool, []byte, error) {
		var cur Lease
		if len(prev) == 0 {
			return false, nil, nil
		}
		if err := json.Unmarshal(prev, &cur); err != nil {
			return false, nil, err
		}
		if cur.Owner != myPeer || cur.Nonce != myNonce {
			return false, nil, ErrLeaseStolen
		}

		cur.Owner = ""
		cur.Nonce = ""
		cur.Expires = time.Time{}
		cur.Version++

		next, _ := json.Marshal(cur)
		return true, next, nil
	})
}

