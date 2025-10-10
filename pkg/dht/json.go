package dht

import (
	"encoding/json"
	"strings"
	"sync"
	"time"
)

// 패키지 전역 로컬 스토어 (데모/싱글노드 폴백용)
var localStore sync.Map // key(string nsKey) -> []byte

// PutJSON: JSON 직렬화 후 5초 타임아웃으로 Put.
// 네트워크 PutValue 실패(피어 0 등) 시 localStore로 폴백.
func (n *Node) PutJSON(key string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	ctx, cancel := n.withTimeout(5 * time.Second) // withTimeout은 node.go에 이미 있음
	defer cancel()

	if err := n.DHT.PutValue(ctx, n.nsKey(key), b); err != nil {
		// 싱글노드/피어 없음 → 로컬 저장 폴백
		if strings.Contains(err.Error(), "failed to find any peer in table") {
			localStore.Store(n.nsKey(key), b)
			return nil
		}
		return err
	}
	return nil
}

// GetJSON: 네트워크 GetValue 실패 시 localStore 폴백.
func (n *Node) GetJSON(key string, out any, timeout time.Duration) error {
    ctx, cancel := n.withTimeout(timeout)
    defer cancel()

    var data []byte
    v, err := n.DHT.GetValue(ctx, n.nsKey(key))
    if err != nil {
        if lv, ok := localStore.Load(n.nsKey(key)); ok {
            data = lv.([]byte)
        } else {
            return err
        }
   } else {
        data = v
    }
    return json.Unmarshal(data, out)
}
