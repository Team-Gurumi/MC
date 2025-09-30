package dht

import (
	"encoding/json"
	"errors"
	"time"
)

// PutJSON: JSON 직렬화 후 5초 타임아웃으로 Put
func (n *Node) PutJSON(key string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	ctx, cancel := n.withTimeout(5 * time.Second)
	defer cancel()
	return n.DHT.PutValue(ctx, n.nsKey(key), b)
}

// GetJSON: 타임아웃 인자 포함 (호출부와 시그니처 일치)
func (n *Node) GetJSON(key string, out any, timeout time.Duration) error {
	ctx, cancel := n.withTimeout(timeout)
	defer cancel()
	val, err := n.DHT.GetValue(ctx, n.nsKey(key))
	if err != nil {
		return err
	}
	if len(val) == 0 {
		return errors.New("not found")
	}
	return json.Unmarshal(val, out)
}
