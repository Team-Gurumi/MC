package main

import (
	"context"
	"fmt"
	"log"
	"time"

	dhtnode "github.com/Team-Gurumi/MC/pkg/dht"
)

func main() {
	// NewNode 만들 때만 ctx 씁니다
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ns := "mc"

	boots := []string{
		// 컨트롤 로그에서 나온 멀티어드레스 붙이세요
		"/ip4/127.0.0.1/tcp/44389/p2p/12D3KooWBViteBK8hFKSxeYPc5L2bkAKre8t3BtoTs9jg5Y7cvtb",
	}

	// 레포에서 seeder가 쓰는 그대로
	node, err := dhtnode.NewNode(ctx, ns, boots)
	if err != nil {
		log.Fatalf("dht new node: %v", err)
	}

	key := "ns/" + ns + "/task/index"

	var out map[string]any

	// ✅ 여기! (key, out, ttl) 순서
	if err := node.GetJSON(key, &out, 5*time.Second); err != nil {
		log.Fatalf("dht get %s: %v", key, err)
	}

	fmt.Printf("✅ key = %s\n", key)
	fmt.Printf("📦 result = %+v\n", out)
}

