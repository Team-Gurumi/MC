package main

import (
	"context"
	"fmt"
	"log"
	"time"

	mcdht "github.com/Team-Gurumi/MC/internal/dht"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ns := "mc"

	// 🔹 여기에서 원하는 키를 지정
	key := "ns/" + ns + "/task/index"
	// 예시:
	// key := "ad/mc/task/job-1762200160"
	// key := "p2p/job-1762200160/manifest"
	// key := "task/job-1762200160/state"

	client, err := mcdht.NewClient(ctx, mcdht.Config{
		Namespace: ns,
		Bootstrap: []string{
			"/ip4/127.0.0.1/tcp/44389/p2p/12D3KooWBViteBK8hFKSxeYPc5L2bkAKre8t3BtoTs9jg5Y7cvtb", // ← Control 실행 로그에서 나온 주소로 교체
		},
	})
	if err != nil {
		log.Fatalf("client init: %v", err)
	}

	var out map[string]any
	if err := client.GetJSON(ctx, key, &out); err != nil {
		log.Fatalf("get key: %v", err)
	}
	fmt.Printf("✅ DHT Key: %s\n", key)
	fmt.Printf("📦 Result: %+v\n", out)
}
