package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	dhtnode "github.com/Team-Gurumi/MC/pkg/dht"
	"github.com/Team-Gurumi/MC/pkg/seeder"
)

func main() {
	ns := flag.String("ns", "default", "namespace")
	bootstrap := flag.String("bootstrap", "", "comma-separated bootstrap peers")
	baseDir := flag.String("base", "./inputs", "directory to seed files from (files named by root_cid)")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 부트스트랩 목록 파싱
	var boots []string
	if *bootstrap != "" {
		for _, s := range strings.Split(*bootstrap, ",") {
			s = strings.TrimSpace(s)
			if s != "" {
				boots = append(boots, s)
			}
		}
	}

	// DHT/libp2p 노드 시작
	node, err := dhtnode.NewNode(ctx, *ns, boots)
	if err != nil {
		log.Fatalf("[seeder] failed to create node: %v", err)
	}
	defer node.Close()

	// 파일 소스: inputs/ 아래에 root_cid와 동일한 파일명을 찾음
	src := seeder.SourceFS{Base: *baseDir}

	// P2P 핸들러 등록 (/mc-get/1.0.0)
	seeder.MountSeedHandler(ctx, node, src)

	// 정보 출력
	fmt.Println("[seeder] PeerID:", node.Host.ID())
	for _, a := range node.Multiaddrs() {
		fmt.Println("[seeder] addr:", a)
	}
	fmt.Printf("[seeder] serving files under %s (name == root_cid)\n", *baseDir)
	fmt.Println("[seeder] register this PeerID/addr in providers[] when posting manifest")

	// 유지
	t := time.NewTicker(5 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// keep alive
		}
	}
}
