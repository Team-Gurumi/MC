package agent

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	dhtnode "github.com/Team-Gurumi/MC/pkg/dht"
	"github.com/Team-Gurumi/MC/pkg/p2p"
	"github.com/Team-Gurumi/MC/pkg/task"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

type FetchResult struct {
	LocalPath string
	Bytes     int64
}

// 한 provider로 시도. 성공하면 파일 경로/바이트 수 반환.
func FetchFromProvider(ctx context.Context, d *dhtnode.Node, rootCID string, pv task.Provider, dir string) (*FetchResult, error) {
	if pv.PeerID == "" || len(pv.Addrs) == 0 {
		return nil, fmt.Errorf("invalid provider")
	}
	// 1) PeerInfo 구성
	var addrs []ma.Multiaddr
	for _, s := range pv.Addrs {
		m, err := ma.NewMultiaddr(s); if err != nil { continue }
		addrs = append(addrs, m)
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("no valid addrs")
	}
	pi, err := peer.AddrInfoFromP2pAddr(addrs[0].Encapsulate(ma.StringCast("/p2p/"+pv.PeerID)))
	if err != nil {
		return nil, fmt.Errorf("peer info: %w", err)
	}

	// 2) Connect (짧은 타임아웃)
	ctx2, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	if err := d.Host.Connect(ctx2, *pi); err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}

	// 3) 스트림 오픈
	s, err := d.Host.NewStream(ctx, pi.ID, p2p.ProtoGet)
	if err != nil {
		return nil, fmt.Errorf("new stream: %w", err)
	}
	defer s.Close()

	// 4) 요청 전송
	req := p2p.GetRequest{RootCID: rootCID}
	if err := json.NewEncoder(s).Encode(&req); err != nil {
		return nil, fmt.Errorf("send req: %w", err)
	}

	// 5) 파일 저장 경로
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	tmp := filepath.Join(dir, rootCID+".part")
	out, err := os.Create(tmp)
	if err != nil {
		return nil, err
	}
	defer out.Close()

	// 6) 바디 스트리밍 복사
	nw, err := io.Copy(out, bufio.NewReader(s))
	if err != nil {
		return nil, fmt.Errorf("recv: %w", err)
	}

	final := filepath.Join(dir, rootCID)
	if err := os.Rename(tmp, final); err != nil {
		return nil, err
	}
	return &FetchResult{LocalPath: final, Bytes: nw}, nil
}

// 여러 provider 순차 시도 → 첫 성공 반환
func FetchAny(ctx context.Context, d *dhtnode.Node, rootCID string, providers []task.Provider, dir string) (*FetchResult, error) {
	var lastErr error
	for _, pv := range providers {
		res, err := FetchFromProvider(ctx, d, rootCID, pv, dir)
		if err == nil {
			return res, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no providers")
	}
	return nil, lastErr
}
