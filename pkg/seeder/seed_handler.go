package seeder

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log"
	"os"
	"path/filepath"

	dhtnode "github.com/Team-Gurumi/MC/pkg/dht"
	"github.com/Team-Gurumi/MC/pkg/p2p"
	"github.com/libp2p/go-libp2p/core/network"
)

// 파일 소스 인터페이스: rootCID로 파일을 연다.
type Source interface {
	Open(rootCID string) (io.ReadCloser, int64, error)
}

// 로컬 디렉터리에서 rootCID 이름의 파일을 연다.
type SourceFS struct{ Base string }

func (s SourceFS) Open(cid string) (io.ReadCloser, int64, error) {
	path := filepath.Join(s.Base, cid)
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	st, _ := f.Stat()
	return f, st.Size(), nil
}

// libp2p 스트림 핸들러 등록: JSON 요청을 받고 파일 바이트를 그대로 전송.
func MountSeedHandler(ctx context.Context, n *dhtnode.Node, src Source) {
	n.Host.SetStreamHandler(p2p.ProtoGet, func(s network.Stream) {
		defer s.Close()

		// 1) JSON 요청 읽기
		var req struct {
			RootCID string `json:"root_cid"`
		}
		if err := json.NewDecoder(bufio.NewReader(s)).Decode(&req); err != nil {
			log.Printf("[seeder] bad json: %v", err)
			return
		}
		if req.RootCID == "" {
			log.Printf("[seeder] empty root_cid")
			return
		}

		// 2) 파일 열기
		rc, size, err := src.Open(req.RootCID)
		if err != nil {
			log.Printf("[seeder] open %s: %v", req.RootCID, err)
			return
		}
		defer rc.Close()

		// 3) 파일 바디 스트리밍
		bw := bufio.NewWriter(s)
		written, err := io.Copy(bw, rc)
		if err != nil {
			log.Printf("[seeder] copy err: %v", err)
			return
		}
		if err := bw.Flush(); err != nil {
			log.Printf("[seeder] flush err: %v", err)
			return
		}
		log.Printf("[seeder] sent %d/%d bytes for %s", written, size, req.RootCID)
	})
}
