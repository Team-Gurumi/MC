package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/Team-Gurumi/MC/pkg/agent"
	dhtnode "github.com/Team-Gurumi/MC/pkg/dht"
	"github.com/Team-Gurumi/MC/pkg/task"
)

func main() {

	ns := flag.String("ns", "default", "네임스페이스")
	discEvery := flag.Duration("discover-every", 5*time.Second, "작업 발견 주기")
	bootstrapPeers := flag.String("bootstrap", "", "컴마로 구분된 부트스트랩 피어 목록")

	
	controlURL := flag.String("control-url", "http://127.0.0.1:8080", "Control API 기본 URL")
	authToken := flag.String("auth-token", "", "Control API 인증 토큰")
	flag.Parse() // 플래그 파싱

	// 애플리케이션의 메인 컨텍스트 생성
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // main 함수 종료 시 컨텍스트 취소

	// DHT 노드 초기화
	d, err := initDHTNode(ctx, *ns, *bootstrapPeers)
	if err != nil {
		log.Fatalf("DHT 노드 초기화 실패: %v", err)
	}

	var token string
	if *authToken != "" {
		token = *authToken 
	} else {
		token = os.Getenv("CONTROL_TOKEN") 
	}

	// Control 서버와 통신할 클라이언트 생성
	claim := &agent.HTTPClaimClient{
		BaseURL: *controlURL,
		Client:  &http.Client{Timeout: 5 * time.Second}, // 5초 타임아웃
		Token:   token,
	}
	// 작업 완료/실패를 보고할 클라이언트
	finish := &agent.FinishClient{
		BaseURL: *controlURL,
		Client:  &http.Client{Timeout: 5 * time.Second},
		Token:   token,
	}
	// 이 에이전트의 고유 ID (P2P 네트워크 ID)
	agentID := d.Host.ID().String()
	// --- 클라이언트 설정 끝 ---

	// 작업 발견(Discoverer) 로직 초기화
	dv := agent.NewDiscoverer(d, *ns, *discEvery)
	// DHT에서 현재 네임스페이스의 작업 ID 목록을 가져오는 함수
	listIDs := func() []string { return agent.ListFromIndex(d, *ns) }

	
	onCandidate := func(jobID string, providers []task.Provider) {
		// 1) try-claim
		ctx2, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()

		lease, err := claim.TryClaim(ctx2, jobID, agentID, 30*time.Second)
		if err != nil {
			return 
		}
		

		log.Printf("[agent] 작업 점유 성공 job=%s ver=%d exp=%s",
			jobID, lease.Version, lease.Expires.Format(time.RFC3339))

jobCtx, cancelJob := context.WithCancel(context.Background())
go func(taskID, nonce string) {
    defer log.Printf("[agent] job=%s 하트비트 종료", taskID)
    t := time.NewTicker(15 * time.Second) // 또는 ttl/2
    defer t.Stop()

    fail := 0
    const maxFail = 3

    for {
        select {
        case <-jobCtx.Done(): // 작업이 끝나면 여기로 빠져나옴
            return
        case <-t.C:
            if _, err := claim.Heartbeat(context.Background(), taskID, agentID, nonce, 30*time.Second); err != nil {
                fail++
                if fail >= maxFail { return }
                continue
            }
            fail = 0
            log.Printf("[agent] job=%s 하트비트 전송 성공", taskID)
        }
    }
}(jobID, lease.Nonce)
	

		// 3) 작업 준비: 메타 읽기 + 워크디렉터리
		workDir := "./work/" + jobID
		_ = os.MkdirAll(workDir, 0o755)

		var meta task.TaskMeta
		if err := d.GetJSON(task.KeyMeta(jobID), &meta, 3*time.Second); err != nil {
			_ = finish.Report(context.Background(), jobID, "failed",
				map[string]any{"error_stage": "get_meta"}, "", nil, "get meta failed: "+err.Error())
				cancelJob() 
			return
		}

		
		res, runErr := agent.RunLocal(context.Background(), workDir, meta.Image, meta.Command)

		cancelJob() 
// 6) 종료 보고
		status := "succeeded"
		errMsg := ""
		if runErr != nil || res.ExitCode != 0 {
			status = "failed"
			if runErr != nil {
				errMsg = runErr.Error()
			}
		}
		metrics := map[string]any{
			"exit_code":    res.ExitCode,
			"duration_ms":  res.Duration.Milliseconds(),
			"stdout_bytes": len(res.Stdout),
			"stderr_bytes": len(res.Stderr),
		}
		if err := finish.Report(context.Background(), jobID, status, metrics, "", nil, errMsg); err != nil {
			log.Printf("[agent] finish report failed job=%s: %v", jobID, err)
		} else {
			log.Printf("[agent] finish reported job=%s status=%s", jobID, status)
		}
	
	}
	
	// 백그라운드에서 Discoverer 실행
	go dv.Run(ctx, listIDs, onCandidate)

	// 프로그램이 종료되지 않도록 대기
	<-ctx.Done()
}

func initDHTNode(ctx context.Context, ns, bootstrapPeers string) (*dhtnode.Node, error) {
	var addrs []string
	if bootstrapPeers != "" {
		for _, s := range strings.Split(bootstrapPeers, ",") {
			s = strings.TrimSpace(s)
			if s != "" {
				addrs = append(addrs, s)
			}
		}
	}
	node, err := dhtnode.NewNode(ctx, ns, addrs)
	if err != nil {
		return nil, err
	}

	log.Printf("[agent] P2P 노드 시작됨: %s", node.Host.ID())
	for _, a := range node.Multiaddrs() {
		log.Printf("[agent] 리스닝 주소: %s", a)
	}
	return node, nil
}
