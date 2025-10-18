package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"path/filepath"
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
	ttlSec := flag.Int("ttl-sec", 15, "lease TTL seconds (권장: 15)")
	hbSec  := flag.Int("heartbeat-sec", 5, "heartbeat interval seconds (권장: 5)")
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
	// TTL/하트비트 실제 값 계산 + 안전 보정
	leaseTTL := time.Duration(*ttlSec) * time.Second
	hbEvery  := time.Duration(*hbSec)  * time.Second
	if hbEvery >= leaseTTL {
		if leaseTTL > time.Second {
			hbEvery = leaseTTL - time.Second
	} else {
			hbEvery = leaseTTL / 2
		}
	}
	log.Printf("[agent] config: TTL=%s heartbeat=%s (flags: -ttl-sec=%d -heartbeat-sec=%d)", leaseTTL, hbEvery, *ttlSec, *hbSec)

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

lease, err := claim.TryClaim(ctx2, jobID, agentID, leaseTTL)
		if err != nil {
			return
		}

		log.Printf("[agent] 작업 점유 성공 job=%s ver=%d exp=%s",
			jobID, lease.Version, lease.Expires.Format(time.RFC3339))

		jobCtx, cancelJob := context.WithCancel(context.Background())
		go func(taskID, nonce string) {
			defer log.Printf("[agent] job=%s heartbeat 종료", taskID)
			t := time.NewTicker(hbEvery)
			defer t.Stop()

			fail := 0
			const maxFail = 3

			for {
				select {
				case <-jobCtx.Done(): // 작업이 끝나면 여기로 빠져나옴
					return
				case <-t.C:
				if _, err := claim.Heartbeat(context.Background(), taskID, agentID, nonce, leaseTTL); err != nil {
						fail++
						if fail >= maxFail {
							return
						}
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
		if err := d.GetJSON(task.KeyMeta(
		jobID), &meta, 3*time.Second); err != nil {
			_ = finish.Report(context.Background(), jobID, "failed",
				map[string]any{"error_stage": "get_meta"}, "", nil, "get meta failed: "+err.Error())
			cancelJob()
			return
		}
		// 4) 입력 파일 준비 (매니페스트 로드 및 Fetch)
inputDir := filepath.Join(workDir, "input")
if err := os.MkdirAll(inputDir, 0o755); err != nil {
	_ = finish.Report(context.Background(), jobID, "failed",
		map[string]any{"error_stage": "create_input_dir"}, "", nil, "create input dir failed: "+err.Error())
	cancelJob()
	return
}

var man task.Manifest
if err := d.GetJSON(task.KeyManifest(jobID), &man, 3*time.Second); err == nil && man.RootCID != "" {
	// 입력이 명시적으로 '없음'이면 fetch 스킵
	if strings.EqualFold(man.RootCID, "noop") || len(man.Providers) == 0 {
		log.Printf("[agent] job=%s no input fetch (root_cid=%q providers=%d) -> skip", jobID, man.RootCID, len(man.Providers))
	} else {
		log.Printf("[agent] job=%s fetching input: %s", jobID, man.RootCID)
		if _, err := agent.FetchAny(context.Background(), d, man.RootCID, man.Providers, inputDir); err != nil {
			// Fetch 실패 시 종료 보고
			_ = finish.Report(context.Background(), jobID, "failed",
				map[string]any{"error_stage": "fetch_input"}, "", nil, "fetch failed: "+err.Error())
			cancelJob()
			return
		}
		log.Printf("[agent] job=%s fetch complete: %s -> %s", jobID, man.RootCID, inputDir)
	}
} else {
	// 매니페스트가 없으면 스킵
	log.Printf("[agent] job=%s no manifest found, skipping fetch", jobID)
}

		// 5) 작업 실행 - 컨테이너를 사용하도록 변경
		res, runErr := agent.RunInContainer(context.Background(), workDir, meta.Image, meta.Command)

		// 디버깅 로그 추가
		if res != nil {
			log.Printf("[agent] job=%s run result: exit_code=%d, stdout_len=%d, stderr_len=%d, duration=%s, error=%v",
				jobID, res.ExitCode, len(res.Stdout), len(res.Stderr), res.Duration, runErr)
		} else {
			log.Printf("[agent] job=%s run failed without result: %v", jobID, runErr)
		}

		cancelJob()
		// 6) 종료 보고
		status := "succeeded"
		errMsg := ""
		if runErr != nil || (res != nil && res.ExitCode != 0) {
			status = "failed"
			if runErr != nil {
				errMsg = runErr.Error()
			}
		}

		metrics := make(map[string]any)
		if res != nil {
			metrics = map[string]any{
				"exit_code":    res.ExitCode,
				"duration_ms":  res.Duration.Milliseconds(),
				"stdout_bytes": len(res.Stdout),
				"stderr_bytes": len(res.Stderr),
			}
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

