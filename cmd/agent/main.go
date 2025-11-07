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
	hbSec := flag.Int("heartbeat-sec", 5, "heartbeat interval seconds (권장: 5)")
	flag.Parse()

	// 메인 컨텍스트
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// DHT 노드
	d, err := initDHTNode(ctx, *ns, *bootstrapPeers)
	if err != nil {
		log.Fatalf("DHT 노드 초기화 실패: %v", err)
	}

	// 토큰
	var token string
	if *authToken != "" {
		token = *authToken
	} else {
		token = os.Getenv("CONTROL_TOKEN")
	}

	// TTL/하트비트 보정
	leaseTTL := time.Duration(*ttlSec) * time.Second
	hbEvery := time.Duration(*hbSec) * time.Second
	if hbEvery >= leaseTTL {
		if leaseTTL > time.Second {
			hbEvery = leaseTTL - time.Second
		} else {
			hbEvery = leaseTTL / 2
		}
	}

	log.Printf("[agent] config: TTL=%s heartbeat=%s (flags: -ttl-sec=%d -heartbeat-sec=%d)", leaseTTL, hbEvery, *ttlSec, *hbSec)

	// 컨트롤과 통신할 클라이언트들
	claim := &agent.HTTPClaimClient{
		BaseURL: *controlURL,
		Client:  &http.Client{Timeout: 5 * time.Second},
		Token:   token,
	}
	finish := &agent.FinishClient{
		BaseURL: *controlURL,
		Client:  &http.Client{Timeout: 5 * time.Second},
		Token:   token,
	}

	agentID := d.Host.ID().String()

	// discoverer
	dv := agent.NewDiscoverer(d, *ns, *discEvery)
	listIDs := func() []string { return agent.ListFromIndex(d, *ns) }

	onCandidate := func(jobID string, providers []task.Provider, demandURL string) {
    // 1) 이 잡에 대해 사용할 베이스 URL을 정한다.
    base := *controlURL
    if demandURL != "" {
        base = demandURL
    }
    // claim 이랑 finish 둘 다 같은 베이스를 쓰도록 맞춘다.
    claim.BaseURL = base
    finish.BaseURL = base

		// 1) try-claim
		ctx2, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()

		lease, err := claim.TryClaim(ctx2, jobID, agentID, leaseTTL)
		if err != nil {
		
			return
		}

		log.Printf("[agent] 작업 점유 성공 job=%s ver=%d exp=%s",
			jobID, lease.Version, lease.Expires.Format(time.RFC3339))
		log.Printf(`{"event":"lease_acquired","timestamp":"%s","job_id":"%s","agent_id":"%s"}`,
			time.Now().UTC().Format(time.RFC3339Nano), jobID, agentID)

		leaseToken := lease.Version

		// 이 job만을 위한 컨텍스트
		jobCtx, cancelJob := context.WithCancel(context.Background())

		// 2) 하트비트 고루틴
		go func(taskID, nonce string, leaseTok int64) {
			defer log.Printf("[agent] job=%s heartbeat 종료", taskID)

			t := time.NewTicker(hbEvery)
			defer t.Stop()

			fail := 0
			const maxFail = 3

			for {
				select {
				case <-jobCtx.Done():
					return
				case <-t.C:
					if _, err := claim.Heartbeat(context.Background(), taskID, agentID, nonce, leaseTTL, leaseTok); err != nil {
						fail++
						if fail >= maxFail {
							return
						}
						continue
					}
					fail = 0
				}
			}
		}(jobID, lease.Nonce, leaseToken)

		// 3) manifest 확인
		var man task.Manifest
		if err := d.GetJSON(task.KeyManifest(jobID), &man, 3*time.Second); err != nil || man.RootCID == "" {
			_ = finish.Report(
				context.Background(),
				jobID,
				"failed",
				map[string]any{
					"error_stage": "manifest_check",
					"reason":      "manifest_not_found",
				},
				"",
				nil,
				"manifest not found on DHT",
				agentID,
				leaseToken,
			)
			cancelJob()
			return
		}

		// 3.5) meta도 읽어와야 run 가능
		var meta task.TaskMeta
		if err := d.GetJSON(task.KeyMeta(jobID), &meta, 3*time.Second); err != nil {
			_ = finish.Report(
				context.Background(),
				jobID,
				"failed",
				map[string]any{
					"error_stage": "get_meta",
				},
				"",
				nil,
				"get meta failed: "+err.Error(),
				agentID,
				leaseToken,
			)
			cancelJob()
			return
		}

		// 4) 입력 fetch
		workDir := "./work/" + jobID
		_ = os.MkdirAll(workDir, 0o755)
		inputDir := filepath.Join(workDir, "input")
		_ = os.MkdirAll(inputDir, 0o755)

		if !strings.EqualFold(man.RootCID, "noop") && len(man.Providers) > 0 {
			if _, err := agent.FetchAny(context.Background(), d, man.RootCID, man.Providers, inputDir); err != nil {
				_ = finish.Report(
					context.Background(),
					jobID,
					"failed",
					map[string]any{
						"error_stage": "fetch_input",
					},
					"",
					nil,
					"fetch failed: "+err.Error(),
					agentID,
					leaseToken,
				)
				cancelJob()
				return
			}
		}

		// 5) 실행
		res, runErr := agent.RunInContainer(context.Background(), workDir, meta.Image, meta.Command)

		// 하트비트 종료
		cancelJob()

		status := "succeeded"
		errMsg := ""
		if runErr != nil || (res != nil && res.ExitCode != 0) {
			status = "failed"
			if runErr != nil {
				errMsg = runErr.Error()
			}
		}

		metrics := map[string]any{}
		if res != nil {
			metrics = map[string]any{
				"exit_code":    res.ExitCode,
				"duration_ms":  res.Duration.Milliseconds(),
				"stdout_bytes": len(res.Stdout),
				"stderr_bytes": len(res.Stderr),
			}
		}

		// 6) 종료 보고 (재시도 포함)
const maxFinishRetries = 20           // 최대 시도 횟수
const finishRetryDelay = 5 * time.Second // 각 시도 간격

var lastErr error
for attempt := 1; attempt <= maxFinishRetries; attempt++ {
    err := finish.Report(
        context.Background(),
        jobID,
        status,
        metrics,
        "",
        nil,
        errMsg,
        agentID,
        leaseToken,
    )
    if err == nil {
        log.Printf("[agent] finish reported job=%s status=%s (attempt %d)", jobID, status, attempt)
        lastErr = nil
        break
    }
    lastErr = err

    if !agent.ShouldRetryFinish(err) {
        log.Printf("[agent] finish report failed (non-retryable) job=%s: %v", jobID, err)
        break
    }

    log.Printf("[agent] finish report retry %d/%d for job=%s: %v", attempt, maxFinishRetries, jobID, err)
    time.Sleep(finishRetryDelay)
}

if lastErr != nil {
    log.Printf("[agent] finish report failed after %d attempts job=%s: %v", maxFinishRetries, jobID, lastErr)
}

	}

	// discoverer 실행
	go dv.Run(ctx, listIDs, onCandidate)

	// 대기
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

