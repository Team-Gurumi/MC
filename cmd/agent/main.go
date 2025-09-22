// 작업을 폴링 → Docker 컨테이너 실행 → 메트릭 로깅

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"

	// ★ DHT 사이드채널용
	"example.com/mc-agent/internal/dht"
)

type Task struct {
	ID         string            `json:"id"`
	Image      string            `json:"image"`
	Cmd        []string          `json:"cmd,omitempty"`
	Env        []string          `json:"env,omitempty"`
	Labels     map[string]string `json:"labels,omitempty"`
	TimeoutSec int               `json:"timeout_sec,omitempty"`
}

func main() {
	// 컨텍스트/시그널 핸들링
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// 노드 식별자
	hostname, _ := os.Hostname()
	nodeID := getenv("NODE_ID", hostname)

	// 컨트롤 서버 (없으면 로컬 데모 작업 1회 실행)
	controlURL := os.Getenv("CONTROL_URL")

	// Docker 클라이언트
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	must(err, "docker client")

	// 메트릭 주기적 로깅
	go metricsLoop(ctx, 10*time.Second)

	log.Printf("[agent] started node_id=%s control=%s", nodeID, emptyDash(controlURL))

	// 작업 루프
	taskDemoDone := false
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("[agent] shutting down")
			return
		case <-ticker.C:
			// 1) 작업 폴링/클레임
			task, ok := pollAndClaim(controlURL, nodeID, &taskDemoDone)
			if !ok {
				continue
			}
			log.Printf("[agent] claimed task id=%s image=%s", task.ID, task.Image)

			// ★ DHT: Claim 직후 상태 반영 (assigned)
			{
				cliDHT := dht.NewFromEnv()
				now := time.Now().UTC().Format(time.RFC3339)
				ctx2, cancel2 := context.WithTimeout(context.Background(), cliDHT.Timeout)
				_ = cliDHT.SetJSON(ctx2, "task:"+task.ID+":state", map[string]any{
					"status":     "assigned",
					"assignedTo": nodeID,
					"updatedAt":  now,
					"lastSeenAt": now,
				})
				cancel2()
			}

			// 2) 실행
			runCtx := ctx
			var cancelRun context.CancelFunc
			if task.TimeoutSec > 0 {
				runCtx, cancelRun = context.WithTimeout(ctx, time.Duration(task.TimeoutSec)*time.Second)
			}
			err := runTaskWithDocker(runCtx, cli, task, nodeID) // ★ nodeID 전달
			if cancelRun != nil {
				cancelRun()
			}

			// 3) finish 보고 (컨트롤 서버가 있을 때만)
			status := "finished"
			notes := "ok"
			if err != nil {
				status = "failed"
				notes = err.Error()
			}

			// ★ DHT: 종료 상태 반영 (finished/failed)
			{
				cliDHT := dht.NewFromEnv()
				now := time.Now().UTC().Format(time.RFC3339)
				ctx2, cancel2 := context.WithTimeout(context.Background(), cliDHT.Timeout)
				// exitCode는 여기선 알 수 없으니 nil로 둡니다(필요시 run 함수 반환값 확장)
				_ = cliDHT.SetJSON(ctx2, "task:"+task.ID+":state", map[string]any{
					"status":     status,
					"assignedTo": nodeID,
					"exitCode":   nil,
					"notes":      notes,
					"updatedAt":  now,
					"lastSeenAt": now,
				})
				cancel2()
			}

			if controlURL != "" {
				reportFinish(controlURL, task.ID, status, nil, notes)
			}

			if err != nil {
				log.Printf("[task:%s] ERROR: %v", task.ID, err)
				continue
			}
		}
	}
}

func metricsLoop(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			cpuP, _ := cpu.Percent(0, false) // 전체 평균
			vm, _ := mem.VirtualMemory()
			if len(cpuP) > 0 {
				log.Printf("[metrics] cpu=%.1f%% mem=%.1f%%", cpuP[0], vm.UsedPercent)
			} else {
				log.Printf("[metrics] mem=%.1f%%", vm.UsedPercent)
			}
		}
	}
}

// CONTROL_URL이 비어있으면, 데모로 nginx:alpine 1회만 실행
// 있으면 컨트롤 서버에 POST /api/tasks/claim 로 작업 요청
func pollAndClaim(controlURL, nodeID string, demoDone *bool) (Task, bool) {
	if controlURL == "" {
		if *demoDone {
			return Task{}, false
		}
		*demoDone = true
		return Task{
			ID:    "demo-1",
			Image: "nginx:alpine",
			Labels: map[string]string{
				"worknet.node": nodeID,
				"worknet.task": "demo",
			},
			TimeoutSec: 120,
		}, true
	}

	// POST {CONTROL_URL}/api/tasks/claim
	body := map[string]any{
		"node_id": nodeID,
	}
	b, _ := json.Marshal(body)

	req, err := http.NewRequest(http.MethodPost, strings.TrimRight(controlURL, "/")+"/api/tasks/claim", bytes.NewReader(b))
	if err != nil {
		log.Printf("[claim] req err: %v", err)
		return Task{}, false
	}
	req.Header.Set("Content-Type", "application/json")

	httpCli := &http.Client{Timeout: 8 * time.Second}
	resp, err := httpCli.Do(req)
	if err != nil {
		log.Printf("[claim] http err: %v", err)
		return Task{}, false
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return Task{}, false
	}
	if resp.StatusCode != http.StatusOK {
		x, _ := io.ReadAll(resp.Body)
		log.Printf("[claim] bad status=%d body=%s", resp.StatusCode, string(x))
		return Task{}, false
	}

	var t Task
	if err := json.NewDecoder(resp.Body).Decode(&t); err != nil {
		log.Printf("[claim] decode err: %v", err)
		return Task{}, false
	}
	return t, true
}

func runTaskWithDocker(ctx context.Context, cli *client.Client, task Task, nodeID string) error {
	// 이미지 풀
	rc, err := cli.ImagePull(ctx, task.Image, image.PullOptions{})
	if err != nil {
		return fmt.Errorf("image pull: %w", err)
	}
	io.Copy(io.Discard, rc)
	rc.Close()

	cfg := &container.Config{
		Image:  task.Image,
		Cmd:    task.Cmd,
		Env:    task.Env,
		Labels: task.Labels,
	}
	hostCfg := &container.HostConfig{
		AutoRemove: true,
		// TODO: 리소스 제한(cgroups) 설정, 볼륨 마운트 등
	}

	resp, err := cli.ContainerCreate(ctx, cfg, hostCfg, nil, nil, "")
	if err != nil {
		return fmt.Errorf("container create: %w", err)
	}

	if err := cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return fmt.Errorf("container start: %w", err)
	}
	log.Printf("[task:%s] container started id=%s", task.ID, resp.ID[:12])

	// ★ DHT: 컨테이너 시작 직후 상태 반영 (running)
	{
		cliDHT := dht.NewFromEnv()
		now := time.Now().UTC().Format(time.RFC3339)
		ctx2, cancel2 := context.WithTimeout(context.Background(), cliDHT.Timeout)
		_ = cliDHT.SetJSON(ctx2, "task:"+task.ID+":state", map[string]any{
			"status":     "running",
			"assignedTo": nodeID,
			"updatedAt":  now,
			"lastSeenAt": now,
		})
		cancel2()
	}

	// 로그 팔로우 (선택)
	logs, err := cli.ContainerLogs(ctx, resp.ID, container.LogsOptions{
		ShowStdout: true, ShowStderr: true, Follow: true, Tail: "50",
	})
	if err == nil {
		go func() {
			defer logs.Close()
			io.Copy(os.Stdout, logs)
		}()
	}

	// 종료 대기
	statusCh, errCh := cli.ContainerWait(ctx, resp.ID, container.WaitConditionNotRunning)
	var exitCode int64 = -1

	select {
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("wait err: %w", err)
		}
	case st := <-statusCh:
		exitCode = st.StatusCode
		b, _ := json.Marshal(st)
		log.Printf("[task:%s] exited: %s", task.ID, string(b))
	}

	// 종료코드는 현재 반환값에 포함하지 않고, 0이 아니면 에러 반환
	if exitCode != 0 {
		return fmt.Errorf("non-zero exit code: %d", exitCode)
	}
	return nil
}

// finish 호출 함수
func reportFinish(controlURL, taskID, status string, exitCode *int, notes string) {
	m := map[string]any{
		"status":    status, // finished | failed
		"exit_code": exitCode,
		"notes":     notes,
	}
	b, _ := json.Marshal(m)
	url := strings.TrimRight(controlURL, "/") + "/api/tasks/" + taskID + "/finish"

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		log.Printf("[finish] req err: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	httpCli := &http.Client{Timeout: 8 * time.Second}
	resp, err := httpCli.Do(req)
	if err != nil {
		log.Printf("[finish] http err: %v", err)
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		log.Printf("[finish] bad status=%d body=%s", resp.StatusCode, string(body))
		return
	}
	log.Printf("[finish] reported id=%s status=%s", taskID, status)
}

func getenv(k, def string) string {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	return v
}
func must(err error, where string) {
	if err != nil {
		log.Fatalf("%s: %v", where, err)
	}
}
func emptyDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
