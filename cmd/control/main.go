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

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
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
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

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

			// 2) 실행 (필요 시 타임아웃)
			runCtx := ctx
			var cancelRun context.CancelFunc
			if task.TimeoutSec > 0 {
				runCtx, cancelRun = context.WithTimeout(ctx, time.Duration(task.TimeoutSec)*time.Second)
			}

			exitCode, runErr := runTaskWithDocker(runCtx, cli, task)

			// 타임아웃 컨텍스트 정리 (defer 누적 방지)
			if cancelRun != nil {
				cancelRun()
			}

			// 3) finish 리포트
			if controlURL != "" {
				status := "finished"
				notes := "ok"
				if runErr != nil {
					status = "failed"
					notes = runErr.Error()
				}
				reportFinish(controlURL, task.ID, status, exitCode, notes)
			}

			if runErr != nil {
				log.Printf("[task:%s] ERROR: %v", task.ID, runErr)
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
			if vm == nil {
				if len(cpuP) > 0 {
					log.Printf("[metrics] cpu=%.1f%% mem=?%%", cpuP[0])
				} else {
					log.Printf("[metrics] mem=?%%")
				}
				continue
			}
			if len(cpuP) > 0 {
				log.Printf("[metrics] cpu=%.1f%% mem=%.1f%%", cpuP[0], vm.UsedPercent)
			} else {
				log.Printf("[metrics] mem=%.1f%%", vm.UsedPercent)
			}
		}
	}
}

// controlURL이 비어있으면, 데모로 nginx:alpine 1회만 실행
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

	// TODO: 실제 컨트롤 서버 연동 (예: GET /api/tasks/claim?node_id=xxx)
	// - 작업 없으면 204
	// - 작업 있으면 200 + JSON Task
	return Task{}, false
}

// 컨테이너 종료코드까지 반환
func runTaskWithDocker(ctx context.Context, cli *client.Client, task Task) (*int, error) {
	// 이미지 풀
	reader, err := cli.ImagePull(ctx, task.Image, types.ImagePullOptions{})
	if err != nil {
		return nil, fmt.Errorf("image pull: %w", err)
	}
	io.Copy(io.Discard, reader)
	reader.Close()

	cfg := &container.Config{
		Image:  task.Image,
		Cmd:    task.Cmd,
		Env:    task.Env,
		Labels: task.Labels,
	}
	hostCfg := &container.HostConfig{
		AutoRemove: true,
		// TODO: 리소스 제한(cgroups), 볼륨 마운트 등
	}

	resp, err := cli.ContainerCreate(ctx, cfg, hostCfg, nil, nil, "")
	if err != nil {
		return nil, fmt.Errorf("container create: %w", err)
	}

	if err := cli.ContainerStart(ctx, resp.ID, types.ContainerStartOptions{}); err != nil {
		return nil, fmt.Errorf("container start: %w", err)
	}
	log.Printf("[task:%s] container started id=%s", task.ID, resp.ID[:12])

	// 로그 팔로우 (선택)
	logs, err := cli.ContainerLogs(ctx, resp.ID, types.ContainerLogsOptions{
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
	select {
	case err := <-errCh:
		if err != nil {
			return nil, fmt.Errorf("wait err: %w", err)
		}
	case st := <-statusCh:
		// st.StatusCode는 int64
		code := int(st.StatusCode)
		b, _ := json.Marshal(st)
		log.Printf("[task:%s] exited: %s", task.ID, string(b))
		return &code, nil
	}
	// 이론상 도달하지 않음
}

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
