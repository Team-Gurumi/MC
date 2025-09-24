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
	"github.com/docker/docker/api/types/strslice"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy" // stdout/stderr 프레임 분리

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
)

// 자원 제한/메트릭/결과 구조
type ThrottleDevice struct {
	Path string `json:"path"`
	Rate uint64 `json:"rate"`
}
type Resources struct {
	CPUQuota        int64            `json:"cpu_quota,omitempty"`
	CPUPeriod       int64            `json:"cpu_period,omitempty"`
	NanoCPUs        int64            `json:"nano_cpus,omitempty"`
	MemoryBytes     int64            `json:"memory_bytes,omitempty"`
	MemorySwap      int64            `json:"memory_swap,omitempty"`
	CPUShares       int64            `json:"cpu_shares,omitempty"`
	CPUSetCPUs      string           `json:"cpuset_cpus,omitempty"`
	PidsLimit       int64            `json:"pids_limit,omitempty"`
	BlkioWeight     uint16           `json:"blkio_weight,omitempty"`
	DeviceReadBps   []ThrottleDevice `json:"device_read_bps,omitempty"`
	DeviceWriteBps  []ThrottleDevice `json:"device_write_bps,omitempty"`
	DeviceReadIOPS  []ThrottleDevice `json:"device_read_iops,omitempty"`
	DeviceWriteIOPS []ThrottleDevice `json:"device_write_iops,omitempty"`
}

type Task struct {
	ID         string            `json:"id"`
	Image      string            `json:"image"`
	Cmd        []string          `json:"cmd,omitempty"`
	Env        []string          `json:"env,omitempty"`
	Labels     map[string]string `json:"labels,omitempty"`
	TimeoutSec int               `json:"timeout_sec,omitempty"`

	// 런타임/자원 제한을 컨트롤에서 내려줌 (임시)
	Runtime   string    `json:"runtime,omitempty"`
	Resources Resources `json:"resources,omitempty"`
}

type Metrics struct {
	// 매트릭 집계
	AvgCPUPercent float64 `json:"avg_cpu_percent"`
	MaxCPUPercent float64 `json:"max_cpu_percent"`

	AvgMemBytes float64 `json:"avg_mem_bytes"`
	MaxMemBytes float64 `json:"max_mem_bytes"`

	SumBlkReadBytes  uint64 `json:"sum_blk_read_bytes"`
	SumBlkWriteBytes uint64 `json:"sum_blk_write_bytes"`

	SumNetRxBytes uint64 `json:"sum_net_rx_bytes"`
	SumNetTxBytes uint64 `json:"sum_net_tx_bytes"`

	Samples int `json:"samples"`
}

type Result struct {
	StdoutTail string `json:"stdout_tail,omitempty"`
	StderrTail string `json:"stderr_tail,omitempty"`
	// 필요시 JSON 결과/요약 등을 추가 가능
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	hostname, _ := os.Hostname()
	nodeID := getenv("NODE_ID", hostname)

	controlURL := os.Getenv("CONTROL_URL")
	if controlURL == "" {
		log.Fatal("CONTROL_URL environment variable is not set. Cannot poll for tasks.")
	}

	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	must(err, "docker client")

	// host 관점의 메트릭 주기적 로깅
	go metricsLoop(ctx, 10*time.Second)

	log.Printf("[agent] started node_id=%s control=%s", nodeID, emptyDash(controlURL))

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("[agent] shutting down")
			return
		case <-ticker.C:
			task, ok := pollAndClaim(controlURL, nodeID)
			if !ok {
				continue
			}
			log.Printf("[agent] claimed task id=%s image=%s", task.ID, task.Image)

			runCtx := ctx
			var cancelRun context.CancelFunc
			if task.TimeoutSec > 0 {
				runCtx, cancelRun = context.WithTimeout(ctx, time.Duration(task.TimeoutSec)*time.Second)
			}
			metrics, res, exitCode, runErr := runTaskWithDocker(runCtx, cli, task)
			if cancelRun != nil {
				cancelRun()
			}

			status := "finished"
			notes := "ok"
			if runErr != nil {
				status = "failed"
				notes = runErr.Error()
			}
			reportFinishAndResult(controlURL, task.ID, status, &exitCode, notes, metrics, res)

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
			cpuP, _ := cpu.Percent(0, false)
			vm, _ := mem.VirtualMemory()
			if len(cpuP) > 0 {
				log.Printf("[metrics] host cpu=%.1f%% mem=%.1f%%", cpuP[0], vm.UsedPercent)
			} else {
				log.Printf("[metrics] host mem=%.1f%%", vm.UsedPercent)
			}
		}
	}
}

func pollAndClaim(controlURL, nodeID string) (Task, bool) {
	body := map[string]any{"node_id": nodeID}
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
	defer resp.Body_Close()

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
	if t.Runtime == "" {
		t.Runtime = getenv("DOCKER_RUNTIME", "kata-runtime") 
	}
	return t, true
}

func runTaskWithDocker(ctx context.Context, cli *client.Client, task Task) (Metrics, Result, int, error) {
	var metrics Metrics
	var result Result
	exitCode := -1

	// 이미지 풀: types.ImagePullOptions
	rc, err := cli.ImagePull(ctx, task.Image, types.ImagePullOptions{})
	if err != nil {
		return metrics, result, exitCode, fmt.Errorf("image pull: %w", err)
	}
	io.Copy(io.Discard, rc)
	rc.Close()

	cfg := &container.Config{
		Image:  task.Image,
		Env:    task.Env,
		Labels: task.Labels,
	}
	// CMD가 있으면 적용
	if len(task.Cmd) > 0 {
		cfg.Cmd = strslice.StrSlice(task.Cmd)
	}

	// Kata 런타임 + 자원 제한
	hostCfg := &container.HostConfig{
		AutoRemove: true,
		Runtime:    task.Runtime, // ex) "kata-runtime"
		Resources:  toDockerResources(task.Resources),
	}

	resp, err := cli.ContainerCreate(ctx, cfg, hostCfg, nil, nil, "")
	if err != nil {
		return metrics, result, exitCode, fmt.Errorf("container create: %w", err)
	}

	// 로그 버퍼(산출물로 전송)
	stdoutBuf := new(bytes.Buffer)
	stderrBuf := new(bytes.Buffer)

	// 로그 팔로우: stdcopy로 stdout/stderr 분리 복사
	logs, err := cli.ContainerLogs(ctx, resp.ID, container.LogsOptions{
		ShowStdout: true, ShowStderr: true, Follow: true, Tail: "200",
	})
	if err == nil {
		go func() {
			defer logs.Close()
			// 도커의 멀티플렉스 프레임을 해석해 각각의 버퍼로 복사
			_, _ = stdcopy.StdCopy(
				io.MultiWriter(os.Stdout, stdoutBuf), // stdout
				io.MultiWriter(os.Stderr, stderrBuf), // stderr
				logs,
			)
		}()
	} else {
		log.Printf("[task:%s] logs open err: %v", task.ID, err)
	}

	// 컨테이너 관점의 stats 스트림 수집
	statsCtx, statsCancel := context.WithCancel(ctx)
	defer statsCancel()
	statsDone := make(chan struct{})
	go func() {
		defer close(statsDone)
		stats, err := cli.ContainerStats(statsCtx, resp.ID, true)
		if err != nil {
			log.Printf("[task:%s] stats err: %v", task.ID, err)
			return
		}
		defer stats.Body.Close()
		dec := json.NewDecoder(stats.Body)
		samples := 0

		var maxCPU float64
		var maxMem float64
		var sumCPU float64
		var sumMem float64

		var sumBlkRead, sumBlkWrite uint64
		var sumNetRx, sumNetTx uint64

		var v types.StatsJSON
		for dec.More() {
			if err := dec.Decode(&v); err != nil {
				break
			}
			samples++

			// CPU 퍼센트 계산
			cpuDelta := float64(v.CPUStats.CPUUsage.TotalUsage - v.PreCPUStats.CPUUsage.TotalUsage)
			sysDelta := float64(v.CPUStats.SystemUsage - v.PreCPUStats.SystemUsage)
			var cpuPercent float64
			if sysDelta > 0 && cpuDelta > 0 && len(v.CPUStats.CPUUsage.PercpuUsage) > 0 {
				cpuPercent = (cpuDelta / sysDelta) * float64(len(v.CPUStats.CPUUsage.PercpuUsage)) * 100.0
			}
			memUsage := float64(v.MemoryStats.Usage)

			sumCPU += cpuPercent
			sumMem += memUsage
			if cpuPercent > maxCPU {
				maxCPU = cpuPercent
			}
			if memUsage > maxMem {
				maxMem = memUsage
			}

			// BlkIO 합산
			for _, e := range v.BlkioStats.IoServiceBytesRecursive {
				switch strings.ToLower(e.Op) {
				case "read":
					sumBlkRead += uint64(e.Value)
				case "write":
					sumBlkWrite += uint64(e.Value)
				}
			}
			// Net 합산 (인터페이스별)
			for _, n := range v.Networks {
				sumNetRx += n.RxBytes
				sumNetTx += n.TxBytes
			}
		}

		if samples > 0 {
			metrics = Metrics{
				AvgCPUPercent:    sumCPU / float64(samples),
				MaxCPUPercent:    maxCPU,
				AvgMemBytes:      sumMem / float64(samples),
				MaxMemBytes:      maxMem,
				SumBlkReadBytes:  sumBlkRead,
				SumBlkWriteBytes: sumBlkWrite,
				SumNetRxBytes:    sumNetRx,
				SumNetTxBytes:    sumNetTx,
				Samples:          samples,
			}
		}
	}()

	if err := cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return metrics, result, exitCode, fmt.Errorf("container start: %w", err)
	}
	log.Printf("[task:%s] container started id=%s runtime=%s", task.ID, resp.ID[:12], hostCfg.Runtime)

	// 종료 대기
	statusCh, errCh := cli.ContainerWait(ctx, resp.ID, container.WaitConditionNotRunning)

	select {
	case err := <-errCh:
		if err != nil {
			return metrics, result, exitCode, fmt.Errorf("wait err: %w", err)
		}
	case st := <-statusCh:
		exitCode = int(st.StatusCode)
		b, _ := json.Marshal(st)
		log.Printf("[task:%s] exited: %s", task.ID, string(b))
	}

	// stats 고루틴 마무리: 작업이 끝나면 스트림을 끊고 동기화
	statsCancel()
	<-statsDone

	// 결과 tail 저장 (너무 크면 tail만)
	result.StdoutTail = tailOf(stdoutBuf.String(), 64*1024)
	result.StderrTail = tailOf(stderrBuf.String(), 64*1024) // 🔧 stderr도 전송

	if exitCode != 0 {
		return metrics, result, exitCode, fmt.Errorf("non-zero exit code: %d", exitCode)
	}
	return metrics, result, exitCode, nil
}

func toDockerResources(r Resources) container.Resources {
	cr := container.Resources{
		CPUPeriod:   r.CPUPeriod,
		CPUQuota:    r.CPUQuota,
		NanoCPUs:    r.NanoCPUs,
		Memory:      r.MemoryBytes,
		MemorySwap:  r.MemorySwap,
		CPUShares:   r.CPUShares,
		CpusetCpus:  r.CPUSetCPUs,
		PidsLimit:   r.PidsLimit,
		BlkioWeight: r.BlkioWeight,
	}
	// Throttle (I/O 제한)
	for _, d := range r.DeviceReadBps {
		cr.BlkioDeviceReadBps = append(cr.BlkioDeviceReadBps, container.ThrottleDevice{
			Path: d.Path, Rate: d.Rate,
		})
	}
	for _, d := range r.DeviceWriteBps {
		cr.BlkioDeviceWriteBps = append(cr.BlkioDeviceWriteBps, container.ThrottleDevice{
			Path: d.Path, Rate: d.Rate,
		})
	}
	for _, d := range r.DeviceReadIOPS {
		cr.BlkioDeviceReadIOps = append(cr.BlkioDeviceReadIOps, container.ThrottleDevice{
			Path: d.Path, Rate: d.Rate,
		})
	}
	for _, d := range r.DeviceWriteIOPS {
		cr.BlkioDeviceWriteIOps = append(cr.BlkioDeviceWriteIOps, container.ThrottleDevice{
			Path: d.Path, Rate: d.Rate,
		})
	}
	return cr
}

func tailOf(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[len(s)-max:]
}

// finish + metrics + result를 한 번에 보고
func reportFinishAndResult(controlURL, taskID, status string, exitCode *int, notes string, m Metrics, r Result) {
	payload := map[string]any{
		"status":    status,
		"exit_code": exitCode,
		"notes":     notes,
		"metrics":   m,
		"result":    r,
	}
	b, _ := json.Marshal(payload)
	url := strings.TrimRight(controlURL, "/") + "/api/tasks/" + taskID + "/finish"

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		log.Printf("[finish] req err: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	httpCli := &http.Client{Timeout: 10 * time.Second}
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