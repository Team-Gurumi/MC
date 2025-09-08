package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ThrottleDevice struct {
	Path string `json:"path"`
	Rate uint64 `json:"rate"`
}
type Resources struct {
	CPUQuota       int64            `json:"cpu_quota,omitempty"`
	CPUPeriod      int64            `json:"cpu_period,omitempty"`
	NanoCPUs       int64            `json:"nano_cpus,omitempty"`
	MemoryBytes    int64            `json:"memory_bytes,omitempty"`
	MemorySwap     int64            `json:"memory_swap,omitempty"`
	CPUShares      int64            `json:"cpu_shares,omitempty"`
	CPUSetCPUs     string           `json:"cpuset_cpus,omitempty"`
	PidsLimit      int64            `json:"pids_limit,omitempty"`
	BlkioWeight    uint16           `json:"blkio_weight,omitempty"`
	DeviceReadBps  []ThrottleDevice `json:"device_read_bps,omitempty"`
	DeviceWriteBps []ThrottleDevice `json:"device_write_bps,omitempty"`
	DeviceReadIOPS []ThrottleDevice `json:"device_read_iops,omitempty"`
	DeviceWriteIOPS []ThrottleDevice `json:"device_write_iops,omitempty"`
}

type Metrics struct {
	AvgCPUPercent   float64 `json:"avg_cpu_percent"`
	MaxCPUPercent   float64 `json:"max_cpu_percent"`
	AvgMemBytes     float64 `json:"avg_mem_bytes"`
	MaxMemBytes     float64 `json:"max_mem_bytes"`
	SumBlkReadBytes uint64  `json:"sum_blk_read_bytes"`
	SumBlkWriteBytes uint64 `json:"sum_blk_write_bytes"`
	SumNetRxBytes   uint64  `json:"sum_net_rx_bytes"`
	SumNetTxBytes   uint64  `json:"sum_net_tx_bytes"`
	Samples         int     `json:"samples"`
}

type Result struct {
	StdoutTail string `json:"stdout_tail,omitempty"`
	StderrTail string `json:"stderr_tail,omitempty"`
}

type Task struct {
	ID         string            `json:"id"`
	Image      string            `json:"image"`
	Cmd        []string          `json:"cmd,omitempty"`
	Env        []string          `json:"env,omitempty"`
	Labels     map[string]string `json:"labels,omitempty"`
	TimeoutSec int               `json:"timeout_sec,omitempty"`

	Runtime   string    `json:"runtime,omitempty"`
	Resources Resources `json:"resources,omitempty"`

	Status     string    `json:"status"`      // queued|assigned|running|finished|failed|expired
	AssignedTo string    `json:"assigned_to"` // node_id
	AssignedAt time.Time `json:"assigned_at"`
	FinishedAt time.Time `json:"finished_at"`
	ExitCode   *int      `json:"exit_code,omitempty"`
	Notes      string    `json:"notes,omitempty"`

	// 결과/메트릭 저장 (사용자 노출/정합성 확인용)
	Metrics Metrics `json:"metrics,omitempty"`
	Result  Result  `json:"result,omitempty"`
}

type claimReq struct {
	NodeID       string                 `json:"node_id"`
	Capabilities map[string]interface{} `json:"capabilities,omitempty"`
}

type store struct {
	mu      sync.Mutex
	tasks   map[string]*Task
	queue   []string
	timeout time.Duration
}

func newStore(timeout time.Duration) *store {
	s := &store{tasks: map[string]*Task{}, queue: []string{}, timeout: timeout}
	go s.requeueLoop()
	return s
}

func (s *store) requeueLoop() {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for range t.C {
		now := time.Now()
		var requeues []string
		s.mu.Lock()
		for id, task := range s.tasks {
			if (task.Status == "assigned" || task.Status == "running") &&
				!task.AssignedAt.IsZero() && now.Sub(task.AssignedAt) > s.timeout {
				task.Status = "expired"
				requeues = append(requeues, id)
			}
		}
		for _, id := range requeues {
			task := s.tasks[id]
			task.Status = "queued"
			task.AssignedTo = ""
			task.AssignedAt = time.Time{}
			s.queue = append(s.queue, id)
			log.Printf("[requeue] %s", id)
		}
		s.mu.Unlock()
	}
}

func (s *store) push(t *Task) *Task {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t.ID == "" {
		t.ID = genID()
	}
	if t.TimeoutSec <= 0 {
		t.TimeoutSec = 120
	}
	if t.Runtime == "" {
		t.Runtime = "kata-runtime" 
	}
	t.Status = "queued"
	s.tasks[t.ID] = t
	s.queue = append(s.queue, t.ID)
	return t
}

func (s *store) claim(node string) (*Task, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for len(s.queue) > 0 {
		id := s.queue[0]
		s.queue = s.queue[1:]
		task := s.tasks[id]
		if task == nil || task.Status != "queued" {
			continue
		}
		task.Status = "assigned"
		task.AssignedTo = node
		task.AssignedAt = time.Now()
		return task, true
	}
	return nil, false
}

func (s *store) finish(id, status string, exitCode *int, notes string, m *Metrics, r *Result) (*Task, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return nil, false
	}
	t.Status = status
	t.ExitCode = exitCode
	t.Notes = notes
	t.FinishedAt = time.Now()
	if m != nil { t.Metrics = *m }
	if r != nil { t.Result  = *r }
	return t, true
}

func genID() string {
	return strconv.FormatInt(time.Now().UnixNano(), 36) + "-" + strconv.Itoa(rand.Intn(100000))
}

type server struct{ s *store }

func (sv *server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/tasks/push", sv.push)
	mux.HandleFunc("/api/tasks/claim", sv.claim)
	mux.HandleFunc("/api/tasks", sv.list)         // GET
	mux.HandleFunc("/api/tasks/", sv.getOrFinish) // GET /{id} or POST /{id}/finish
	return withJSON(mux)
}

func withJSON(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (sv *server) push(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		httpErr(w, 405, "method not allowed"); return
	}
	var t Task
	if err := json.NewDecoder(r.Body).Decode(&t); err != nil || t.Image == "" {
		httpErr(w, 400, "invalid payload"); return
	}
	sv.s.push(&t)
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(t)
}

func (sv *server) claim(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		httpErr(w, 405, "method not allowed"); return
	}
	var req claimReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.NodeID == "" {
		httpErr(w, 400, "node_id required"); return
	}
	t, ok := sv.s.claim(req.NodeID)
	if !ok {
		w.WriteHeader(http.StatusNoContent); return
	}
	_ = json.NewEncoder(w).Encode(t)
}

func (sv *server) list(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httpErr(w, 405, "method not allowed"); return
	}
	sv.s.mu.Lock()
	out := make([]*Task, 0, len(sv.s.tasks))
	for _, t := range sv.s.tasks {
		out = append(out, t)
	}
	sv.s.mu.Unlock()
	_ = json.NewEncoder(w).Encode(out)
}

func (sv *server) getOrFinish(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/tasks/")
	parts := strings.Split(path, "/")
	if len(parts) == 1 && r.Method == http.MethodGet {
		id := parts[0]
		sv.s.mu.Lock()
		t, ok := sv.s.tasks[id]
		sv.s.mu.Unlock()
		if !ok {
			httpErr(w, 404, "not found"); return
		}
		_ = json.NewEncoder(w).Encode(t); return
	}
	if len(parts) == 2 && parts[1] == "finish" && r.Method == http.MethodPost {
		id := parts[0]
		var body struct {
			Status   string   `json:"status"`
			ExitCode *int     `json:"exit_code,omitempty"`
			Notes    string   `json:"notes,omitempty"`
			Metrics  *Metrics `json:"metrics,omitempty"` // ★★ 추가
			Result   *Result  `json:"result,omitempty"`  // ★★ 추가
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Status == "" {
			httpErr(w, 400, "status required"); return
		}
		t, ok := sv.s.finish(id, body.Status, body.ExitCode, body.Notes, body.Metrics, body.Result)
		if !ok {
			httpErr(w, 404, "not found"); return
		}
		_ = json.NewEncoder(w).Encode(t); return
	}
	http.NotFound(w, r)
}

func httpErr(w http.ResponseWriter, code int, msg string) {
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func main() {
	rand.Seed(time.Now().UnixNano())
	timeout, _ := strconv.Atoi(getenv("TASK_TIMEOUT_SEC", "120"))
	s := newStore(time.Duration(timeout) * time.Second)

	addr := ":" + getenv("PORT", "8080")
	log.Printf("[control] listen %s timeout=%ds", addr, timeout)
	if err := http.ListenAndServe(addr, (&server{s}).routes()); err != nil {
		log.Fatal(err)
	}
}

func getenv(k, d string) string { if v := os.Getenv(k); v != "" { return v }; return d }
