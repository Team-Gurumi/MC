// cmd/control/control_http.go
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	dhtnode "github.com/Team-Gurumi/MC/pkg/dht"
	"github.com/Team-Gurumi/MC/pkg/task"
)

// ===== 공용 유틸 =====

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func requireAuth(r *http.Request) bool {
	// 데모용: CONTROL_TOKEN 이 설정되면 Bearer 검증, 없으면 무조건 통과
	want := os.Getenv("CONTROL_TOKEN")
	if want == "" {
		return true
	}
	got := r.Header.Get("Authorization")
	return got == ("Bearer " + want)
}

func addToIndex(d *dhtnode.Node, id string) error {
	var idx task.TaskIndex
	if err := d.GetJSON(task.IndexKey, &idx, 2*time.Second); err != nil {
		// not found -> 새로 생성
		idx = task.TaskIndex{IDs: []string{id}, UpdatedAt: time.Now(), Version: 1}
		return d.PutJSON(task.IndexKey, idx)
	}
	for _, e := range idx.IDs {
		if e == id {
			return nil
		}
	}
	idx.IDs = append(idx.IDs, id)
	idx.UpdatedAt = time.Now()
	idx.Version++
	return d.PutJSON(task.IndexKey, idx)
}

// ===== /api/tasks =====

type CreateTaskReq struct {
	ID      string   `json:"id"`
	Image   string   `json:"image"`
	Command []string `json:"command"`
}

func createTaskHandler(d *dhtnode.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requireAuth(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req CreateTaskReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ID == "" {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		meta := task.TaskMeta{
			ID:        req.ID,
			Image:     req.Image,
			Command:   req.Command,
			CreatedAt: time.Now(),
		}
		if err := d.PutJSON(task.KeyMeta(req.ID), meta); err != nil {
			http.Error(w, "dht put meta failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
	     st := task.TaskState{
           ID: req.ID, Status: task.StatusQueued, UpdatedAt: time.Now(), Version: 1,
       }
		if err := d.PutJSON(task.KeyState(req.ID), st); err != nil {
			http.Error(w, "dht put state failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		_ = addToIndex(d, req.ID)

		// 데모: Demand/P2P 지시사항 흉내
		resp := map[string]any{
			"job_id": req.ID,
			"p2p_instructions": map[string]any{
				"allowed_peer_policy": "any", // demo
				"agent_pubkey":        "",    // demo
			},
		}
		writeJSON(w, 200, resp)
	}
}

func getTaskHandler(d *dhtnode.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requireAuth(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		id := strings.TrimPrefix(r.URL.Path, "/api/tasks/")
		if id == "" {
			http.NotFound(w, r)
			return
		}
		var meta task.TaskMeta
		if err := d.GetJSON(task.KeyMeta(id), &meta, 2*time.Second); err != nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		var st task.TaskState
		_ = d.GetJSON(task.KeyState(id), &st, 2*time.Second)
		// 결과 CID가 있으면 같이 실어줌(데모)
		var result map[string]any
		_ = d.GetJSON(fmt.Sprintf("task/%s/result", id), &result, 1*time.Second)

		writeJSON(w, 200, map[string]any{"meta": meta, "state": st, "result": result})
	}
}

// ===== /jobs/{id}/manifest =====

type ManifestReq struct {
	CID     string   `json:"cid"`
	Seeders []string `json:"seeders"`
	EncMeta string   `json:"enc_meta"`
}

func manifestHandler(d *dhtnode.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requireAuth(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/") // jobs/{id}/manifest
		if len(parts) != 3 || parts[0] != "jobs" || parts[2] != "manifest" {
			http.Error(w, "bad path", http.StatusBadRequest)
			return
		}
		id := parts[1]

		var req ManifestReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.CID == "" {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		key := fmt.Sprintf("p2p/%s/manifest", id)
		if err := d.PutJSON(key, req); err != nil {
			http.Error(w, "dht put failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		// TASK_AD 데모 레코드
		ad := map[string]any{
			"job_id":     id,
			"demand_url": fmt.Sprintf("http://%s/api/tasks/%s", r.Host, id),
			"topic":      "job:" + id,
			"exp":        time.Now().Add(1 * time.Hour),
		}
		_ = d.PutJSON("TASK_AD/"+id, ad)
		w.WriteHeader(http.StatusNoContent)
	}
}

// ===== GET /api/tasks/{id}/logs =====

func logsHandler(d *dhtnode.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requireAuth(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/tasks/"), "/logs")
		var endp map[string]any
		if err := d.GetJSON(task.KeyWS(id), &endp, 2*time.Second); err != nil {
			http.Error(w, "no ws endpoint", http.StatusNotFound)
			return
		}
		// 데모 토큰 발급(실사용 X)
		tkn := fmt.Sprintf("demo-%d", time.Now().Unix())
		_ = d.PutJSON(task.KeyWS(id)+"_token", map[string]any{"token": tkn, "issued": time.Now()})

		writeJSON(w, 200, map[string]any{"endpoint": endp, "token": tkn})
	}
}

// ===== POST /jobs/{id}/finish =====

func finishHandler(d *dhtnode.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requireAuth(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/") // jobs/{id}/finish
		if len(parts) != 3 || parts[0] != "jobs" || parts[2] != "finish" {
			http.Error(w, "bad path", http.StatusBadRequest)
			return
		}
		id := parts[1]

		var p map[string]any
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}
		// state 업데이트
		var st task.TaskState
		_ = d.GetJSON(task.KeyState(id), &st, 2*time.Second)
		switch fmt.Sprint(p["status"]) {
		case "succeeded":
			st.Status = task.StatusFinished
		default:
			st.Status = task.StatusFailed
		}
		
    st.UpdatedAt = time.Now()
       st.Version++
		_ = d.PutJSON(task.KeyState(id), st)

		// 결과 CID 있으면 저장
		if cid, ok := p["result_root_cid"].(string); ok && cid != "" {
			_ = d.PutJSON(fmt.Sprintf("task/%s/result", id), map[string]any{"cid": cid})
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

// ===== 라우팅 등록 =====

func mountHTTP(d *dhtnode.Node) *http.ServeMux {
	mux := http.NewServeMux()
	 // POST /api/tasks
	mux.Handle("/api/tasks", createTaskHandler(d))
	// /api/tasks/{id} 와 /api/tasks/{id}/logs 를 한 핸들러에서 분기
	mux.Handle("/api/tasks/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        if strings.HasSuffix(r.URL.Path, "/logs") {
            logsHandler(d).ServeHTTP(w, r)
            return
        }
        getTaskHandler(d).ServeHTTP(w, r) // 기본은 GET /api/tasks/{id}
  }))
 
	
	mux.Handle("/jobs/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/manifest") {
			manifestHandler(d).ServeHTTP(w, r)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/finish") {
			finishHandler(d).ServeHTTP(w, r)
			return
		}
		http.NotFound(w, r)
	}))
	return mux
}
