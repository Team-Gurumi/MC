package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"errors"
	"strconv"
	"net/http"
	"os"
	"strings"
	"time"
	"log"
	mrand "math/rand"
	"github.com/Team-Gurumi/MC/pkg/demand"
	dhtnode "github.com/Team-Gurumi/MC/pkg/dht"
	"github.com/Team-Gurumi/MC/pkg/task"
	peer "github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

type providerDTO struct {
	PeerID string   `json:"peer_id"`
	Addrs  []string `json:"addrs"`
	Relays []string `json:"relays,omitempty"`
	Caps   []string `json:"caps,omitempty"`
}

type manifestInDTO struct {
	RootCID    string        `json:"root_cid"`
	Providers  []providerDTO `json:"providers"`
	EncMeta    string        `json:"enc_meta,omitempty"`
	Rendezvous string        `json:"rendezvous,omitempty"`
	Transports []string      `json:"transports,omitempty"`
	Version    int64         `json:"version"`
	UpdatedAt  time.Time     `json:"updated_at"`
}

// manifest 출력용(일반 사용자용: enc_meta 제외)
type manifestOutDTO struct {
	RootCID    string        `json:"root_cid"`
	Providers  []providerDTO `json:"providers"`
	Rendezvous string        `json:"rendezvous,omitempty"`
	Transports []string      `json:"transports,omitempty"`
	Version    int64         `json:"version"`
	UpdatedAt  time.Time     `json:"updated_at"`
}

// manifest 출력용(에이전트용: enc_meta 포함)
type manifestOutWithSecretDTO struct {
	manifestOutDTO
	EncMeta string `json:"enc_meta"` // agent 권한에서만
}

type tryClaimIn struct {
	AgentID string `json:"agent_id"`
	TTLSec  int    `json:"ttl_sec"`
}
type hbIn struct {
	AgentID      string `json:"agent_id"`
	Nonce        string `json:"nonce"`
	TTLSec       int    `json:"ttl_sec"`
	LeaseVersion int64  `json:"lease_version"`
}
type releaseIn struct {
	AgentID string `json:"agent_id"`
	Nonce   string `json:"nonce"`
}

type leaseOut struct {
	OK    bool       `json:"ok"`
	Lease task.Lease `json:"lease"`
}

func newNonce() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b[:]), nil
}

// ===== /jobs/{id}/finish =====

type finishIn struct {
	Status    string         `json:"status"`
	Metrics   map[string]any `json:"metrics,omitempty"`
	Artifacts []string       `json:"artifacts,omitempty"`
	ResultCID string         `json:"result_root_cid,omitempty"`
	Error     string         `json:"error,omitempty"`
}

func putManifestMirror(node *dhtnode.Node, id string, providers []task.Provider, ttl time.Duration) error {
	mir := manifestMirror{
		Providers: providers,
		Exp:       time.Now().UTC().Add(ttl),
	}
	return node.PutJSON(keyP2PManifestMirror(id), mir)
}

func finishHandler(d *dhtnode.Node, store demand.Store, ns string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requireAuth(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(parts) != 3 || parts[0] != "jobs" || parts[2] != "finish" {
			http.Error(w, "bad path", http.StatusBadRequest)
			return
		}
		id := parts[1]

		var in finishIn
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
			return
		}

		agentID := r.Header.Get("X-Agent-ID")
		tokenStr := r.Header.Get("X-Lease-Token")
		var token uint64
		if tokenStr != "" {
			if v, err := strconv.ParseUint(tokenStr, 10, 64); err == nil {
				token = v
			}
		}

		succeeded := strings.ToLower(in.Status) == "succeeded"

		if err := store.Finish(r.Context(), id, agentID, token, succeeded, in.ResultCID, in.Artifacts, in.Metrics); err != nil {
			http.Error(w, "finish failed: "+err.Error(), http.StatusConflict)
			return
		}

		// DHT 쪽도 기존 코드처럼 덮어주기
		var st task.TaskState
		_ = d.GetJSON(task.KeyState(id), &st, 2*time.Second)
		now := time.Now().UTC()
		if succeeded {
			st.Status = task.StatusSucceeded
		} else {
			st.Status = task.StatusFailed
		}
		st.UpdatedAt = now
		st.FinishedAt = &now
		st.Version++
		_ = d.PutJSON(task.KeyState(id), st)
		_ = d.DelJSON(task.KeyLease(id))
_ = d.DelJSON(keyTaskAd(ns, id))      // ad/<ns>/task/<id>
		_ = d.DelJSON(keyP2PManifestMirror(id)) // p2p/<id>/manifest
		w.WriteHeader(http.StatusNoContent)
	}
}

func ttlOrDefault(sec int) time.Duration {
	if sec <= 0 || sec > 3600 {
		return 15 * time.Second
	}
	return time.Duration(sec) * time.Second
}

var allowedTransports = map[string]bool{
	"quic-v1": true,
	"webrtc":  true,
}

type taskAd struct {
	JobID     string    `json:"job_id"`
	Namespace string    `json:"ns,omitempty"`
	Topic     string    `json:"topic,omitempty"`
	DemandURL string    `json:"demand_url,omitempty"` 
	Exp       time.Time `json:"exp"`
	Sig       string    `json:"sig,omitempty"`
}
type manifestMirror struct {
	Providers []task.Provider `json:"providers"`
	Exp       time.Time       `json:"exp"`
}

// p2p/<id>/manifest 미러: P2P 좌표만 짧게
type manifestAd struct {
	RootCID    string         `json:"root_cid"`
	Providers  []task.Provider `json:"providers"`
	Rendezvous string         `json:"rendezvous,omitempty"`
	Transports []string       `json:"transports,omitempty"`
	Exp        time.Time      `json:"exp"`
}

func getNamespace() string {
	if v := os.Getenv("MC_NS"); v != "" {
		return v
	}
	return "default"
}

func keyTaskAd(ns, id string) string        { return "ad/" + ns + "/task/" + id }
func keyP2PManifestMirror(id string) string { return "p2p/" + id + "/manifest" }

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func requireAuth(r *http.Request) bool {
	if os.Getenv("MC_DISABLE_AUTH") == "1" {
		return true
	}
	want := os.Getenv("CONTROL_TOKEN")
	if want == "" {
		return false
	}
	got := r.Header.Get("Authorization")
	return got == ("Bearer " + want)
}

// DHT 인덱스를 샤드 중 하나에만 기록
func addToIndex(d *dhtnode.Node, ns, id string) error {
	// 0 ~ TaskIndexShardCount-1 중 하나 선택
	shard := mrand.Intn(task.TaskIndexShardCount)
	key := task.KeyIndexShard(ns, shard)

	var idx task.TaskIndex
	// 이 샤드는 아직 없을 수 있으니 에러 무시
	_ = d.GetJSON(key, &idx, 2*time.Second)

	// 중복 방지
	for _, x := range idx.IDs {
		if x == id {
			return nil
		}
	}

	idx.IDs = append(idx.IDs, id)
	idx.UpdatedAt = time.Now().UTC()
	idx.Version++

	return d.PutJSON(key, idx)
}
// ===== /api/tasks =====

type CreateTaskReq struct {
	ID      string   `json:"id"`
	Image   string   `json:"image"`
	Command []string `json:"command"`
}

func debugLeaseHandler(d *dhtnode.Node, ns string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requireAuth(r) {
			http.Error(w, "unauthorized", 401)
			return
		}
		id := strings.TrimPrefix(r.URL.Path, "/internal/tasks/")
		id = strings.TrimSuffix(id, "/lease")
		var st task.TaskState
		_ = d.GetJSON(task.KeyState(id), &st, 2*time.Second)
		var le task.Lease
		_ = d.GetJSON(task.KeyLease(id), &le, 2*time.Second)
		writeJSON(w, 200, map[string]any{
			"id":           id,
			"status":       st.Status,
			"owner":        st.AssignedTo,
			"lease_owner":  le.Owner,
			"lease_expires": le.Expires,
			"lease_ver":    le.Version,
			"updated_at":   st.UpdatedAt,
		})
	}
}

// ★ 여기서 enqueue 추가
func createTaskHandler(d *dhtnode.Node, store demand.Store, ns string, enqueue func(string)) http.HandlerFunc {
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

		now := time.Now().UTC()

		// 1) DB에 먼저 기록
		if err := store.CreateJob(r.Context(), demand.DBJob{
			ID:        req.ID,
			Image:     req.Image,
			Command:   req.Command,
			Status:    demand.StatusQueued,
			CreatedAt: now,
		}); err != nil {
			http.Error(w, "db create failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		// 2) 에이전트 discover 용으로 DHT에도 메타 쓰기
		meta := task.TaskMeta{ID: req.ID, Image: req.Image, Command: req.Command, CreatedAt: now}
		if err := d.PutJSON(task.KeyMeta(req.ID), meta); err != nil {
			http.Error(w, "dht put meta failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		// 상태도 그대로
		st := task.TaskState{
			ID:        req.ID,
			Status:    task.StatusQueued,
			UpdatedAt: now,
			Version:   1,
		}
		if err := d.PutJSON(task.KeyState(req.ID), st); err != nil {
			http.Error(w, "dht put state failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if err := addToIndex(d, ns, req.ID); err != nil {
			http.Error(w, "index update failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		// ★ 새 잡도 바로 광고 큐에 넣는다
		if enqueue != nil {
			enqueue(req.ID)
		}

		writeJSON(w, http.StatusCreated, map[string]any{"job_id": req.ID})
	}
}

func getTaskHandler(store demand.Store, d *dhtnode.Node) http.HandlerFunc {
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

		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		job, err := store.GetJob(ctx, id)
		if err != nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}

		// manifest 는 DHT에서 쓰는 게 맞음 (시더 정보)
		var man task.Manifest
		var outMan *manifestOutDTO
		if err := d.GetJSON(task.KeyManifest(id), &man, 2*time.Second); err == nil && man.RootCID != "" {
			m := &manifestOutDTO{
				RootCID:    man.RootCID,
				Providers:  make([]providerDTO, len(man.Providers)),
				Rendezvous: man.Rendezvous,
				Transports: man.Transports,
				Version:    man.Version,
				UpdatedAt:  man.UpdatedAt,
			}
			for i, p := range man.Providers {
				m.Providers[i] = providerDTO{
					PeerID: p.PeerID,
					Addrs:  p.Addrs,
					Relays: p.Relays,
					Caps:   p.Caps,
				}
			}
			outMan = m
		}

		resp := map[string]any{
			"meta": map[string]any{
				"id":      job.ID,
				"image":   job.Image,
				"command": job.Command,
			},
			"state": map[string]any{
				"id":     job.ID,
				"status": job.Status,
			},
		}
		if outMan != nil {
			resp["manifest"] = outMan
		}

		writeJSON(w, http.StatusOK, resp)
	}
}

func manifestHandler(d *dhtnode.Node, store demand.Store, enqueue func(string)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requireAuth(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(parts) != 3 || parts[0] != "jobs" || parts[2] != "manifest" {
			http.Error(w, "bad path", http.StatusBadRequest)
			return
		}
		id := parts[1]

		var in manifestInDTO
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
			return
		}
		if in.RootCID == "" {
			http.Error(w, "root_cid required", http.StatusBadRequest)
			return
		}
		if err := validateTransports(in.Transports); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		
		// allow noop manifest: root_cid == "noop" and no providers
       if !(in.RootCID == "noop" && len(in.Providers) == 0) {
           for i, pv := range in.Providers {
               if err := validateProvider(pv); err != nil {
                   http.Error(w, fmt.Sprintf("provider[%d]: %v", i, err), http.StatusBadRequest)
                   return
               }
           }
       }
		

		if in.UpdatedAt.IsZero() {
			in.UpdatedAt = time.Now().UTC()
		}

		man := task.Manifest{
			RootCID:    in.RootCID,
			Providers:  make([]task.Provider, len(in.Providers)),
			EncMeta:    in.EncMeta,
			Rendezvous: in.Rendezvous,
			Transports: in.Transports,
			Version:    in.Version,
			UpdatedAt:  in.UpdatedAt,
		}
		for i, pv := range in.Providers {
			man.Providers[i] = task.Provider{
				PeerID: pv.PeerID,
				Addrs:  pv.Addrs,
				Relays: pv.Relays,
				Caps:   pv.Caps,
			}
		}

		if err := d.PutJSON(task.KeyManifest(id), man); err != nil {
			http.Error(w, "store error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		ttl := 2 * time.Minute
		mir := manifestMirror{
			Providers: man.Providers,
			Exp:       time.Now().UTC().Add(ttl),
		}
		if err := d.PutJSON(keyP2PManifestMirror(id), mir); err != nil {
			http.Error(w, "mirror store error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if err := store.AttachManifest(r.Context(), id, demand.Manifest{
			RootCID:   in.RootCID,
			Providers: providerIDs(in.Providers),
			EncMeta:   in.EncMeta,
		}); err != nil {
			http.Error(w, "db attach manifest failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		enqueue(id)

		w.WriteHeader(http.StatusNoContent)
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"ok":true}`))
}

// ===== GET /api/tasks/{id}/logs =====

func logsHandler(d *dhtnode.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requireAuth(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/tasks/"), "/logs")
		var endp map[string]any
		if err := d.GetJSON(task.KeyWS(id), &endp, 2*time.Second); err != nil {
			http.Error(w, "no ws endpoint", http.StatusNotFound)
			return
		}
		tkn := fmt.Sprintf("demo-%d", time.Now().Unix())
		_ = d.PutJSON(task.KeyWS(id)+"_token", map[string]any{"token": tkn, "issued": time.Now()})

		writeJSON(w, 200, map[string]any{"endpoint": endp, "token": tkn})
	}
}

func tryClaimHandler(d *dhtnode.Node, store demand.Store, ns string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requireAuth(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(parts) != 4 || parts[0] != "api" || parts[1] != "tasks" || parts[3] != "try-claim" {
			http.Error(w, "bad path", http.StatusBadRequest)
			return
		}
		id := parts[2]

		var in tryClaimIn
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
			return
		}
		if in.AgentID == "" {
			http.Error(w, "agent_id required", http.StatusBadRequest)
			return
		}
		ttl := ttlOrDefault(in.TTLSec)

		lease, err := store.TryClaim(r.Context(), id, in.AgentID, ttl)
if err != nil {
    // manifest가 없다고 거절하지 말고 계속 진행
    if errors.Is(err, demand.ErrLeaseConflict) {
        _ = json.NewEncoder(w).Encode(leaseOut{OK: false})
        return
    }
    if errors.Is(err, demand.ErrJobNotFound) {
        http.Error(w, "not found", http.StatusNotFound)
        return
    }

    // ErrNoManifest는 무시 (noop manifest 허용)
    if !errors.Is(err, demand.ErrNoManifest) {
        http.Error(w, "store error: "+err.Error(), http.StatusInternalServerError)
        return
    }
}

		// DHT 상태도 맞춰주기
		now := time.Now().UTC()
		var st task.TaskState
		_ = d.GetJSON(task.KeyState(id), &st, 2*time.Second)
		st.ID = id
		st.Status = task.StatusAssigned
		st.AssignedTo = in.AgentID
		st.UpdatedAt = now
		if st.StartedAt == nil {
			st.StartedAt = &now
		}
		st.Version++
		_ = d.PutJSON(task.KeyState(id), st)
		_ = d.PutJSON(task.KeyLease(id), task.Lease{
			Owner:   in.AgentID,
			Expires: lease.ExpireAt,
			Version: int64(lease.FencingToken),
		})
log.Printf(`{"event":"reassigned","timestamp":"%s","job_id":"%s","agent_id":"%s"}`,
    time.Now().UTC().Format(time.RFC3339Nano), id, in.AgentID)

		_ = json.NewEncoder(w).Encode(leaseOut{
			OK: true,
			Lease: task.Lease{
				Owner:   in.AgentID,
				Expires: lease.ExpireAt,
				Version: int64(lease.FencingToken),
			},
		})
	}
}

func releaseHandler(d *dhtnode.Node, ns string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requireAuth(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(parts) != 4 || parts[0] != "api" || parts[1] != "tasks" || parts[3] != "release" {
			http.Error(w, "bad path", http.StatusBadRequest)
			return
		}
		id := parts[2]

		var in releaseIn
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
			return
		}

		var cur task.Lease
		if err := d.GetJSON(task.KeyLease(id), &cur, 2*time.Second); err != nil || cur.Owner == "" {
			http.Error(w, "no lease", http.StatusNotFound)
			return
		}
		if cur.Owner != in.AgentID || cur.Nonce != in.Nonce {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}

		_ = d.DelJSON(task.KeyLease(id))
		var st task.TaskState
		_ = d.GetJSON(task.KeyState(id), &st, 2*time.Second)
		if st.ID != "" && (st.AssignedTo == in.AgentID || st.Status == task.StatusAssigned || st.Status == task.StatusRunning) {
			st.Status = task.StatusQueued
			st.AssignedTo = ""
			st.UpdatedAt = time.Now().UTC()
			st.Version++
			_ = d.PutJSON(task.KeyState(id), st)
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	}
}

func listTasksHandler(store demand.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requireAuth(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		limit := 100
		offset := 0
		if v := r.URL.Query().Get("limit"); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				limit = n
			}
		}
		if v := r.URL.Query().Get("offset"); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				offset = n
			}
		}

		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		jobs, err := store.ListPaged(ctx, limit, offset)
		if err != nil {
			http.Error(w, "db list failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		out := make([]map[string]any, 0, len(jobs))
		for _, j := range jobs {
			out = append(out, map[string]any{
				"id":          j.ID,
				"status":      j.Status,
				"image":       j.Image,
				"command":     j.Command,
				"created_at":  j.CreatedAt,
				"retry_count": j.RetryCount,
			})
		}
		writeJSON(w, http.StatusOK, out)
	}
}

func agentManifestHandler(d *dhtnode.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(parts) != 4 || parts[0] != "internal" || parts[1] != "agents" || parts[3] != "manifest" {
			http.Error(w, "bad path", http.StatusBadRequest)
			return
		}
		id := parts[2]

		token := r.URL.Query().Get("token")
		if token == "" || !validateAgentToken(token, id) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		var man task.Manifest
		if err := d.GetJSON(task.KeyManifest(id), &man, 2*time.Second); err != nil || man.RootCID == "" {
			http.Error(w, "manifest not found", http.StatusNotFound)
			return
		}

		out := manifestOutWithSecretDTO{
			manifestOutDTO: manifestOutDTO{
				RootCID:    man.RootCID,
				Providers:  make([]providerDTO, len(man.Providers)),
				Rendezvous: man.Rendezvous,
				Transports: man.Transports,
				Version:    man.Version,
				UpdatedAt:  man.UpdatedAt,
			},
			EncMeta: man.EncMeta,
		}
		for i, p := range man.Providers {
			out.Providers[i] = providerDTO{
				PeerID: p.PeerID,
				Addrs:  p.Addrs,
				Relays: p.Relays,
				Caps:   p.Caps,
			}
		}
		writeJSON(w, http.StatusOK, out)
	}
}

func validateAgentToken(token, taskID string) bool {
	return len(token) > 10 && strings.Contains(token, taskID)
}

func mountHTTP(d *dhtnode.Node, store demand.Store, ns string, enqueue func(string)) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/health", healthHandler)

	mux.HandleFunc("/api/tasks", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			listTasksHandler(store).ServeHTTP(w, r)
			return
		}
		if r.Method == http.MethodPost {
			createTaskHandler(d, store, ns, enqueue).ServeHTTP(w, r)
			return
		}
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	})

	mux.Handle("/api/tasks/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			if strings.HasSuffix(r.URL.Path, "/try-claim") {
				tryClaimHandler(d, store, ns).ServeHTTP(w, r)
				return
			}
			if strings.HasSuffix(r.URL.Path, "/heartbeat") {
				heartbeatHandler(d, store, ns).ServeHTTP(w, r)
				return
			}
			if strings.HasSuffix(r.URL.Path, "/release") {
				releaseHandler(d, ns).ServeHTTP(w, r)
				return
			}
		}
		if strings.HasSuffix(r.URL.Path, "/logs") {
			logsHandler(d).ServeHTTP(w, r)
			return
		}
		getTaskHandler(store, d).ServeHTTP(w, r)
	}))

	mux.Handle("/api/stats/tasks", statsHandler(store))

	mux.Handle("/jobs/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/manifest") {
			manifestHandler(d, store, enqueue).ServeHTTP(w, r)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/finish") {
			finishHandler(d, store, ns).ServeHTTP(w, r)
			return
		}
		http.NotFound(w, r)
	}))
mux.Handle("/api/jobs/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
    if strings.HasSuffix(r.URL.Path, "/finish") {
        finishHandler(d, store, ns).ServeHTTP(w, r)
        return
    }
    http.NotFound(w, r)
}))
 mux.Handle("/internal/tasks/",   debugLeaseHandler(d, ns))
    mux.Handle("/internal/requeue/", forceRequeueHandler(d, ns, enqueue))
	return mux
}

func statsHandler(store demand.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requireAuth(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		m, err := store.CountByStatus(ctx)
		if err != nil {
			http.Error(w, "count failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		out := map[string]any{
			"queued":    m[demand.StatusQueued],
			"assigned":  m[demand.StatusAssigned],
			"running":   m[demand.StatusRunning],
			"succeeded": m[demand.StatusSucceeded],
			"failed":    m[demand.StatusFailed],
		}
		writeJSON(w, http.StatusOK, out)
	}
}

func heartbeatHandler(d *dhtnode.Node, store demand.Store, ns string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requireAuth(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(parts) != 4 || parts[0] != "api" || parts[1] != "tasks" || parts[3] != "heartbeat" {
			http.Error(w, "bad path", http.StatusBadRequest)
			return
		}
		id := parts[2]

		var in hbIn
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
			return
		}
		ttl := ttlOrDefault(in.TTLSec)

		var token uint64
		if in.LeaseVersion > 0 {
			token = uint64(in.LeaseVersion)
		} else {
			tokenStr := strings.TrimSpace(in.Nonce)
			if tokenStr == "" {
				tokenStr = r.Header.Get("X-Lease-Token")
			}
			if tokenStr != "" {
				if v, err := strconv.ParseUint(tokenStr, 10, 64); err == nil {
					token = v
				}
			}
		}

		if token == 0 {
			http.Error(w, "missing lease token", http.StatusBadRequest)
			return
		}

		if err := store.Heartbeat(r.Context(), id, in.AgentID, token, ttl); err != nil {
			http.Error(w, "heartbeat failed: "+err.Error(), http.StatusConflict)
			return
		}

		var cur task.Lease
		_ = d.GetJSON(task.KeyLease(id), &cur, 2*time.Second)
		cur.Owner = in.AgentID
		cur.Expires = time.Now().UTC().Add(ttl)
		cur.Version++
		_ = d.PutJSON(task.KeyLease(id), cur)

		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	}
}

func forceRequeueHandler(d *dhtnode.Node, ns string, enqueue func(string)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requireAuth(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		   id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/internal/requeue/"), "/force-requeue")

		now := time.Now().UTC()
		var st task.TaskState
		if err := d.GetJSON(task.KeyState(id), &st, 2*time.Second); err != nil || st.ID == "" {
			http.Error(w, "state not found", http.StatusNotFound)
			return
		}
		_ = d.DelJSON(task.KeyLease(id))
		st.Status = task.StatusQueued
		st.AssignedTo = ""
		st.UpdatedAt = now
		st.Version++
		if err := d.PutJSON(task.KeyState(id), st); err != nil {
			http.Error(w, "put state failed", http.StatusInternalServerError)
			return
		}
		enqueue(id)
		w.WriteHeader(http.StatusNoContent)
	}
}

func providerIDs(in []providerDTO) []string {
	out := make([]string, 0, len(in))
	for _, p := range in {
		out = append(out, p.PeerID)
	}
	return out
}

func validateProvider(p providerDTO) error {
	if p.PeerID == "" {
		return fmt.Errorf("missing peer_id")
	}
	if _, err := peer.Decode(p.PeerID); err != nil {
		return fmt.Errorf("invalid peer_id: %w", err)
	}
	if len(p.Addrs) == 0 {
		return fmt.Errorf("addrs empty")
	}
	for _, a := range p.Addrs {
		if _, err := ma.NewMultiaddr(a); err != nil {
			return fmt.Errorf("invalid multiaddr: %s", a)
		}
		if strings.HasPrefix(a, "http://") || strings.HasPrefix(a, "https://") {
			return fmt.Errorf("http(s) not allowed in addrs: %s", a)
		}
	}
	return nil
}

func validateTransports(ts []string) error {
	for _, t := range ts {
		if !allowedTransports[strings.ToLower(t)] {
			return fmt.Errorf("transport not allowed: %s", t)
		}
	}
	return nil
}

func announceAds(ctx context.Context, d *dhtnode.Node, ns, id string, man *task.Manifest, ttl time.Duration) {
    exp := time.Now().UTC().Add(ttl)

    // 환경변수에서 PUBLIC URL 읽기
    durl := os.Getenv("CONTROL_PUBLIC_URL")
    if durl == "" {
        // 기본값은 로컬 개발 환경용
        durl = fmt.Sprintf("http://127.0.0.1:%s", os.Getenv("CONTROL_HTTP_PORT"))
        if durl == "http://127.0.0.1:" { // 포트도 없으면 완전 기본값
            durl = "http://127.0.0.1:8080"
        }
    }

    ad := taskAd{
        JobID:     id,
        Namespace: ns,
        Topic:     man.Rendezvous,
         DemandURL: durl,
 Exp:       exp,
    }

    if err := d.PutJSON(keyTaskAd(ns, id), ad); err != nil {
        log.Printf("[announce] failed to put taskAd for %s: %v", id, err)
    }

    m := manifestMirror{
        Providers: man.Providers,
        Exp:       exp,
    }
    _ = d.PutJSON(keyP2PManifestMirror(id), m)
}

