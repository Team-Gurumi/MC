package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	dhtnode "github.com/Team-Gurumi/MC/pkg/dht"
	"github.com/Team-Gurumi/MC/pkg/task"
	peer "github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

type providerDTO struct {
	PeerID string   `json:"peer_id"`          // 12D3Koo... 형식
	Addrs  []string `json:"addrs"`            // libp2p multiaddrs
	Relays []string `json:"relays,omitempty"` // optional
	Caps   []string `json:"caps,omitempty"`   // optional: "mc-get/1.0.0", "graphsync", ...
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
	EncMeta string `json:"enc_meta"` // 반드시 agent 권한에서만
}

var allowedTransports = map[string]bool{
	"quic-v1": true,
	"webrtc":  true,
}

// ---- DHT 광고 페이로드 (짧은 TTL/exp 포함) ----

// TASK_AD: 작업 발견용 얕은 광고 (메타만)
type taskAd struct {
	JobID     string    `json:"job_id"`
	Namespace string    `json:"ns,omitempty"`
	DemandURL string    `json:"demand_url,omitempty"` // Control API base (옵션)
	Topic     string    `json:"topic,omitempty"`      // Rendezvous 토픽(있으면)
	Exp       time.Time `json:"exp"`                  // 만료(UTC)
	Sig       string    `json:"sig,omitempty"`        // (TODO) 서명
}

// p2p/<id>/manifest 미러: P2P 좌표만 짧게
type manifestAd struct {
	RootCID    string        `json:"root_cid"`
	Providers  []task.Provider `json:"providers"`
	Rendezvous string        `json:"rendezvous,omitempty"`
	Transports []string      `json:"transports,omitempty"`
	Exp        time.Time     `json:"exp"` // 만료(UTC)
}

// ---- 키 유틸 ----
// 네임스페이스는 ENV로 받아 쓰되, 없으면 "default"
func getNamespace() string {
	if v := os.Getenv("MC_NS"); v != "" {
		return v
	}
	return "default"
}

// Control의 외부 접근 base URL을 ENV로 주입(옵션)
func getDemandURL() string {
	return os.Getenv("MC_DEMAND_URL") // e.g. https://control.example.com
}

func keyTaskAd(ns, id string) string        { return "ad/" + ns + "/task/" + id }
func keyP2PManifestMirror(id string) string { return "p2p/" + id + "/manifest" }

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

func addToIndex(d *dhtnode.Node, ns, id string) error {
	var idx task.TaskIndex
	key := task.KeyIndex(ns) // 
	if err := d.GetJSON(key, &idx, 2*time.Second); err != nil {
		// not found -> 새로 생성
		idx = task.TaskIndex{IDs: []string{id}, UpdatedAt: time.Now(), Version: 1}
		return d.PutJSON(key, idx)
	}
	for _, e := range idx.IDs {
		if e == id {
			return nil
		}
	}
	idx.IDs = append(idx.IDs, id)
	idx.UpdatedAt = time.Now()
	idx.Version++
	return d.PutJSON(key, idx)
}

// ===== /api/tasks =====

type CreateTaskReq struct {
	ID      string   `json:"id"`
	Image   string   `json:"image"`
	Command []string `json:"command"`
}

func createTaskHandler(d *dhtnode.Node, ns string) http.HandlerFunc {
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
			Image:   req.Image,
			Command: req.Command,
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
		  _ = addToIndex(d, ns, req.ID)
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
		var result map[string]any
		_ = d.GetJSON(fmt.Sprintf("task/%s/result", id), &result, 1*time.Second)

		// manifest 읽기 (없으면 스킵)
		var man task.Manifest
		hasManifest := false
		if err := d.GetJSON(task.KeyManifest(id), &man, 2*time.Second); err == nil && man.RootCID != "" {
			hasManifest = true
		}

		var outMan *manifestOutDTO
		if hasManifest {
			// EncMeta는 여기서 절대 노출하지 않음
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

		// 기존 응답 페이로드에 manifest를 추가
		resp := map[string]any{
			"meta":   meta,
			"state":  st,
			"result": result,
		}
		if outMan != nil {
			resp["manifest"] = outMan
		}

		writeJSON(w, http.StatusOK, resp)

	}
}

func manifestHandler(d *dhtnode.Node, enqueue func(string)) http.HandlerFunc {
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
		if len(in.Providers) == 0 {
			http.Error(w, "providers required (seeders deprecated)", http.StatusBadRequest)
			return
		}
		if err := validateTransports(in.Transports); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		for i, pv := range in.Providers {
			if err := validateProvider(pv); err != nil {
				http.Error(w, fmt.Sprintf("provider[%d]: %v", i, err), http.StatusBadRequest)
				return
			}
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

		// 여기서 바로 dht에 광고하지 않고, 매니저 큐에 요청만 넣는다.
		enqueue(id)

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

// ===== POST /jobs/{id}/finish =====

func finishHandler(d *dhtnode.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requireAuth(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
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

		if cid, ok := p["result_root_cid"].(string); ok && cid != "" {
			_ = d.PutJSON(fmt.Sprintf("task/%s/result", id), map[string]any{"cid": cid})
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

// --- [새로운 핸들러 추가 시작] ---
func agentManifestHandler(d *dhtnode.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// 경로: /internal/agents/{id}/manifest
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(parts) != 4 || parts[0] != "internal" || parts[1] != "agents" || parts[3] != "manifest" {
			http.Error(w, "bad path", http.StatusBadRequest)
			return
		}
		id := parts[2]

		// 임시 토큰 검증 (다음 단계에서 강화/대체 예정)
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

		// enc_meta 포함하여 반환(에이전트 전용)
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

// 최소 토큰 검증(임시). 다음 단계에서 실제 서명/키 교환으로 교체.
func validateAgentToken(token, taskID string) bool {
	// TODO: Control이 발급/검증하는 HMAC 또는 Ed25519 서명 토큰으로 교체
	return len(token) > 10 && strings.Contains(token, taskID)
}

// --- [새로운 핸들러 추가 끝] ---

// ===== 라우팅 등록 =====

 func mountHTTP(d *dhtnode.Node, ns string, enqueue func(string)) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/api/tasks", createTaskHandler(d, ns))
	mux.Handle("/api/tasks/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/logs") {
			logsHandler(d).ServeHTTP(w, r)
			return
		}
		getTaskHandler(d).ServeHTTP(w, r)
	}))

	mux.Handle("/jobs/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/manifest") {
			manifestHandler(d, enqueue).ServeHTTP(w, r)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/finish") {
			finishHandler(d).ServeHTTP(w, r)
			return
		}
		http.NotFound(w, r)
	}))

	// --- [라우팅 추가] ---
	mux.HandleFunc("/internal/agents/", agentManifestHandler(d).ServeHTTP)

	return mux
}

// 유효성 검증
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
	// 만료 시각(UTC)
	exp := time.Now().UTC().Add(ttl)

	// 1) TASK_AD
	ad := taskAd{
		JobID:     id,
		Namespace: ns, // 
		DemandURL: getDemandURL(),
		Topic:     man.Rendezvous, // 있으면 채움
		Exp:       exp,
		// Sig:    TODO: 서명 붙이기(다음 단계)
	}
	if err := d.PutJSON(keyTaskAd(ns, id), ad); err != nil {
		// 실패는 로그만 — 광고는 베스트에포트
		// log.Printf("[ad] task_ad put err: %v", err)
	}

	// 2) p2p/<id>/manifest 미러 (Providers만 싣는 얕은 페이로드)
	m := manifestAd{
		RootCID:    man.RootCID,
		Providers:  man.Providers,
		Rendezvous: man.Rendezvous,
		Transports: man.Transports,
		Exp:        exp,
	}
	if err := d.PutJSON(keyP2PManifestMirror(id), m); err != nil {
		// log.Printf("[ad] p2p manifest mirror put err: %v", err)
	}
}
