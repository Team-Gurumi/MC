package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
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
	AgentID string `json:"agent_id"`
	Nonce   string `json:"nonce"`
	TTLSec  int    `json:"ttl_sec"`
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

func finishHandler(d *dhtnode.Node, ns string) http.HandlerFunc {
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

		// 상태 갱신
		var st task.TaskState
		_ = d.GetJSON(task.KeyState(id), &st, 2*time.Second)
		st.ID = id
		switch strings.ToLower(in.Status) {
		case "succeeded":
			st.Status = task.StatusSucceeded
		default:
			st.Status = task.StatusFailed
		}
		now := time.Now().UTC()
		st.UpdatedAt = now
		st.Version++
		st.FinishedAt = &now

		 if st.StartedAt != nil { // st.StartedAt이 포인터이므로 nil인지 확인
			if in.Metrics == nil {
				in.Metrics = make(map[string]any)
			}
			in.Metrics["duration_ms"] = now.Sub(*st.StartedAt).Milliseconds()
		}

		//메트릭/결과 저장
		if in.Metrics != nil {
			
			st.Metrics = in.Metrics
			_ = d.PutJSON(fmt.Sprintf("task/%s/metrics", id), in.Metrics)
		}
		if in.ResultCID != "" {
			_ = d.PutJSON(fmt.Sprintf("task/%s/result_root", id), map[string]string{"cid": in.ResultCID})
			 st.ResultRootCID = in.ResultCID
		}
		if len(in.Artifacts) > 0 {
			_ = d.PutJSON(fmt.Sprintf("task/%s/artifacts", id), in.Artifacts)
		}
		_ = d.PutJSON(task.KeyState(id), st)
_ = d.DelJSON(task.KeyLease(id))

		w.WriteHeader(http.StatusNoContent)
	}
}

func ttlOrDefault(sec int) time.Duration {
	if sec <= 0 || sec > 3600 {
		return 30 * time.Second
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
	Exp       time.Time `json:"exp"`
	Sig       string    `json:"sig,omitempty"`
}
type manifestMirror struct {
    Providers []task.Provider `json:"providers"`
    Exp       time.Time       `json:"exp"`
}
// p2p/<id>/manifest 미러: P2P 좌표만 짧게
type manifestAd struct {
	RootCID    string        `json:"root_cid"`
	Providers  []task.Provider `json:"providers"`
	Rendezvous string        `json:"rendezvous,omitempty"`
	Transports []string      `json:"transports,omitempty"`
	Exp        time.Time     `json:"exp"`
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

func addToIndex(d *dhtnode.Node, ns, id string) error {
	var idx task.TaskIndex
	key := task.KeyIndex(ns) //
	if err := d.GetJSON(key, &idx, 2*time.Second); err != nil {

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
			
			CreatedAt: time.Now().UTC(),
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
		if err := addToIndex(d, ns, req.ID); err != nil {
			http.Error(w, "index update failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusCreated, map[string]any{"job_id": req.ID})
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

		
		var metrics map[string]any
		_ = d.GetJSON(fmt.Sprintf("task/%s/metrics", id), &metrics, 2*time.Second)
		if metrics == nil {
			metrics = make(map[string]any)
		}

		var artifacts []string
		_ = d.GetJSON(fmt.Sprintf("task/%s/artifacts", id), &artifacts, 2*time.Second)
		if artifacts == nil {
			artifacts = make([]string, 0)
		}

		var resultRoot map[string]string
		_ = d.GetJSON(fmt.Sprintf("task/%s/result_root", id), &resultRoot, 2*time.Second)

		
		var result any
		if cid, ok := resultRoot["cid"]; ok && cid != "" {
			result = map[string]string{"root_cid": cid}
		} else {
			result = make(map[string]any) // Return empty object instead of null
		}

		var man task.Manifest
		hasManifest := false
		if err := d.GetJSON(task.KeyManifest(id), &man, 2*time.Second); err == nil && man.RootCID != "" {
			hasManifest = true
		}

		var outMan *manifestOutDTO
		if hasManifest {
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

		st.Metrics = nil

		resp := map[string]any{
			"meta":      meta,
			"state":     st,
			"metrics":   metrics,
			"artifacts": artifacts,
			"result":    result,
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
		/*
		// providers가 비어 있어도 허용하도록 주석 처리
		if len(in.Providers) == 0 {
			http.Error(w, "providers required (seeders deprecated)", http.StatusBadRequest)
			return
		}
		*/
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

func tryClaimHandler(d *dhtnode.Node, ns string) http.HandlerFunc {
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
		ttl := ttlOrDefault(in.TTLSec)

		var cur task.Lease
		_ = d.GetJSON(task.KeyLease(id), &cur, 2*time.Second)

		now := time.Now().UTC()
		alive := cur.Owner != "" && cur.Expires.After(now)
		if alive && cur.Owner != in.AgentID {
			http.Error(w, "conflict: owned", http.StatusConflict)
			return
		}

		nz, err := newNonce()
		if err != nil {
			http.Error(w, "nonce err", http.StatusInternalServerError)
			return
		}

		next := task.Lease{
			Owner:   in.AgentID,
			Nonce:   nz,
			Expires: now.Add(ttl),
			Version: cur.Version + 1,
		}

		if err := d.PutJSON(task.KeyLease(id), next); err != nil {
			http.Error(w, "store err: "+err.Error(), http.StatusInternalServerError)
			return
		}

		var st task.TaskState
		_ = d.GetJSON(task.KeyState(id), &st, 2*time.Second)
		if st.ID != "" {
			st.Status = task.StatusAssigned
			st.AssignedTo = in.AgentID
			st.UpdatedAt = now
			
			if st.StartedAt == nil { 
    st.StartedAt = &now 
}
			st.Version++
			_ = d.PutJSON(task.KeyState(id), st)
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(leaseOut{OK: true, Lease: next})
	}
}

func heartbeatHandler(d *dhtnode.Node, ns string) http.HandlerFunc {
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

		var cur task.Lease
		if err := d.GetJSON(task.KeyLease(id), &cur, 2*time.Second); err != nil || cur.Owner == "" {
			http.Error(w, "no lease", http.StatusNotFound)
			return
		}
		if cur.Owner != in.AgentID || cur.Nonce != in.Nonce {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}

		cur.Expires = time.Now().UTC().Add(ttl)
		cur.Version++

		if err := d.PutJSON(task.KeyLease(id), cur); err != nil {
			http.Error(w, "store err: "+err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(leaseOut{OK: true, Lease: cur})
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

		cur.Expires = time.Now().UTC()
		cur.Version++
		_ = d.PutJSON(task.KeyLease(id), cur)

		var st task.TaskState
		_ = d.GetJSON(task.KeyState(id), &st, 2*time.Second)
		if st.ID != "" && st.AssignedTo == in.AgentID {
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
	// TODO: Control이 발급/검증하는 HMAC 또는 Ed25519 서명 토큰으로 교체
	return len(token) > 10 && strings.Contains(token, taskID)
}

func mountHTTP(d *dhtnode.Node, ns string, enqueue func(string)) *http.ServeMux {
	mux := http.NewServeMux()
mux.HandleFunc("/api/health", healthHandler)
mux.Handle("/api/tasks", createTaskHandler(d, ns))
	mux.Handle("/api/tasks/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		if r.Method == http.MethodPost {
			if strings.HasSuffix(r.URL.Path, "/try-claim") {
				tryClaimHandler(d, ns).ServeHTTP(w, r)
				return
			}
			if strings.HasSuffix(r.URL.Path, "/heartbeat") {
				heartbeatHandler(d, ns).ServeHTTP(w, r)
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
		getTaskHandler(d).ServeHTTP(w, r)
	}))
	mux.Handle("/jobs/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/manifest") {
			manifestHandler(d, enqueue).ServeHTTP(w, r)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/finish") {
			finishHandler(d, ns).ServeHTTP(w, r)
			return
		}
		http.NotFound(w, r)
	}))

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
		Namespace: ns,
		Topic:     man.Rendezvous,
		Exp:       exp,
		// Sig:    TODO: 서명 붙이기
	}
	if err := d.PutJSON(keyTaskAd(ns, id), ad); err != nil {

		// log.Printf("[ad] task_ad put err: %v", err)
	}

	
	m := manifestMirror{
      Providers: man.Providers,
      Exp:       exp,
  }
  if err := d.PutJSON(keyP2PManifestMirror(id), m); err != nil {
	
		// log.Printf("[ad] p2p manifest mirror put err: %v", err)
	}
}
