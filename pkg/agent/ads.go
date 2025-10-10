package agent

import (
	"time"

	task "github.com/Team-Gurumi/MC/pkg/task"
)

// Control이 DHT에 올리는 광고 페이로드와 동일해야 함
type TaskAd struct {
	JobID     string    `json:"job_id"`
	Namespace string    `json:"ns,omitempty"`
	DemandURL string    `json:"demand_url,omitempty"`
	Topic     string    `json:"topic,omitempty"`
	Exp       time.Time `json:"exp"`
	Sig       string    `json:"sig,omitempty"`
}

type ManifestAd struct {
	RootCID    string          `json:"root_cid"`
	Providers  []task.Provider `json:"providers"`
	Rendezvous string          `json:"rendezvous,omitempty"`
	Transports []string        `json:"transports,omitempty"`
	Exp        time.Time       `json:"exp"`
}

// DHT 키 규약(컨트롤과 동일)
func KeyTaskAd(ns, id string) string        { return "ad/" + ns + "/task/" + id }
func KeyP2PManifestMirror(id string) string { return "p2p/" + id + "/manifest" }
