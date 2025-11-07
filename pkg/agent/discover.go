package agent

import (
	"context"
	"strings"
	"sync"
	"time"
	"log"
	dhtnode "github.com/Team-Gurumi/MC/pkg/dht"
	task "github.com/Team-Gurumi/MC/pkg/task"
)

type Discoverer struct {
	d        *dhtnode.Node
	ns       string
	interval time.Duration
	seen     sync.Map // key: jobID, val: time.Time
}

func NewDiscoverer(d *dhtnode.Node, ns string, interval time.Duration) *Discoverer {
	return &Discoverer{d: d, ns: ns, interval: interval}
}

// discover.go
func ListFromIndex(d *dhtnode.Node, ns string) []string {
	ids := make([]string, 0, 256)

	// 새로 만든 샤드 전부 훑기
	for shard := 0; shard < task.TaskIndexShardCount; shard++ {
		key := task.KeyIndexShard(ns, shard)

		var idx task.TaskIndex
		if err := d.GetJSON(key, &idx, 3*time.Second); err != nil {
            // 이 샤드는 아직 없을 수 있으니 그냥 넘어감
			continue
		}

		ids = append(ids, idx.IDs...)
	}

	// var legacy task.TaskIndex
	// if err := d.GetJSON(task.KeyIndex(ns), &legacy, 3*time.Second); err == nil {
	//     ids = append(ids, legacy.IDs...)
	// }

	return ids
}


func (dv *Discoverer) readTaskAd(ctx context.Context, id string) (*TaskAd, error) {
	var ad TaskAd
	if err := dv.d.GetJSON(KeyTaskAd(dv.ns, id), &ad, 2*time.Second); err != nil {
		return nil, err
	}
	if ad.Exp.Before(time.Now().UTC()) {
		return nil, context.DeadlineExceeded
	}
	return &ad, nil
}

func (dv *Discoverer) readManifestMirror(ctx context.Context, id string) (*ManifestAd, error) {
	var m ManifestAd
	if err := dv.d.GetJSON(KeyP2PManifestMirror(id), &m, 2*time.Second); err != nil {
		return nil, err
	}
	if m.Exp.Before(time.Now().UTC()) {
		return nil, context.DeadlineExceeded
	}
	return &m, nil
}

// 유효 provider만 추림
func filterProviders(ps []task.Provider) []task.Provider {
	out := make([]task.Provider, 0, len(ps))
	for _, p := range ps {
		if p.PeerID == "" || len(p.Addrs) == 0 {
			continue
		}
		bad := false
		for _, a := range p.Addrs {
			if strings.HasPrefix(a, "http://") || strings.HasPrefix(a, "https://") {
				bad = true
				break
			}
		}
		if !bad {
			out = append(out, p)
		}
	}
	return out
}

func (dv *Discoverer) handleJob(ctx context.Context, id string, onCandidate func(jobID string, providers []task.Provider, demandURL string)) {
	if id == "" {
		return
	}
  
    var st task.TaskState
    if err := dv.d.GetJSON(task.KeyState(id), &st, 1*time.Second); err == nil {
        if st.Status != task.StatusQueued {

            return
        }
    }

    if v, ok := dv.seen.Load(id); ok {
        if t, ok2 := v.(time.Time); ok2 && time.Since(t) < 500*time.Millisecond {
            return
        }
    }
	ad, err := dv.readTaskAd(ctx, id)
if err != nil {
    log.Printf("[agent] skip job=%s: cannot read taskAd: %v", id, err)
    return
}

	
	 m, err := dv.readManifestMirror(ctx, id)
  if err != nil {
       return
   }
   providers := filterProviders(m.Providers)

 if len(providers) == 0 {
    onCandidate(id, nil, ad.DemandURL)
} else {
    onCandidate(id, providers, ad.DemandURL)
}

	
	dv.seen.Store(id, time.Now())
}

func (dv *Discoverer) Run(ctx context.Context, listIDs func() []string, onCandidate func(jobID string, providers []task.Provider, demandURL string)) {
	ticker := time.NewTicker(dv.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, id := range listIDs() {
				dv.handleJob(ctx, id, onCandidate)
			}
		}
	}
}
