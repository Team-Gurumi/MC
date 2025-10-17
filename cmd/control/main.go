package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	dhtnode "github.com/Team-Gurumi/MC/pkg/dht"
	"github.com/Team-Gurumi/MC/pkg/task"
)

type AnnounceManager struct {
	d        *dhtnode.Node
	ns       string             
	ttl      time.Duration       
	interval time.Duration       
	mu       sync.Mutex
	dirty    map[string]struct{} 
	last     map[string]time.Time 
}

func NewAnnounceManager(d *dhtnode.Node, ns string, ttl, interval time.Duration) *AnnounceManager {
	return &AnnounceManager{
		d:        d,
		ns:       ns, 
		ttl:      ttl,
		interval: interval,
		dirty:    make(map[string]struct{}),
		last:     make(map[string]time.Time),
	}
}

func (m *AnnounceManager) Enqueue(id string) {
	m.mu.Lock()
	m.dirty[id] = struct{}{}
	m.mu.Unlock()
	
	   go m.announceOnce(context.Background(), id)
}


func (m *AnnounceManager) announceOnce(ctx context.Context, id string) {
	
	var man task.Manifest
	  if err := m.d.GetJSON(task.KeyManifest(id), &man, 2*time.Second); err != nil || man.RootCID == "" {
        return
    }
	announceAds(ctx, m.d, m.ns, id, &man, m.ttl)

	m.mu.Lock() 
	m.last[id] = time.Now().UTC()
	m.mu.Unlock() 
}

func (m *AnnounceManager) Run(ctx context.Context) {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.mu.Lock()
			// snapshot
			ids := make([]string, 0, len(m.dirty))
			now := time.Now().UTC()
			for id := range m.dirty {
								last := m.last[id]
				if last.IsZero() || now.Sub(last) >= m.interval {
					ids = append(ids, id)
				}
			}
			m.mu.Unlock()

			for _, id := range ids {
				 
				m.announceOnce(ctx, id)
			}
		}
	}
}

func createTask(d *dhtnode.Node, ns string, id string, image string, cmd []string) error {
	now := time.Now().UTC()

	meta := task.TaskMeta{ID: id, Image: image, Command: cmd, CreatedAt: now}
	if err := d.PutJSON(task.KeyMeta(id), meta); err != nil {
		return err
	}
	initSt := task.TaskState{ID: id, Status: task.StatusQueued, UpdatedAt: now, Version: 1}
	if err := d.PutJSON(task.KeyState(id), initSt); err != nil {
		return err
	}

	// index CAS
	return d.PutJSONCAS(task.KeyIndex(ns), func(prev []byte) (bool, []byte, error) { // 
		var idx task.TaskIndex
		if len(prev) > 0 {
			_ = json.Unmarshal(prev, &idx)
		}
		// 중복 방지
		for _, x := range idx.IDs {
			if x == id {
				idx.Version++
				idx.UpdatedAt = now
				next, _ := json.Marshal(idx)
				return true, next, nil
			}
		}
		idx.IDs = append(idx.IDs, id)
		idx.Version++
		idx.UpdatedAt = now
		next, _ := json.Marshal(idx)
		return true, next, nil
	})
}
// Corrected Code
func requeueLoop(ctx context.Context, d *dhtnode.Node, ns string, mgr *AnnounceManager) {
    ticker := time.NewTicker(2 * time.Second) // 2초마다 검사
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            var idx task.TaskIndex
            if err := d.GetJSON(task.KeyIndex(ns), &idx, 2*time.Second); err != nil {
                continue
            }
            now := time.Now().UTC()
            for _, id := range idx.IDs {
                var st task.TaskState
                if err := d.GetJSON(task.KeyState(id), &st, 2*time.Second); err != nil {
                    continue
                }
               
                // The '+' character has been removed from the line below
                if st.Status == task.StatusAssigned {
                    var l task.ClaimRecord
                    err := d.GetJSON(task.KeyLease(id), &l, 2*time.Second)

                    // ★ 만료 판단: (1) 읽기 에러(리스 없음) OR (2) owner 없음 OR (3) 만료시간 없음 OR (4) now >= l.Expires
                    expiredOrMissing := err != nil || l.Owner == "" || l.Expires.IsZero() || !l.Expires.After(now)
                    if !expiredOrMissing {
                        continue
                    }

                    // 재큐잉
                    st.Status = task.StatusQueued
                    st.AssignedTo = ""
                    st.UpdatedAt = now
                    st.Version++
                    _ = d.PutJSON(task.KeyState(id), st)

                    // 발견 재개(미러 TTL도 announce에서 갱신됨)
                    mgr.Enqueue(id)
                    log.Printf("[requeue] lease missing/expired -> queued: %s", id)
                }
            }
        }
    }
}
func snapshotLoop(d *dhtnode.Node, ns string) {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for range t.C {
		var idx task.TaskIndex
		// TODO: 프로덕션에서 사용할 경우 네임스페이스 인식 기능도 필요
		
		if err := d.GetJSON(task.KeyIndex(ns), &idx, 3*time.Second); err != nil {
			continue
		}
		for _, id := range idx.IDs {
			var st task.TaskState
			if err := d.GetJSON(task.KeyState(id), &st, 2*time.Second); err != nil {
				continue
			}
			if st.Status == task.StatusFinished || st.Status == task.StatusFailed {
				// TODO: 여기에 DB upsert 등 스냅샷 로직
				log.Printf("[control] snapshot %s status=%s ver=%d\n", id, st.Status, st.Version)
			}
		}
	}
}

func main() {


	var (
		ns        = flag.String("ns", "mc", "DHT namespace prefix")
		bootstrap = flag.String("bootstrap", "", "comma-separated bootstrap multiaddrs")
		createStr = flag.String("create", "", "comma-separated task IDs to create")
		image     = flag.String("image", "alpine", "container image")
		cmdStr    = flag.String("cmd", "echo,hello", "comma-separated command")
		httpPort  = flag.Int("http-port", 8080, "HTTP listening port") 
	)
	flag.Parse()

	var boots []string
	if *bootstrap != "" {
		for _, s := range strings.Split(*bootstrap, ",") {
			s = strings.TrimSpace(s)
			if s != "" {
				boots = append(boots, s)
			}
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, err := dhtnode.NewNode(ctx, *ns, boots)
	if err != nil {
		log.Fatal(err)
	}
	defer node.Close()

	
   fmt.Println("[control] PeerID:", node.Host.ID().String())
    for _, a := range node.Multiaddrs() {
        fmt.Println("[control] addr:", a)
    }

    //추가: 트스트랩 피어가 비어 있으면 스스로 루트 노드로 동작
    if len(boots) == 0 {
        log.Println("[control] no bootstrap peers provided; acting as DHT seed node")
    } else {
        // 여러 피어를 부트스트랩에 연결
        for _, maddr := range boots {
            if err := node.Connect(ctx, maddr); err != nil {
                log.Printf("[control] bootstrap connect failed %s: %v", maddr, err)
            } else {
                log.Printf("[control] connected bootstrap peer %s", maddr)
            }
        }
    }


	// AnnounceManager 생성 및 실행
	const ttl = 30 * time.Second
	const interval = ttl / 2
	mgr := NewAnnounceManager(node, *ns, 30*time.Second /*ttl*/, 3*time.Second /*interval*/)

go mgr.Run(ctx)
go requeueLoop(ctx, node, *ns, mgr) // 리스 만료 감시 루프

	// HTTP 서버 기동 
	mux := mountHTTP(node, *ns, mgr.Enqueue)
addr := fmt.Sprintf(":%d", *httpPort)
srv := &http.Server{Addr: addr, Handler: mux}
go func() {
    fmt.Printf("[control] http listening %s\n", addr)
    if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
        log.Fatal(err)
    }
}()
	defer srv.Shutdown(ctx) 

	// 작업 생성
	if *createStr != "" {
		ids := strings.Split(*createStr, ",")
		var cmd []string
		for _, c := range strings.Split(*cmdStr, ",") {
			c = strings.TrimSpace(c)
			if c != "" {
				cmd = append(cmd, c)
			}
		}
		for _, id := range ids {
			id = strings.TrimSpace(id)
			if id == "" {
				continue
			}
			if err := createTask(node, *ns, id, *image, cmd); err != nil { 
				log.Println("[control] create error:", err)
			} else {
				log.Println("[control] created task:", id)
			}
		}
	}

	// 스냅샷 루프
	snapshotLoop(node, *ns)
}
