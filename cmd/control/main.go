package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Team-Gurumi/MC/pkg/demand"
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
	// 1) DHT에서 manifest를 읽어본다
	var man task.Manifest
	err := m.d.GetJSON(task.KeyManifest(id), &man, 2*time.Second)

	// 2) 없으면 placeholder 만들어서 DHT에 써둔다
	if err != nil || man.RootCID == "" {
		man = task.Manifest{
			RootCID:   "noop",
			Version:   1,
			UpdatedAt: time.Now().UTC(),
		}
		// 에이전트가 /task/<id>/manifest 를 꼭 찾으니까 여기서 써주는 게 중요
		_ = m.d.PutJSON(task.KeyManifest(id), man)
	}

	// 3) 실제 광고
	announceAds(ctx, m.d, m.ns, id, &man, m.ttl)

	// 4) 광고한 시간 기록
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

	return d.PutJSONCAS(task.KeyIndex(ns), func(prev []byte) (bool, []byte, error) {
		var idx task.TaskIndex
		if len(prev) > 0 {
			_ = json.Unmarshal(prev, &idx)
		}
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

func restoreJobsFromDemand(ctx context.Context, store demand.Store, d *dhtnode.Node, ns string, mgr *AnnounceManager) {
	jobs, err := store.ListQueued(ctx)
	if err != nil {
		log.Printf("[restore] list queued from db failed: %v", err)
		return
	}
	if len(jobs) == 0 {
		log.Printf("[restore] no queued jobs to re-announce")
		return
	}

	for _, j := range jobs {
		meta := task.TaskMeta{
			ID:        j.ID,
			Image:     j.Image,
			Command:   j.Command,
			CreatedAt: j.CreatedAt,
		}
		if err := d.PutJSON(task.KeyMeta(j.ID), meta); err != nil {
			log.Printf("[restore] dht put meta failed id=%s: %v", j.ID, err)
		}

		st := task.TaskState{
			ID:        j.ID,
			Status:    task.StatusQueued,
			UpdatedAt: time.Now().UTC(),
			Version:   1,
		}
		_ = d.PutJSON(task.KeyState(j.ID), st)

		_ = addToIndex(d, ns, j.ID)

		mgr.Enqueue(j.ID)
	}
	log.Printf("[restore] re-announced %d queued jobs from DB", len(jobs))
}

const maxRetry = 5

func requeueLoop(ctx context.Context, store demand.Store, d *dhtnode.Node, ns string, mgr *AnnounceManager) {
    ticker := time.NewTicker(500 * time.Millisecond)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            now := time.Now().UTC()

            // 만료된 지 3초 이상 지난 작업만 재큐잉한다
            expired, err := store.ListExpiredLeases(ctx, now.Add(-3*time.Second))
            if err != nil {
                log.Printf("[requeue] list expired failed: %v", err)
                continue
            }

            for _, id := range expired {
                if err := store.SetStatusQueued(ctx, id); err != nil {
                    log.Printf("[requeue] expired -> queued err id=%s: %v", id, err)
                    continue
                }
                // DHT에 있는 lease/state도 정리
                _ = d.DelJSON(task.KeyLease(id))
                updateDHTStateQueued(d, id, now)
                mgr.Enqueue(id)
            }
        }
    }
}


func updateDHTStateQueued(d *dhtnode.Node, id string, now time.Time) {
	var st task.TaskState
	_ = d.GetJSON(task.KeyState(id), &st, 2*time.Second)
	st.ID = id
	st.Status = task.StatusQueued
	st.AssignedTo = ""
	st.UpdatedAt = now
	st.Version++
	_ = d.PutJSON(task.KeyState(id), st)
}

func snapshotLoop(store demand.Store) {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()

	for range t.C {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		jobs, err := store.ListAll(ctx)
		cancel()
		if err != nil {
			log.Printf("[snapshot] list from db failed: %v", err)
			continue
		}
		for _, j := range jobs {
			if j.Status == demand.StatusSucceeded || j.Status == demand.StatusFailed {
				log.Printf("[snapshot] job=%s status=%s", j.ID, j.Status)
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

	dsn := os.Getenv("MC_DB_DSN")
	if dsn == "" {
		log.Fatal("MC_DB_DSN is empty (postgres dsn needed)")
	}
	store, err := demand.NewPGStore(dsn)
	if err != nil {
		log.Fatalf("connect postgres failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, err := dhtnode.NewNode(ctx, *ns, splitCSV(*bootstrap))
	if err != nil {
		log.Fatal(err)
	}
	defer node.Close()

	fmt.Println("[control] PeerID:", node.Host.ID().String())
	for _, a := range node.Multiaddrs() {
		fmt.Println("[control] addr:", a)
	}

	const ttl = 30 * time.Second
	mgr := NewAnnounceManager(node, *ns, ttl, 3*time.Second)
	go mgr.Run(ctx)

	restoreJobsFromDemand(ctx, store, node, *ns, mgr)
	go requeueLoop(ctx, store, node, *ns, mgr)

	mux := mountHTTP(node, store, *ns, mgr.Enqueue)
	addr := fmt.Sprintf(":%d", *httpPort)
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		fmt.Printf("[control] http listening %s\n", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()
	defer srv.Shutdown(ctx)

	if *createStr != "" {
		var cmd []string
		for _, c := range strings.Split(*cmdStr, ",") {
			c = strings.TrimSpace(c)
			if c != "" {
				cmd = append(cmd, c)
			}
		}
		for _, id := range strings.Split(*createStr, ",") {
			id = strings.TrimSpace(id)
			if id == "" {
				continue
			}

			if err := createTask(node, *ns, id, *image, cmd); err != nil {
				log.Println("[control] create error:", err)
			} else {
				log.Println("[control] created task:", id)
			}

			_ = store.CreateJob(ctx, demand.DBJob{
				ID:        id,
				Image:     *image,
				Command:   cmd,
				Status:    demand.StatusQueued,
				CreatedAt: time.Now().UTC(),
			})

			// ★ 여기서도 즉시 광고
			mgr.Enqueue(id)
		}
	}

	select {}
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	var out []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}


