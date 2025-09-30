package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	dhtnode "github.com/Team-Gurumi/MC/pkg/dht"
	"github.com/Team-Gurumi/MC/pkg/task"
)

func createTask(d *dhtnode.Node, id string, image string, cmd []string) error {
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
	return d.PutJSONCAS(task.IndexKey, func(prev []byte) (bool, []byte, error) {
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

// 중앙은 주기 스냅샷만 수행 (보조자)
func snapshotLoop(d *dhtnode.Node) {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for range t.C {
		var idx task.TaskIndex
		if err := d.GetJSON(task.IndexKey, &idx, 3*time.Second); err != nil {
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

	ctx := context.Background()
	node, err := dhtnode.NewNode(ctx, *ns, boots)
	if err != nil {
		log.Fatal(err)
	}
	defer node.Close()

	fmt.Println("[control] PeerID:", node.Host.ID().String())
	for _, a := range node.Multiaddrs() {
		fmt.Println("[control] addr:", a)
	}

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
			if err := createTask(node, id, *image, cmd); err != nil {
				log.Println("[control] create error:", err)
			} else {
				log.Println("[control] created task:", id)
			}
		}
	}

	// 스냅샷 루프
	snapshotLoop(node)
}
