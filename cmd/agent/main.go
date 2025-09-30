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

func runAgent(d *dhtnode.Node, myPeerID string) error {
	// 주기적으로 인덱스를 보고 queued → claim → run → finish
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	log.Printf("[agent] peer=%s addrs=%v\n", myPeerID, d.Multiaddrs())

	for {
		select {
		case <-d.Context().Done():
			return d.Context().Err()
		case <-ticker.C:
			var idx task.TaskIndex
			if err := d.GetJSON(task.IndexKey, &idx, 3*time.Second); err != nil {
				continue
			}
			for _, id := range idx.IDs {
				// 상태 확인
				var st task.TaskState
				if err := d.GetJSON(task.KeyState(id), &st, 2*time.Second); err != nil {
					continue
				}
				if st.Status != task.StatusQueued {
					continue
				}

				// Claim 시도
				nonce, err := task.Claim(d, id, myPeerID, task.DefaultLeaseTTL)
				if err != nil {
					continue // 다른 에이전트가 선점
				}

				now := time.Now().UTC()
				// assigned로 전이 (CAS)
				assign := func(prev []byte) (bool, []byte, error) {
					var cur task.TaskState
					if len(prev) > 0 {
						_ = json.Unmarshal(prev, &cur)
					}
					if cur.Status != "" && cur.Status != task.StatusQueued {
						return false, nil, nil
					}
					cur.ID = id
					cur.Status = task.StatusAssigned
					cur.AssignedTo = myPeerID
					cur.UpdatedAt = now
					cur.Version++
					next, _ := json.Marshal(cur)
					return true, next, nil
				}
				if err := d.PutJSONCAS(task.KeyState(id), assign); err != nil {
					_ = task.Release(d, id, myPeerID, nonce)
					continue
				}

				// 실제 실행 고루틴
				go func(taskID, nonce string) {
					// 엔드포인트 게시 (예: 로컬 WS 에코 서버가 8080에 떠있다고 가정)
					endp := task.TaskEndpoint{
						TaskID:   taskID,
						Proto:    "ws",
						Endpoint: "ws://127.0.0.1:8080/ws/" + taskID,
						Updated:  time.Now().UTC(),
					}
					_ = d.PutJSON(task.KeyWS(taskID), endp)

					// running 전이
					_ = d.PutJSONCAS(task.KeyState(taskID), func(prev []byte) (bool, []byte, error) {
						var cur task.TaskState
						if len(prev) > 0 {
							_ = json.Unmarshal(prev, &cur)
						}
						if cur.AssignedTo != myPeerID {
							return false, nil, task.ErrLeaseStolen
						}
						now := time.Now().UTC()
						cur.Status = task.StatusRunning
						cur.UpdatedAt = now
						cur.StartedAt = &now
						cur.Version++
						next, _ := json.Marshal(cur)
						return true, next, nil
					})

					// 하트비트 + 실제 작업 실행 모사
					hb := time.NewTicker(task.DefaultHeartbeatGap)
					defer hb.Stop()

					execDone := time.After(5 * time.Second) // 데모: 5초 뒤 완료
					exitCode := 0
					execErr := ""

				runLoop:
					for {
						select {
						case <-hb.C:
							if err := task.Heartbeat(d, taskID, myPeerID, nonce, task.DefaultLeaseTTL); err != nil {
								log.Println("[agent] heartbeat err:", err)
								break runLoop
							}
						case <-execDone:
							break runLoop
						case <-d.Context().Done():
							return
						}
					}

					now2 := time.Now().UTC()
					// finish (아이템포턴트)
					_ = d.PutJSONCAS(task.KeyState(taskID), func(prev []byte) (bool, []byte, error) {
						var cur task.TaskState
						if len(prev) > 0 {
							_ = json.Unmarshal(prev, &cur)
						}
						if cur.AssignedTo != myPeerID {
							return false, nil, task.ErrLeaseStolen
						}
						if cur.Status == task.StatusFinished || cur.Status == task.StatusFailed {
							return true, prev, nil
						}
						cur.UpdatedAt = now2
						cur.FinishedAt = &now2
						if execErr != "" {
							cur.Status = task.StatusFailed
							cur.Error = execErr
							ec := 1
							cur.ExitCode = &ec
						} else {
							cur.Status = task.StatusFinished
							cur.ExitCode = &exitCode
						}
						cur.Version++
						next, _ := json.Marshal(cur)
						return true, next, nil
					})

					_ = task.Release(d, taskID, myPeerID, nonce)
					log.Printf("[agent] finished task=%s\n", taskID)
				}(id, nonce)
			}
		}
	}
}

func main() {
	var (
		ns        = flag.String("ns", "mc", "DHT namespace prefix")
		bootstrap = flag.String("bootstrap", "", "comma-separated bootstrap multiaddrs")
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

	myPeerID := node.Host.ID().String()
	fmt.Println("[agent] PeerID:", myPeerID)
	for _, a := range node.Multiaddrs() {
		fmt.Println("[agent] addr:", a)
	}

	if err := runAgent(node, myPeerID); err != nil {
		log.Fatal(err)
	}
}
