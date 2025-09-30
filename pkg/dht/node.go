package dht

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	kaddht "github.com/libp2p/go-libp2p-kad-dht"
	lp2pconfig "github.com/libp2p/go-libp2p/config"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/multiformats/go-multiaddr"
)

type Node struct {
	ctx       context.Context
	cancel    context.CancelFunc
	Host      host.Host
	DHT       *kaddht.IpfsDHT
	namespace string
}

// NewNode: 부트스트랩 멀티addr 목록을 받아 DHT 노드 생성/부트스트랩
func NewNode(parent context.Context, namespace string, bootstrapAddrs []string) (*Node, error) {
	ctx, cancel := context.WithCancel(parent)

	var (
		h   host.Host
		dht *kaddht.IpfsDHT
	)

	// libp2p.Routing은 lp2pconfig.RoutingC 타입을 요구하므로 캐스팅 필수
	h, err := libp2p.New(
		libp2p.Routing(lp2pconfig.RoutingC(
			func(c context.Context, hh host.Host) (routing.Routing, error) {
				dd, err := kaddht.New(c, hh, kaddht.Mode(kaddht.ModeAuto))
				if err == nil {
					dht = dd // 바깥에서 참조
				}
				return dd, err
			},
		)),
	)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("libp2p new: %w", err)
	}
	if dht == nil {
		cancel()
		return nil, errors.New("dht not initialized")
	}

	// 부트스트랩 피어 연결
	if len(bootstrapAddrs) > 0 {
		var peers []peer.AddrInfo
		for _, s := range bootstrapAddrs {
			m, err := multiaddr.NewMultiaddr(s)
			if err != nil {
				continue
			}
			info, err := peer.AddrInfoFromP2pAddr(m)
			if err != nil {
				continue
			}
			peers = append(peers, *info)
		}
		for _, p := range peers {
			_ = h.Connect(ctx, p)
		}
	}

	// DHT 부트스트랩
	if err := dht.Bootstrap(ctx); err != nil {
		cancel()
		return nil, fmt.Errorf("dht bootstrap: %w", err)
	}

	n := &Node{
		ctx:       ctx,
		cancel:    cancel,
		Host:      h,
		DHT:       dht,
		namespace: namespace,
	}
	return n, nil
}

func (n *Node) Context() context.Context { return n.ctx }
func (n *Node) Close()                   { n.cancel(); _ = n.Host.Close() }

func (n *Node) nsKey(key string) string {
	if n.namespace == "" {
		return key
	}
	return n.namespace + "/" + key
}

func (n *Node) withTimeout(d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		d = 5 * time.Second
	}
	return context.WithTimeout(n.ctx, d)
}

// Put: raw bytes
func (n *Node) Put(key string, val []byte, timeout time.Duration) error {
	ctx, cancel := n.withTimeout(timeout)
	defer cancel()
	return n.DHT.PutValue(ctx, n.nsKey(key), val)
}

// Get: raw bytes
func (n *Node) Get(key string, timeout time.Duration) ([]byte, error) {
	ctx, cancel := n.withTimeout(timeout)
	defer cancel()
	return n.DHT.GetValue(ctx, n.nsKey(key))
}

// 내 피어 multiaddr(+/p2p/ID) 목록
func (n *Node) Multiaddrs() []string {
	var out []string
	pid := n.Host.ID()
	for _, a := range n.Host.Addrs() {
		out = append(out, a.Encapsulate(multiaddr.StringCast("/p2p/"+pid.String())).String())
	}
	return out
}

// ---------- CAS & 백오프 ----------

var (
	ErrCASConflict = errors.New("cas conflict")
)

func jitterBackoff(i int) time.Duration {
	base := 100 * time.Millisecond * time.Duration(1<<i)
	j := time.Duration(rand.Intn(200)) * time.Millisecond
	return base + j
}

// PutJSONCAS: check(prev) -> next 생성 -> Put -> Verify 의 재시도 루프
func (n *Node) PutJSONCAS(key string, check func(prev []byte) (ok bool, next []byte, err error)) error {
	var lastErr error
	for i := 0; i < 5; i++ {
		// 1) prev read
		var prev []byte
		{
			ctx, cancel := n.withTimeout(3 * time.Second)
			val, err := n.DHT.GetValue(ctx, n.nsKey(key))
			cancel()
			if err == nil {
				prev = val
			} else {
				prev = nil
			}
		}

		// 2) check & next 생성
		ok, next, err := check(prev)
		if err != nil {
			return err
		}
		if !ok {
			lastErr = ErrCASConflict
			time.Sleep(jitterBackoff(i))
			continue
		}

		// 3) put
		{
			ctx, cancel := n.withTimeout(5 * time.Second)
			err = n.DHT.PutValue(ctx, n.nsKey(key), next)
			cancel()
			if err != nil {
				lastErr = err
				time.Sleep(jitterBackoff(i))
				continue
			}
		}

		// 4) verify
		{
			ctx, cancel := n.withTimeout(3 * time.Second)
			after, err := n.DHT.GetValue(ctx, n.nsKey(key))
			cancel()
			if err != nil {
				lastErr = err
				time.Sleep(jitterBackoff(i))
				continue
			}
			if string(after) != string(next) {
				lastErr = ErrCASConflict
				time.Sleep(jitterBackoff(i))
				continue
			}
		}
		return nil
	}
	if lastErr == nil {
		lastErr = ErrCASConflict
	}
	return lastErr
}
