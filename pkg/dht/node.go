package dht

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
    libp2p "github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	protocol "github.com/libp2p/go-libp2p/core/protocol"
	ma "github.com/multiformats/go-multiaddr"
	"math/rand"
	"time"
)

type Node struct {
	ctx       context.Context
	cancel    context.CancelFunc
	Host      host.Host
	DHT       *dht.IpfsDHT
	namespace string
}

type noopValidator struct{}

func (noopValidator) Validate(string, []byte) error        { return nil }
func (noopValidator) Select(string, [][]byte) (int, error) { return 0, nil }

// NewNode: 부트스트랩 멀티addr 목록을 받아 DHT 노드 생성/부트스트랩
func NewNode(parent context.Context, namespace string, bootstrapAddrs []string) (*Node, error) {
	ctx, cancel := context.WithCancel(parent)

	var (
		h      host.Host
		dhtVal *dht.IpfsDHT // 변수 이름을 dht에서 dhtVal로 변경하여 import 이름과 충돌 방지
		err    error
	)


// --- 멀티 인터페이스 IP 탐지 및 광고 ---
ips := detectIPv4Addrs()
fmt.Printf("[dht] detected IPs: %v\n", ips)

var opts []libp2p.Option
opts = append(opts,
    libp2p.ListenAddrStrings(
        "/ip4/0.0.0.0/tcp/0",
        "/ip4/0.0.0.0/udp/0/quic-v1",
    ),
)

opts = append(opts, libp2p.AddrsFactory(func(in []ma.Multiaddr) []ma.Multiaddr {
    out := make([]ma.Multiaddr, 0, len(in)*len(ips))
    for _, a := range in {
        // 1) TCP인지 확인
        if p, err := a.ValueForProtocol(ma.P_TCP); err == nil {
            // p는 문자열 포트 번호
            for _, ip := range ips {
                if m, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%s", ip, p)); err == nil {
                    out = append(out, m)
                }
            }
            continue
        }

        // 2) UDP(+QUIC)인지 확인
        if p, err := a.ValueForProtocol(ma.P_UDP); err == nil {
            isQuicV1 := strings.Contains(a.String(), "/quic-v1")
            for _, ip := range ips {
                var s string
                if isQuicV1 {
                    s = fmt.Sprintf("/ip4/%s/udp/%s/quic-v1", ip, p)
                } else {
                    s = fmt.Sprintf("/ip4/%s/udp/%s", ip, p)
                }
                if m, err := ma.NewMultiaddr(s); err == nil {
                    out = append(out, m)
                }
            }
            continue
        }
        // 그 외 프로토콜은 그대로(필요시 생략 가능)
    }
    return out
}))

h, err = libp2p.New(opts...)
if err != nil {
    cancel()
    return nil, fmt.Errorf("libp2p new: %w", err)
}



mode := dht.Mode(dht.ModeAuto)
if len(bootstrapAddrs) == 0 {
    mode = dht.Mode(dht.ModeServer)
}

	dhtVal, err = dht.New(
		ctx,
		h,
		mode,
		dht.ProtocolPrefix(protocol.ID("/"+namespace)),
		dht.Validator(record.NamespacedValidator{
			namespace: noopValidator{},
		}),
	)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("dht new: %w", err)

	}
	if dhtVal == nil {
		cancel()
		return nil, errors.New("dht not initialized")
	}

	// 부트스트랩 피어 연결

	if len(bootstrapAddrs) > 0 {
		for _, s := range bootstrapAddrs {
			m, err := ma.NewMultiaddr(s)
			if err != nil {
				fmt.Printf("[bootstrap] bad multiaddr: %q: %v\n", s, err)
				continue
			}
			info, err := peer.AddrInfoFromP2pAddr(m)
			if err != nil {
				fmt.Printf("[bootstrap] bad p2p addr: %q: %v\n", s, err)
				continue
			}
			if err := h.Connect(ctx, *info); err != nil {
				fmt.Printf("[bootstrap] connect %s: %v\n", info.ID, err)
			}
		}
	}

	if err := dhtVal.Bootstrap(ctx); err != nil {
		cancel()
		return nil, fmt.Errorf("dht bootstrap: %w", err)
	}

	if len(bootstrapAddrs) > 0 { // ← 부트스트랩 주소가 있을 때만 대기
		deadline := time.Now().Add(5 * time.Second)
		for {
			if dhtVal.RoutingTable() != nil && dhtVal.RoutingTable().Size() > 0 {
				break
			}
			if time.Now().After(deadline) {
				cancel()
				return nil, fmt.Errorf("no peers in routing table after bootstrap")
			}
			time.Sleep(200 * time.Millisecond)
		}
	}

	n := &Node{
		ctx:       ctx,
		cancel:    cancel,
		Host:      h,
		DHT:       dhtVal,
		namespace: namespace,
	}
	return n, nil
}

func detectTailnetIPv4() string {
  ifaces, _ := net.Interfaces()
    for _, iface := range ifaces {
        if iface.Name == "tailscale0" && (iface.Flags&net.FlagUp) != 0 {
            addrs, _ := iface.Addrs()
            for _, a := range addrs {
                if ipnet, ok := a.(*net.IPNet); ok {
                    ip := ipnet.IP.To4()
                    if ip == nil {
                        continue
                    }
                    if ip[0] == 100 && ip[1] >= 64 && ip[1] <= 127 {
                        return ip.String()
                    }
                }
            }
        }
    }
    // tailscale0 못 찾으면 전체 인터페이스에서 100.64/10 검색
    ifaces, _ = net.Interfaces()
    for _, iface := range ifaces {
        if (iface.Flags & net.FlagUp) == 0 {
            continue
        }
        addrs, _ := iface.Addrs()
        for _, a := range addrs {
            if ipnet, ok := a.(*net.IPNet); ok {
                ip := ipnet.IP.To4()
                if ip == nil {
                    continue
                }
                if ip[0] == 100 && ip[1] >= 64 && ip[1] <= 127 {
                    return ip.String()
                }
            }
        }
    }
    return ""
}

func detectIPv4Addrs() []string {
    var out []string
    ifaces, _ := net.Interfaces()
    for _, iface := range ifaces {
        if (iface.Flags & net.FlagUp) == 0 {
            continue
        }
        addrs, _ := iface.Addrs()
        for _, a := range addrs {
            if ipnet, ok := a.(*net.IPNet); ok {
                ip := ipnet.IP.To4()
                if ip == nil {
                    continue
                }
                // tailscale(100.64/10) 또는 사설망만 포함
                if (ip[0] == 100 && ip[1] >= 64 && ip[1] <= 127) || ip.IsPrivate() {
                    out = append(out, ip.String())
                }
            }
        }
    }
    // 항상 루프백도 추가
    out = append(out, "127.0.0.1")
    return out
}


func (n *Node) Context() context.Context { return n.ctx }
func (n *Node) Close()                   { n.cancel(); _ = n.Host.Close() }

func (n *Node) nsKey(key string) string {
	if n.namespace == "" {
		return "/" + key
	}
	return "/" + n.namespace + "/" + key
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
		out = append(out, a.Encapsulate(ma.StringCast("/p2p/"+pid.String())).String())
     
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
func (n *Node) Connect(ctx context.Context, maddrStr string) error {
	m, err := ma.NewMultiaddr(maddrStr)
	if err != nil {
		return fmt.Errorf("bad multiaddr %q: %w", maddrStr, err)
	}
	info, err := peer.AddrInfoFromP2pAddr(m)
	if err != nil {
		return fmt.Errorf("bad p2p addr %q: %w", maddrStr, err)
	}
	return n.Host.Connect(ctx, *info)
}

