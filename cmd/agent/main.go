package main

import (
    "context"
    "flag"
    "log"
    "strings"
    "time"

    "github.com/Team-Gurumi/MC/pkg/agent"
    dhtnode "github.com/Team-Gurumi/MC/pkg/dht"
    "github.com/Team-Gurumi/MC/pkg/task"
)

func main() {
    ns := flag.String("ns", "default", "namespace")
    discEvery := flag.Duration("discover-every", 5*time.Second, "discover interval")
    bootstrapPeers := flag.String("bootstrap", "", "comma-separated bootstrap peers")
    flag.Parse()

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    d, err := initDHTNode(ctx, *ns, *bootstrapPeers)
    if err != nil {
        log.Fatalf("failed to init DHT node: %v", err)
    }

    dv := agent.NewDiscoverer(d, *ns, *discEvery)
    listIDs := func() []string { return agent.ListFromIndex(d, *ns) }

    onCandidate := func(jobID string, providers []task.Provider) {
        log.Printf("[agent] candidate job=%s providers=%d", jobID, len(providers))
    }

    go dv.Run(ctx, listIDs, onCandidate)
    <-ctx.Done()
}

func initDHTNode(ctx context.Context, ns, bootstrapPeers string) (*dhtnode.Node, error) {
    var addrs []string
    if bootstrapPeers != "" {
        for _, s := range strings.Split(bootstrapPeers, ",") {
            s = strings.TrimSpace(s)
            if s != "" {
                addrs = append(addrs, s)
            }
        }
    }
    node, err := dhtnode.NewNode(ctx, ns, addrs)
    if err != nil {
        return nil, err
    }

    log.Printf("[agent] p2p node started: %s", node.Host.ID())
    for _, a := range node.Multiaddrs() {
        log.Printf("[agent] listen: %s", a)
    }
    return node, nil
}

