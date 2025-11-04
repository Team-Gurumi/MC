package agent

import (
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "net/http"
    "time"

    task "github.com/Team-Gurumi/MC/pkg/task"
)

type Lease = task.Lease

type ClaimClient interface {
    TryClaim(ctx context.Context, taskID, agentID string, ttl time.Duration) (*Lease, error)
    Heartbeat(ctx context.Context, taskID, agentID, nonce string, ttl time.Duration) (*Lease, error)
    Release(ctx context.Context, taskID, agentID, nonce string) error
}

type HTTPClaimClient struct {
    BaseURL string
    Client  *http.Client
    Token   string // (옵션) Authorization: Bearer <Token>
}

func (c *HTTPClaimClient) post(ctx context.Context, path string, in, out any) error {
    b, _ := json.Marshal(in)
    req, _ := http.NewRequestWithContext(ctx, "POST", c.BaseURL+path, bytes.NewReader(b))
    req.Header.Set("Content-Type", "application/json")
    if c.Token != "" { req.Header.Set("Authorization", "Bearer "+c.Token) }
    res, err := c.Client.Do(req)
    if err != nil { return err }
    defer res.Body.Close()
    if res.StatusCode/100 != 2 { return fmt.Errorf("http %s: %s", path, res.Status) }
    if out != nil { return json.NewDecoder(res.Body).Decode(out) }
    return nil
}

func (c *HTTPClaimClient) TryClaim(ctx context.Context, taskID, agentID string, ttl time.Duration) (*Lease, error) {
    in := map[string]any{"agent_id": agentID, "ttl_sec": int(ttl.Seconds())}
    var out struct{ OK bool `json:"ok"`; Lease Lease `json:"lease"` }
    if err := c.post(ctx, "/api/tasks/"+taskID+"/try-claim", in, &out); err != nil { return nil, err }
    if !out.OK { return nil, fmt.Errorf("claim not ok") }
    return &out.Lease, nil
}

func (c *HTTPClaimClient) Heartbeat(ctx context.Context, taskID, agentID, nonce string, ttl time.Duration, leaseVersion int64,) (*Lease, error) {
    in := map[string]any{"agent_id": agentID, "nonce": nonce, "ttl_sec": int(ttl.Seconds()), "lease_version": leaseVersion,}
    var out struct{ OK bool `json:"ok"`; Lease Lease `json:"lease"` }
    if err := c.post(ctx, "/api/tasks/"+taskID+"/heartbeat", in, &out); err != nil { return nil, err }
    if !out.OK { return nil, fmt.Errorf("heartbeat not ok") }
    return &out.Lease, nil
}

func (c *HTTPClaimClient) Release(ctx context.Context, taskID, agentID, nonce string) error {
    in := map[string]any{"agent_id": agentID, "nonce": nonce}
    var out struct{ OK bool `json:"ok"` }
    if err := c.post(ctx, "/api/tasks/"+taskID+"/release", in, &out); err != nil { return err }
    if !out.OK { return fmt.Errorf("release not ok") }
    return nil
}
