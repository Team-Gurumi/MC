package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"  
	"net/http"
	"strings"
)

type FinishClient struct {
	BaseURL string
	Client  *http.Client
	Token   string
}

type httpError struct{ Status string }
func (e *httpError) Error() string { return e.Status }
func (c *FinishClient) post(ctx context.Context, path string, in any, agentID string, leaseToken int64) error {
	b, _ := json.Marshal(in)
	req, _ := http.NewRequestWithContext(ctx, "POST", c.BaseURL+path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	if agentID != "" {
		req.Header.Set("X-Agent-ID", agentID)
	}
	if leaseToken > 0 {
		req.Header.Set("X-Lease-Token", fmt.Sprintf("%d", leaseToken))
	}

	res, err := c.Client.Do(req)
if err != nil {
    // network-ish error: let caller decide to retry
    return fmt.Errorf("finish http request failed: %w", err)
}
defer res.Body.Close()

// 5xx -> temporary, can retry
if res.StatusCode >= 500 {
    return fmt.Errorf("finish http server error: %d", res.StatusCode)
}
// 4xx -> client or logical error, do not retry
if res.StatusCode >= 400 {
    return fmt.Errorf("finish http client error: %d", res.StatusCode)
}
return nil

}


// ShouldRetryFinish reports whether a finish error is worth retrying.
// We only retry for network problems or 5xx.
func ShouldRetryFinish(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	// crude checks; refine with net.Error if you have it
	if strings.Contains(msg, "connection refused") {
		return true
	}
	if strings.Contains(msg, "timeout") {
		return true
	}
	if strings.Contains(msg, "server error") {
		return true
	}
	return false
}
func (c *FinishClient) Report(ctx context.Context, jobID string, status string,
    metrics map[string]any, resultCID string, artifacts []string, errMsg string,
    agentID string, leaseToken int64,) error {

    body := map[string]any{
        "status":          status,
        "metrics":         metrics,
        "result_root_cid": resultCID,
        "artifacts":       artifacts,
    }
    if errMsg != "" {
        body["error"] = errMsg
    }

    return c.post(ctx, "/jobs/"+jobID+"/finish", body, agentID, leaseToken)

}

