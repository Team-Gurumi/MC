package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
)

type FinishClient struct {
	BaseURL string
	Client  *http.Client
	Token   string
}

type httpError struct{ Status string }
func (e *httpError) Error() string { return e.Status }

func (c *FinishClient) post(ctx context.Context, path string, in any) error {
	b, _ := json.Marshal(in)
	req, _ := http.NewRequestWithContext(ctx, "POST", c.BaseURL+path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	if c.Token != "" { req.Header.Set("Authorization", "Bearer "+c.Token) }
	res, err := c.Client.Do(req)
	if err != nil { return err }
	defer res.Body.Close()
	if res.StatusCode/100 != 2 { return &httpError{Status: res.Status} }
	return nil
}

func (c *FinishClient) Report(ctx context.Context, jobID string, status string, metrics map[string]any, resultCID string, artifacts []string, errMsg string) error {
	body := map[string]any{
		"status":           status,
		"metrics":          metrics,
		"result_root_cid":  resultCID,
		"artifacts":        artifacts,
	}
	if errMsg != "" { body["error"] = errMsg }
	return c.post(ctx, "/jobs/"+jobID+"/finish", body)
}
	
