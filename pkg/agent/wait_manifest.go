package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/Team-Gurumi/MC/pkg/task"
)

// WaitForManifest polls the Control API until the manifest appears or timeout expires.
func WaitForManifest(ctx context.Context, baseURL, token, taskID string, maxWait time.Duration) (*task.Manifest, error) {
	deadline := time.Now().Add(maxWait)
	client := &http.Client{Timeout: 3 * time.Second}

	for {
		req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/api/tasks/%s", baseURL, taskID), nil)
		if err != nil {
			return nil, err
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}

		resp, err := client.Do(req)
		if err == nil && resp.StatusCode == 200 {
			var out struct {
				Manifest *task.Manifest `json:"manifest"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&out); err == nil {
				resp.Body.Close()
				if out.Manifest != nil && out.Manifest.RootCID != "" && len(out.Manifest.Providers) > 0 {
					return out.Manifest, nil
				}
			} else {
				resp.Body.Close()
			}
		}

		if time.Now().After(deadline) {
			return nil, fmt.Errorf("manifest not ready")
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}
