package dht

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"time"
)

type Client struct {
	Port         int
	BootstrapCSV string
	Timeout      time.Duration
	PythonBin    string
}

func getenv(k, d string) string { if v := os.Getenv(k); v != "" { return v }; return d }

func NewFromEnv() *Client {
	p, _ := strconv.Atoi(getenv("DHT_CLIENT_PORT", "8470"))

	// ← 여기! 타임아웃을 환경변수로 조절 (기본 10초)
	toSec, _ := strconv.Atoi(getenv("DHT_TIMEOUT_SEC", "10"))
	if toSec <= 0 {
		toSec = 10
	}

	return &Client{
		Port:         p,
		BootstrapCSV: getenv("DHT_BOOTSTRAP", ""),
		Timeout:      time.Duration(toSec) * time.Second,
		PythonBin:    getenv("DHT_PY_BIN", "python3"),
	}
}

func (c *Client) pyPrelude() string {
	return `
import os, sys, json, asyncio
pp = os.getenv("DHT_PY_PATH")
if pp: sys.path.insert(0, pp)
from dht_utils import dht_set, dht_get
`
}

func (c *Client) scriptSet(key string, value any) string {
	b, _ := json.Marshal(value)
	return fmt.Sprintf(`%s
async def main():
    port=%d
    key=%q
    val=json.loads(%q)
    bootstrap=%q if %q else None
    await dht_set(port, key, val, bootstrap)
asyncio.run(main())
`, c.pyPrelude(), c.Port, key, string(b), c.BootstrapCSV, c.BootstrapCSV)
}

func (c *Client) scriptGet(key string) string {
	return fmt.Sprintf(`%s
async def main():
    port=%d
    key=%q
    bootstrap=%q if %q else None
    val = await dht_get(port, key, bootstrap)
    print(json.dumps(val or {}))
asyncio.run(main())
`, c.pyPrelude(), c.Port, key, c.BootstrapCSV, c.BootstrapCSV)
}

func (c *Client) SetJSON(ctx context.Context, key string, value any) error {
	cmd := exec.CommandContext(ctx, c.PythonBin, "-c", c.scriptSet(key, value))
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("dht set failed: %v: %s", err, stderr.String())
	}
	return nil
}

func (c *Client) GetJSON(ctx context.Context, key string, out any) error {
	cmd := exec.CommandContext(ctx, c.PythonBin, "-c", c.scriptGet(key))
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("dht get failed: %v: %s", err, stderr.String())
	}
	if stdout.Len() == 0 {
		return nil
	}
	return json.Unmarshal(stdout.Bytes(), out)
}
