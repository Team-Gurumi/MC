package agent

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

type RunResult struct {
	ExitCode int
	Stdout   string
	Stderr   string
	Duration time.Duration
}

func RunLocal(ctx context.Context, workDir string, image string, command []string) (*RunResult, error) {
	if len(command) == 0 {
		return nil, fmt.Errorf("empty command")
	}
	cmd := exec.CommandContext(ctx, command[0], command[1:]...)
	cmd.Dir = workDir
	var outBuf, errBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf

	start := time.Now()
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	waitErr := cmd.Wait()
	dur := time.Since(start)

	exit := 0
	if waitErr != nil {
		if ee, ok := waitErr.(*exec.ExitError); ok {
			exit = ee.ExitCode()
		} else {
			exit = -1
		}
	}


	_ = os.MkdirAll(filepath.Join(workDir, "_logs"), 0o755)
	_ = os.WriteFile(filepath.Join(workDir, "_logs", "stdout.log"), outBuf.Bytes(), 0o644)
	_ = os.WriteFile(filepath.Join(workDir, "_logs", "stderr.log"), errBuf.Bytes(), 0o644)

	return &RunResult{
		ExitCode: exit,
		Stdout:   outBuf.String(),
		Stderr:   errBuf.String(),
		Duration: dur,
	}, nil
}
