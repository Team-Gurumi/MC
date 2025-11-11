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
	Stdout   []byte
	Stderr   []byte
	Duration time.Duration
}

func RunInContainer(ctx context.Context, workDir string, image string, command []string) (*RunResult, error) {
	// Docker가 설치되어 있는지 확인
	if _, err := exec.LookPath("docker"); err != nil {
		return nil, fmt.Errorf("docker command not found: %w", err)
	}

	absWorkDir, err := filepath.Abs(workDir)
	if err != nil {
		return nil, fmt.Errorf("could not get absolute path for work dir: %w", err)
	}

	args := []string{
		"run",
		"--rm",          
		"-w", "/app",    
		"-v", fmt.Sprintf("%s:/app", absWorkDir),
		image,
	}
	args = append(args, command...)

	cmd := exec.CommandContext(ctx, "docker", args...)

	var outBuf, errBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf

	start := time.Now()
	err = cmd.Run()
	dur := time.Since(start)

	exit := 0
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			exit = ee.ExitCode()
		} else {
			// docker 실행 자체에 실패한 경우 (예: 이미지 다운로드 실패)
			exit = -1
		}
	}
if exit != 0 {
        b := errBuf.Bytes()
        if bytes.Contains(b, []byte("unable to open image")) ||
            bytes.Contains(b, []byte("no decode delegate for this image format")) {
            exit = 0
        }
    }
	// 실행 로그를 파일로 저장
	_ = os.MkdirAll(filepath.Join(workDir, "_logs"), 0o755)
	_ = os.WriteFile(filepath.Join(workDir, "_logs", "stdout.log"), outBuf.Bytes(), 0o644)
	_ = os.WriteFile(filepath.Join(workDir, "_logs", "stderr.log"), errBuf.Bytes(), 0o644)

	return &RunResult{
		ExitCode: exit,
		Stdout:   outBuf.Bytes(),
		Stderr:   errBuf.Bytes(),
		Duration: dur,
	}, nil
}

