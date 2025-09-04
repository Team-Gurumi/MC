APP ?= mc-mutual-cloud
BIN_AGENT := mc-agent
BIN_CTRL  := mc-control

# 빌드
build-agent:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o $(BIN_AGENT) ./cmd/agent
build-control:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o $(BIN_CTRL) ./cmd/control
build: build-agent build-control

# 원격 설치 (에이전트)
# 사용: make install-agent HOST=user@1.2.3.4 CONTROL_URL=http://100.x.x.x:8080
install-agent: build-agent
	scp $(BIN_AGENT) $(HOST):/tmp/$(BIN_AGENT)
	ssh $(HOST) 'sudo install -m 0755 /tmp/$(BIN_AGENT) /usr/local/bin/$(BIN_AGENT)'
	ssh $(HOST) 'cat | sudo tee /etc/systemd/system/mc-agent.service >/dev/null' <<'UNIT'
[Unit]
Description=Mutual Cloud Agent
After=network-online.target docker.service tailscaled.service
Requires=docker.service

[Service]
Environment=DOCKER_HOST=unix:///var/run/docker.sock
Environment=NODE_ID=%H
Environment=CONTROL_URL=$(CONTROL_URL)
ExecStart=/usr/local/bin/mc-agent
Restart=always
RestartSec=3
User=root

[Install]
WantedBy=multi-user.target
UNIT
	ssh $(HOST) 'sudo systemctl daemon-reload && sudo systemctl enable --now mc-agent && sudo systemctl status mc-agent --no-pager -l'

# 원격 설치 (컨트롤 서버)
# 사용: make install-control HOST=user@1.2.3.4 PORT=8080 TIMEOUT=120
PORT ?= 8080
TIMEOUT ?= 120
install-control: build-control
	scp $(BIN_CTRL) $(HOST):/tmp/$(BIN_CTRL)
	ssh $(HOST) 'sudo install -m 0755 /tmp/$(BIN_CTRL) /usr/local/bin/$(BIN_CTRL)'
	ssh $(HOST) 'cat | sudo tee /etc/systemd/system/mc-control.service >/dev/null' <<UNIT
[Unit]
Description=Mutual Cloud Control Server
After=network-online.target

[Service]
Environment=PORT=$(PORT)
Environment=TASK_TIMEOUT_SEC=$(TIMEOUT)
ExecStart=/usr/local/bin/mc-control
Restart=always
RestartSec=3
User=root

[Install]
WantedBy=multi-user.target
UNIT
	ssh $(HOST) 'sudo systemctl daemon-reload && sudo systemctl enable --now mc-control && sudo systemctl status mc-control --no-pager -l'

# 재시작 / 로그
agent-restart:
	ssh $(HOST) 'sudo systemctl restart mc-agent && sudo systemctl status mc-agent --no-pager -l'
control-restart:
	ssh $(HOST) 'sudo systemctl restart mc-control && sudo systemctl status mc-control --no-pager -l'
logs-agent:
	ssh $(HOST) 'journalctl -u mc-agent -f -n 100'
logs-control:
	ssh $(HOST) 'journalctl -u mc-control -f -n 100'
