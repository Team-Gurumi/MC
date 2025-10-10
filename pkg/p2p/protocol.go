package p2p

// libp2p protocol ID (버전 포함, 이후 호환성 관리)
const ProtoGet = "/mc-get/1.0.0"

// 요청은 최소 root CID만. (추가: range, piece, merkle path 등은 이후 단계)
type GetRequest struct {
    RootCID string `json:"root_cid"`
}

// 응답 헤더(옵션): 총 길이나 mime 등. (지금은 바디를 그대로 스트리밍)
type GetResponse struct {
    Size int64  `json:"size,omitempty"`
    Err  string `json:"err,omitempty"`
}
