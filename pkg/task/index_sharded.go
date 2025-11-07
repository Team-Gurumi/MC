package task

import "fmt"

// 여러 인덱스에 분산시키기 위한 샤드 개수
const TaskIndexShardCount = 32

// 샤드 하나의 키: 기존 KeyIndex 와 같은 prefix 를 쓰되 뒤에 /<shard> 만 붙인다.
func KeyIndexShard(ns string, shard int) string {
	return fmt.Sprintf("ns/%s/task/index/%02d", ns, shard)
}

// 전 샤드 키 목록을 돌고 싶을 때
func AllIndexShards(ns string) []string {
	out := make([]string, 0, TaskIndexShardCount)
	for i := 0; i < TaskIndexShardCount; i++ {
		out = append(out, KeyIndexShard(ns, i))
	}
	return out
}

