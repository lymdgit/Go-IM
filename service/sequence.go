/*
Package service - 消息序列号服务

=== 为什么需要序列号？===

在分布式系统中，消息可能通过不同路径到达，导致乱序：

	时间 ─────────────────────────────────────────▶

	发送端:    消息1 ────▶ 消息2 ────▶ 消息3
	            │          │          │
	            ▼          ▼          ▼
	网络:      路径A      路径B      路径A
	            │          │          │
	            ▼          ▼          ▼
	接收端:    消息1 ────▶ 消息3 ────▶ 消息2  (乱序!)

解决方案：为每条消息分配递增的序列号

	发送端:    消息(seq=1) → 消息(seq=2) → 消息(seq=3)
	                           ↓
	接收端按序列号排序:    1 → 2 → 3  ✓

=== Redis INCR 的原子性 ===

多个 Gateway 同时生成序列号时：

	Gateway-1: INCR seq:alice:bob → 返回 1
	Gateway-2: INCR seq:alice:bob → 返回 2  (自动 +1)
	Gateway-1: INCR seq:alice:bob → 返回 3

Redis 的 INCR 是原子操作，保证：
- 不会有两个请求得到相同的值
- 自增操作不会被中断

=== 序列号的作用 ===

1. 消息排序：接收端按 SeqID 排序显示
2. ACK 机制：客户端 ACK 某个 SeqID，表示该 SeqID 之前的消息都已收到
3. 断点续传：客户端记住最后收到的 SeqID，重连后请求后续消息
4. 去重：相同 SeqID 的消息只处理一次
*/
package service

import (
	"context"
	"fmt"
	"log"

	pkgredis "go-im/pkg/redis"
)

// ==================== 常量定义 ====================

const (
	// SequenceKeyPrefix 序列号 Key 前缀
	// 完整 Key 格式: seq:alice:bob （会话维度）
	// 也可以用 seq:group_123 （群聊维度）
	SequenceKeyPrefix = "seq:"
)

// ==================== 结构体定义 ====================

// SequenceManager 序列号管理器
type SequenceManager struct {
	ctx context.Context
}

// ==================== 构造函数 ====================

// NewSequenceManager 创建序列号管理器
func NewSequenceManager() *SequenceManager {
	return &SequenceManager{
		ctx: pkgredis.Context(),
	}
}

// ==================== 序列号生成 ====================

// NextSeq 生成下一个序列号
//
// 使用 Redis INCR 命令：
// - 如果 Key 不存在，会自动创建并从 1 开始
// - 每次调用返回递增的值：1, 2, 3, ...
// - INCR 是原子操作，并发安全
//
// 参数:
//   - conversationID: 会话标识，如 "alice:bob" 或 "group_123"
//
// 返回:
//   - int64: 新生成的序列号
func (m *SequenceManager) NextSeq(conversationID string) (int64, error) {
	key := SequenceKeyPrefix + conversationID

	// INCR: 原子自增并返回新值
	seq, err := pkgredis.Client.Incr(m.ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to generate sequence: %w", err)
	}
	return seq, nil
}

// NextSeqBatch 批量生成序列号
//
// 一次性获取多个序列号，减少 Redis 请求次数
// 适用于批量发送消息的场景
//
// 示例:
//
//	start, end, _ := NextSeqBatch("room:123", 10)
//	// start=1, end=10 → 分配序列号 1,2,3...10
//
// 参数:
//   - conversationID: 会话标识
//   - count: 需要的序列号数量
//
// 返回:
//   - startSeq: 起始序列号
//   - endSeq: 结束序列号
func (m *SequenceManager) NextSeqBatch(conversationID string, count int64) (startSeq int64, endSeq int64, err error) {
	key := SequenceKeyPrefix + conversationID

	// INCRBY: 原子自增指定值
	endSeq, err = pkgredis.Client.IncrBy(m.ctx, key, count).Result()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to generate sequence batch: %w", err)
	}

	// 计算起始序列号
	startSeq = endSeq - count + 1
	return startSeq, endSeq, nil
}

// ==================== 查询 ====================

// GetCurrentSeq 获取当前序列号（不自增）
//
// 用于查询当前进度，不会改变序列号
func (m *SequenceManager) GetCurrentSeq(conversationID string) (int64, error) {
	key := SequenceKeyPrefix + conversationID

	seq, err := pkgredis.Client.Get(m.ctx, key).Int64()
	if err != nil {
		if err.Error() == "redis: nil" {
			// Key 不存在，返回 0
			return 0, nil
		}
		return 0, err
	}
	return seq, nil
}

// ==================== 重置（仅测试用）====================

// ResetSeq 重置序列号
// 警告：生产环境不应该使用此方法，会导致消息序号重复
func (m *SequenceManager) ResetSeq(conversationID string) error {
	key := SequenceKeyPrefix + conversationID
	return pkgredis.Client.Del(m.ctx, key).Err()
}

func init() {
	log.Println("[Sequence] Sequence manager initialized")
}
