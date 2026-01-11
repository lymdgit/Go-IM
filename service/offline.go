/*
Package service - 离线消息服务

=== 为什么需要离线消息存储？===

用户不可能 24 小时在线，当用户离线时收到的消息需要：
1. 临时存储在服务端
2. 用户上线后推送给他
3. 用户确认收到后删除

=== Redis ZSet (有序集合) ===

ZSet 是 Redis 的有序集合，每个元素有一个 Score 用于排序：

	Key: msg_box:bob
	┌─────────────────────────────────────┐
	│  Score (SeqID)  │      Member       │
	│─────────────────│───────────────────│
	│       1         │  {"from":"alice"} │
	│       2         │  {"from":"carol"} │
	│       3         │  {"from":"alice"} │
	│       4         │  {"from":"dave"}  │
	└─────────────────────────────────────┘

ZSet 的优势：
- 自动按 Score (SeqID) 排序
- O(log N) 的插入和查询
- 支持范围查询（ZRANGEBYSCORE）
- 支持反向查询（ZREVRANGE）

=== 离线消息流程 ===

1. Alice 给 Bob 发消息
2. 检查 Bob 是否在线
  - 在线：直接推送
  - 离线：存入 msg_box:bob

3. Bob 上线
4. 拉取 msg_box:bob 中的消息
5. Bob 发送 ACK
6. 删除已确认的消息

=== 消息拉取方式 ===

1. ZRANGE: 从旧到新（按 SeqID 升序）
  - 用于同步消息，确保顺序

2. ZREVRANGE: 从新到旧（按 SeqID 降序）
  - 用于"下拉加载历史"的 UI 交互
*/
package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	pkgredis "go-im/pkg/redis"

	"github.com/redis/go-redis/v9"
)

// ==================== 常量定义 ====================

const (
	// OfflineBoxPrefix 离线消息盒子 Key 前缀
	// 完整 Key: msg_box:bob
	OfflineBoxPrefix = "msg_box:"

	// MaxOfflineMessages 每个用户最多存储的离线消息数
	// 超过此数量会删除最旧的消息
	MaxOfflineMessages = 1000

	// OfflineMessageTTL 离线消息过期时间
	// 7 天后自动删除未读消息
	OfflineMessageTTL = 7 * 24 * time.Hour
)

// ==================== 消息结构 ====================

// OfflineMessage 离线消息结构
type OfflineMessage struct {
	FromUserID string    `json:"from_user_id"` // 发送者
	ToUserID   string    `json:"to_user_id"`   // 接收者
	Content    []byte    `json:"content"`      // 消息内容
	MsgType    int       `json:"msg_type"`     // 消息类型
	SeqID      int64     `json:"seq_id"`       // 序列号（用作 ZSet Score）
	Timestamp  time.Time `json:"timestamp"`    // 发送时间
}

// ==================== 管理器结构 ====================

// OfflineManager 离线消息管理器
type OfflineManager struct {
	ctx context.Context
}

// NewOfflineManager 创建离线消息管理器
func NewOfflineManager() *OfflineManager {
	return &OfflineManager{
		ctx: pkgredis.Context(),
	}
}

// ==================== 存储消息 ====================

// Store 存储离线消息
//
// Redis 操作：
// 1. ZADD msg_box:bob SeqID "消息JSON"
// 2. ZREMRANGEBYRANK msg_box:bob 0 -(MaxOfflineMessages+1)  // 保留最新的 N 条
// 3. EXPIRE msg_box:bob 604800  // 7天过期
//
// 参数:
//   - userID: 接收者用户 ID
//   - msg: 离线消息
func (m *OfflineManager) Store(userID string, msg *OfflineMessage) error {
	key := OfflineBoxPrefix + userID
	msg.Timestamp = time.Now()

	// 序列化消息为 JSON
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	// 添加到 ZSet
	// Score = SeqID，用于排序
	// Member = 消息 JSON
	err = pkgredis.Client.ZAdd(m.ctx, key, redis.Z{
		Score:  float64(msg.SeqID),
		Member: string(data),
	}).Err()
	if err != nil {
		return fmt.Errorf("failed to store offline message: %w", err)
	}

	// 限制消息数量（删除最旧的）
	// ZREMRANGEBYRANK key 0 -(N+1) 保留最新的 N 条
	pkgredis.Client.ZRemRangeByRank(m.ctx, key, 0, -MaxOfflineMessages-1)

	// 设置过期时间
	pkgredis.Client.Expire(m.ctx, key, OfflineMessageTTL)

	log.Printf("[Offline] Stored message for user %s, seqID=%d", userID, msg.SeqID)
	return nil
}

// ==================== 拉取消息 ====================

// Fetch 按序列号范围拉取消息
//
// 使用 ZRANGEBYSCORE 查询：
// - Min: 起始 SeqID
// - Max: +inf（无上限）
// - Offset: 0
// - Count: 限制数量
//
// 返回的消息按 SeqID 升序排列（从旧到新）
func (m *OfflineManager) Fetch(userID string, startSeq, count int64) ([]*OfflineMessage, error) {
	key := OfflineBoxPrefix + userID

	// ZRANGEBYSCORE: 按 Score 范围查询
	results, err := pkgredis.Client.ZRangeByScore(m.ctx, key, &redis.ZRangeBy{
		Min:    fmt.Sprintf("%d", startSeq),
		Max:    "+inf",
		Offset: 0,
		Count:  count,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch offline messages: %w", err)
	}

	// 反序列化
	messages := make([]*OfflineMessage, 0, len(results))
	for _, data := range results {
		var msg OfflineMessage
		if err := json.Unmarshal([]byte(data), &msg); err != nil {
			log.Printf("[Offline] Failed to unmarshal message: %v", err)
			continue
		}
		messages = append(messages, &msg)
	}

	return messages, nil
}

// FetchLatest 拉取最新的 N 条消息
//
// 使用 ZREVRANGE 查询（降序，从新到旧）
// 适用于"下拉加载历史消息"的场景
func (m *OfflineManager) FetchLatest(userID string, count int64) ([]*OfflineMessage, error) {
	key := OfflineBoxPrefix + userID

	// ZREVRANGE: 反向范围查询
	// 0 到 count-1，返回最新的 count 条
	results, err := pkgredis.Client.ZRevRange(m.ctx, key, 0, count-1).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch latest messages: %w", err)
	}

	messages := make([]*OfflineMessage, 0, len(results))
	for _, data := range results {
		var msg OfflineMessage
		if err := json.Unmarshal([]byte(data), &msg); err != nil {
			log.Printf("[Offline] Failed to unmarshal message: %v", err)
			continue
		}
		messages = append(messages, &msg)
	}

	return messages, nil
}

// ==================== 删除消息（ACK 后）====================

// Remove 删除已确认的消息
//
// 当客户端 ACK 某个 SeqID 时，删除该 SeqID 及之前的所有消息
// 使用 ZREMRANGEBYSCORE 按 Score 范围删除
func (m *OfflineManager) Remove(userID string, maxSeqID int64) error {
	key := OfflineBoxPrefix + userID
	return pkgredis.Client.ZRemRangeByScore(m.ctx, key, "-inf", fmt.Sprintf("%d", maxSeqID)).Err()
}

// ==================== 辅助方法 ====================

// Count 获取离线消息数量
func (m *OfflineManager) Count(userID string) (int64, error) {
	key := OfflineBoxPrefix + userID
	return pkgredis.Client.ZCard(m.ctx, key).Result()
}

// Clear 清空用户的所有离线消息
func (m *OfflineManager) Clear(userID string) error {
	key := OfflineBoxPrefix + userID
	return pkgredis.Client.Del(m.ctx, key).Err()
}
