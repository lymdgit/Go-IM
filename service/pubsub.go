/*
Package service - Redis Pub/Sub 消息路由

=== 为什么需要 Pub/Sub？===

在分布式 IM 系统中，用户可能连接在不同的 Gateway：

	┌─────────────┐          ┌─────────────┐
	│  Gateway-1  │          │  Gateway-2  │
	│   (Alice)   │          │    (Bob)    │
	└──────┬──────┘          └──────┬──────┘
	       │                        │
	       │    Alice → Bob 消息    │
	       │                        │
	       └────────────────────────┘
	                 如何传递？

解决方案：Redis Pub/Sub

	┌─────────────┐                    ┌─────────────┐
	│  Gateway-1  │                    │  Gateway-2  │
	│   (Alice)   │                    │    (Bob)    │
	└──────┬──────┘                    └──────┬──────┘
	       │                                  │
	       │ PUBLISH channel:gateway_2 msg    │ SUBSCRIBE channel:gateway_2
	       │                                  │
	       ▼                                  ▼
	┌──────────────────────────────────────────────┐
	│                    Redis                      │
	│         Pub/Sub 消息分发中心                  │
	└──────────────────────────────────────────────┘

=== Pub/Sub vs 消息队列 ===

| 特性      | Pub/Sub        | 消息队列 (如 Kafka)  |
|-----------|----------------|---------------------|
| 持久化    | ❌ 不持久化    | ✓ 持久化            |
| 消费模式  | 广播（所有订阅者）| 竞争（只有一个消费）|
| 适用场景  | 实时通知       | 可靠消息传递        |
| 复杂度    | 简单           | 复杂                |

本项目使用 Pub/Sub 因为：
- 消息是实时的，不需要持久化（离线消息有专门的存储）
- 每个 Gateway 只关心自己的 Channel
- 实现简单，延迟低
*/
package service

import (
	"context"
	"encoding/json"
	"log"

	pkgredis "go-im/pkg/redis"

	"github.com/redis/go-redis/v9"
)

// ==================== 消息结构 ====================

// PubSubMessage Pub/Sub 传输的消息格式
// 这是跨 Gateway 传递的消息结构
type PubSubMessage struct {
	FromUserID string `json:"from_user_id"` // 发送者
	ToUserID   string `json:"to_user_id"`   // 接收者
	Content    []byte `json:"content"`      // 消息内容
	MsgType    int    `json:"msg_type"`     // 消息类型
	SeqID      int64  `json:"seq_id"`       // 序列号
}

// ==================== Pub/Sub 管理器 ====================

// PubSubManager Redis Pub/Sub 管理器
// 每个 Gateway 实例有一个 PubSubManager
type PubSubManager struct {
	// gatewayID 当前网关 ID
	gatewayID string

	// channelKey 本网关订阅的频道
	// 格式: channel:gateway_xxx
	channelKey string

	// pubsub Redis Pub/Sub 订阅器
	pubsub *redis.PubSub

	// ctx 上下文，用于取消订阅
	ctx context.Context

	// cancel 取消函数
	cancel context.CancelFunc

	// handler 消息处理回调
	handler func(*PubSubMessage)
}

// ==================== 构造函数 ====================

// NewPubSubManager 创建 Pub/Sub 管理器
func NewPubSubManager(gatewayID string) *PubSubManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &PubSubManager{
		gatewayID:  gatewayID,
		channelKey: "channel:gateway_" + gatewayID, // 每个 Gateway 有自己的频道
		ctx:        ctx,
		cancel:     cancel,
	}
}

// ==================== 订阅 ====================

// Start 开始订阅消息
//
// 订阅流程：
// 1. 订阅自己的频道 (channel:gateway_xxx)
// 2. 启动接收循环
// 3. 收到消息后调用 handler 处理
func (m *PubSubManager) Start(handler func(*PubSubMessage)) error {
	m.handler = handler

	// 订阅频道
	m.pubsub = pkgredis.Client.Subscribe(m.ctx, m.channelKey)

	// 等待订阅确认
	// 这确保订阅已经生效
	_, err := m.pubsub.Receive(m.ctx)
	if err != nil {
		return err
	}

	log.Printf("[PubSub] Subscribed to channel: %s", m.channelKey)

	// 启动接收循环（后台 Goroutine）
	go m.receiveLoop()
	return nil
}

// receiveLoop 消息接收循环
// 持续从 Redis 接收消息并处理
func (m *PubSubManager) receiveLoop() {
	// 获取消息通道
	ch := m.pubsub.Channel()

	for {
		select {
		case <-m.ctx.Done():
			// 收到取消信号，退出
			return

		case msg, ok := <-ch:
			if !ok {
				// 通道关闭
				return
			}

			// 解析消息
			var pubsubMsg PubSubMessage
			if err := json.Unmarshal([]byte(msg.Payload), &pubsubMsg); err != nil {
				log.Printf("[PubSub] Failed to unmarshal message: %v", err)
				continue
			}

			// 调用处理器
			if m.handler != nil {
				m.handler(&pubsubMsg)
			}
		}
	}
}

// ==================== 发布 ====================

// Publish 发布消息到指定网关
//
// 流程：
// 1. 将消息序列化为 JSON
// 2. PUBLISH 到目标网关的频道
// 3. 目标网关的 receiveLoop 会收到消息
//
// 参数:
//   - targetGatewayID: 目标网关 ID
//   - msg: 要发送的消息
func (m *PubSubManager) Publish(targetGatewayID string, msg *PubSubMessage) error {
	// 序列化消息
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	// 构造目标频道名
	channelKey := "channel:gateway_" + targetGatewayID

	// 发布消息
	return pkgredis.Client.Publish(m.ctx, channelKey, data).Err()
}

// ==================== 停止 ====================

// Stop 停止 Pub/Sub
func (m *PubSubManager) Stop() {
	// 取消上下文，通知 receiveLoop 退出
	m.cancel()

	// 关闭订阅
	if m.pubsub != nil {
		m.pubsub.Close()
	}
}
