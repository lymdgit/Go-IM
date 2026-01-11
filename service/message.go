/*
Package service - 消息路由服务

=== 消息路由的核心问题 ===

在分布式 IM 系统中，消息路由要回答一个问题：

	"如何把消息从发送者传递到接收者？"

这涉及几个子问题：
1. 接收者在线吗？
2. 接收者在哪个 Gateway？
3. 是本地推送还是跨节点转发？
4. 如果离线，消息存哪里？

=== 消息路由流程图 ===

	   Alice 发消息给 Bob

	   ┌─────────────────────────────────────────────────────────┐
	   │                     消息路由流程                         │
	   └─────────────────────────────────────────────────────────┘
	                           │
	                           ▼
	                   ┌───────────────┐
	                   │ 生成消息 SeqID │
	                   │ (Redis INCR)  │
	                   └───────┬───────┘
	                           │
	                           ▼
	                   ┌───────────────┐
	                   │ 查询 Bob 在线？│
	                   │ (Redis GET)   │
	                   └───────┬───────┘
	                           │
	              ┌────────────┴────────────┐
	              │                         │
	          在线│                         │离线
	              ▼                         ▼
	   ┌─────────────────┐       ┌─────────────────┐
	   │  查询 Bob 在     │       │  存入离线盒子   │
	   │  哪个 Gateway    │       │  (Redis ZSet)   │
	   └────────┬────────┘       └─────────────────┘
	            │
	   ┌────────┴────────┐
	   │                 │
	本地│              远程│
	   ▼                 ▼

┌─────────┐   ┌──────────────┐
│直接推送 │   │ Pub/Sub 转发 │
│(本地Map)│   │ 到其他Gateway│
└─────────┘   └──────────────┘

=== 本项目的简化处理 ===

由于是 Demo 项目，Gateway 和 Logic Server 合并在一起：
- 没有独立的 Logic Server
- 消息路由逻辑直接在 Gateway 中处理
- 这简化了部署，但在大规模系统中应该分离
*/
package service

import (
	"encoding/json"
	"go-im/protocol"
	"go-im/server"
	"log"
)

// ==================== 常量定义 ====================

// 消息类型
const (
	MsgTypePrivate = 1 // 单聊消息
	MsgTypeGroup   = 2 // 群聊消息
	MsgTypeSystem  = 3 // 系统消息
)

// ==================== 消息结构 ====================

// ChatMessage 聊天消息结构
// 这是业务层的消息格式，不同于协议层的 Message
type ChatMessage struct {
	FromUserID string `json:"from_user_id"` // 发送者
	ToUserID   string `json:"to_user_id"`   // 接收者
	Content    string `json:"content"`      // 消息内容
	MsgType    int    `json:"msg_type"`     // 消息类型
	SeqID      int64  `json:"seq_id"`       // 序列号
	Timestamp  int64  `json:"timestamp"`    // 时间戳
}

// ==================== 消息处理器 ====================

// MessageHandler 消息路由处理器
// 协调各个服务组件，完成消息的路由和投递
type MessageHandler struct {
	gatewayID   string                    // 当前网关 ID
	connManager *server.ConnectionManager // 连接管理器
	session     *SessionManager           // 会话服务
	pubsub      *PubSubManager            // Pub/Sub 服务
	sequence    *SequenceManager          // 序列号服务
	offline     *OfflineManager           // 离线消息服务
}

// NewMessageHandler 创建消息处理器
// 通过依赖注入的方式，将各个服务组件组合在一起
func NewMessageHandler(
	gatewayID string,
	connManager *server.ConnectionManager,
	session *SessionManager,
	pubsub *PubSubManager,
	sequence *SequenceManager,
	offline *OfflineManager,
) *MessageHandler {
	return &MessageHandler{
		gatewayID:   gatewayID,
		connManager: connManager,
		session:     session,
		pubsub:      pubsub,
		sequence:    sequence,
		offline:     offline,
	}
}

// ==================== 发送私聊消息 ====================

// SendPrivateMessage 发送私聊消息
//
// 这是消息路由的入口函数，完整流程：
// 1. 生成序列号
// 2. 查询目标用户位置
// 3. 决定投递方式（本地/远程/离线）
// 4. 执行投递
func (h *MessageHandler) SendPrivateMessage(fromUserID, toUserID string, content []byte) error {
	// Step 1: 生成消息序列号
	// 用于消息排序和 ACK
	conversationID := getConversationID(fromUserID, toUserID)
	seqID, err := h.sequence.NextSeq(conversationID)
	if err != nil {
		return err
	}

	// Step 2: 查询目标用户所在的 Gateway
	targetGateway, err := h.session.GetUserGateway(toUserID)
	if err != nil {
		// 用户不在线，存入离线消息盒子
		log.Printf("[Message] User %s is offline, storing message", toUserID)
		return h.storeOfflineMessage(fromUserID, toUserID, content, seqID)
	}

	// Step 3: 构造聊天消息
	msg := &ChatMessage{
		FromUserID: fromUserID,
		ToUserID:   toUserID,
		Content:    string(content),
		MsgType:    MsgTypePrivate,
		SeqID:      seqID,
	}

	// Step 4: 根据用户位置选择投递方式
	if targetGateway == h.gatewayID {
		// 用户在本地 Gateway，直接推送
		return h.deliverLocal(toUserID, msg)
	}

	// 用户在其他 Gateway，通过 Pub/Sub 转发
	return h.deliverRemote(targetGateway, msg)
}

// ==================== 本地投递 ====================

// deliverLocal 本地投递消息
//
// 用户在当前 Gateway，直接从内存中查找连接并推送
// 这是最快的投递方式，无需网络请求
func (h *MessageHandler) deliverLocal(userID string, msg *ChatMessage) error {
	// 从 ConnectionManager 中查找用户连接
	conn := h.connManager.GetByUserID(userID)
	if conn == nil {
		// 连接不存在（可能刚刚断开），存入离线
		log.Printf("[Message] Connection not found for user %s", userID)
		return h.storeOfflineMessage(msg.FromUserID, msg.ToUserID, []byte(msg.Content), msg.SeqID)
	}

	// 序列化消息
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	// 封装为协议消息并发送
	protoMsg := &protocol.Message{
		CmdType: protocol.CmdTypeMessage,
		Body:    data,
	}

	log.Printf("[Message] Delivering message to user %s locally", userID)
	return conn.Send(protoMsg)
}

// ==================== 远程投递 ====================

// deliverRemote 远程投递消息（跨 Gateway）
//
// 用户在其他 Gateway，通过 Redis Pub/Sub 转发
// 目标 Gateway 会收到消息并投递给用户
func (h *MessageHandler) deliverRemote(targetGateway string, msg *ChatMessage) error {
	// 构造 Pub/Sub 消息
	pubsubMsg := &PubSubMessage{
		FromUserID: msg.FromUserID,
		ToUserID:   msg.ToUserID,
		Content:    []byte(msg.Content),
		MsgType:    msg.MsgType,
		SeqID:      msg.SeqID,
	}

	log.Printf("[Message] Routing message to gateway %s via Pub/Sub", targetGateway)
	return h.pubsub.Publish(targetGateway, pubsubMsg)
}

// ==================== 离线存储 ====================

// storeOfflineMessage 存储离线消息
func (h *MessageHandler) storeOfflineMessage(fromUserID, toUserID string, content []byte, seqID int64) error {
	offlineMsg := &OfflineMessage{
		FromUserID: fromUserID,
		ToUserID:   toUserID,
		Content:    content,
		MsgType:    MsgTypePrivate,
		SeqID:      seqID,
	}
	return h.offline.Store(toUserID, offlineMsg)
}

// ==================== Pub/Sub 消息处理 ====================

// HandlePubSubMessage 处理从 Pub/Sub 收到的消息
//
// 当其他 Gateway 向本 Gateway 发送消息时，会通过这个方法处理
// 本质上是将远程消息转换为本地投递
func (h *MessageHandler) HandlePubSubMessage(msg *PubSubMessage) {
	chatMsg := &ChatMessage{
		FromUserID: msg.FromUserID,
		ToUserID:   msg.ToUserID,
		Content:    string(msg.Content),
		MsgType:    msg.MsgType,
		SeqID:      msg.SeqID,
	}

	// 尝试本地投递
	if err := h.deliverLocal(msg.ToUserID, chatMsg); err != nil {
		log.Printf("[Message] Failed to deliver Pub/Sub message: %v", err)
	}
}

// ==================== 离线消息投递 ====================

// DeliverOfflineMessages 投递离线消息
//
// 用户上线时调用，将存储的离线消息推送给用户
func (h *MessageHandler) DeliverOfflineMessages(userID string, conn *server.Connection) error {
	// 拉取最近的离线消息
	messages, err := h.offline.FetchLatest(userID, 100)
	if err != nil {
		return err
	}

	// 逐条推送
	for _, msg := range messages {
		chatMsg := &ChatMessage{
			FromUserID: msg.FromUserID,
			ToUserID:   msg.ToUserID,
			Content:    string(msg.Content),
			MsgType:    msg.MsgType,
			SeqID:      msg.SeqID,
		}

		data, err := json.Marshal(chatMsg)
		if err != nil {
			continue
		}

		protoMsg := &protocol.Message{
			CmdType: protocol.CmdTypeMessage,
			Body:    data,
		}
		conn.Send(protoMsg)
	}

	log.Printf("[Message] Delivered %d offline messages to user %s", len(messages), userID)
	return nil
}

// ==================== 工具函数 ====================

// getConversationID 生成会话标识
//
// 私聊的会话 ID 由两个用户 ID 组成，保证 A→B 和 B→A 使用相同的会话 ID
// 实现方式：按字典序排序后拼接
//
// 示例：
//   - getConversationID("alice", "bob") → "alice:bob"
//   - getConversationID("bob", "alice") → "alice:bob" (相同)
func getConversationID(user1, user2 string) string {
	if user1 < user2 {
		return user1 + ":" + user2
	}
	return user2 + ":" + user1
}
