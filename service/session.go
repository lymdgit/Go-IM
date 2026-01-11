/*
Package service - 会话管理服务

=== 会话 (Session) 的作用 ===

会话管理解决两个核心问题：

1. 用户在线状态
  - 知道某个用户是否在线
  - 用于消息推送决策（在线推送 vs 离线存储）

2. 用户位置路由
  - 知道用户连接在哪个 Gateway
  - 跨节点消息转发的基础

=== Redis 数据结构 ===

 1. 会话信息（Hash）
    Key: user_session:alice
    Fields:
    - gateway_id: "gateway_1"
    - conn_id: "123"
    - login_time: "1699999999"
    TTL: 5分钟（需要心跳续期）

 2. 网关位置（String）
    Key: user_gateway:alice
    Value: "gateway_1"
    TTL: 5分钟

=== 心跳续期机制 ===

	时间轴
	────┬──────┬──────┬──────┬──────┬────▶
	    │      │      │      │      │
	   登录   心跳1  心跳2  心跳3   ...
	    │      │      │      │
	    ▼      ▼      ▼      ▼
	   创建   续期   续期   续期
	   TTL    TTL    TTL    TTL
	  5分钟  5分钟  5分钟  5分钟

如果客户端停止发送心跳，Key 自动过期，用户变为离线状态。
这是利用 Redis 的 EXPIRE 特性实现的轻量级心跳检测。
*/
package service

import (
	"context"
	"fmt"
	"log"
	"time"

	pkgredis "go-im/pkg/redis"
)

// ==================== 常量定义 ====================

const (
	// SessionKeyPrefix 会话 Key 前缀
	// 完整 Key: user_session:alice
	SessionKeyPrefix = "user_session:"

	// GatewayKeyPrefix 网关位置 Key 前缀
	// 完整 Key: user_gateway:alice
	GatewayKeyPrefix = "user_gateway:"

	// SessionTTL 会话过期时间
	// 客户端需要在此时间内发送心跳，否则会话过期
	SessionTTL = 5 * time.Minute
)

// ==================== 结构体定义 ====================

// Session 用户会话信息
type Session struct {
	UserID    string    // 用户 ID
	GatewayID string    // 所在网关
	ConnID    uint64    // 连接 ID
	LoginTime time.Time // 登录时间
}

// SessionManager 会话管理器
// 负责用户会话的创建、更新、删除和查询
type SessionManager struct {
	// gatewayID 当前网关的 ID
	// 登录时会记录用户在哪个网关
	gatewayID string

	// ctx Redis 操作的上下文
	ctx context.Context
}

// ==================== 构造函数 ====================

// NewSessionManager 创建会话管理器
// gatewayID: 当前网关的唯一标识
func NewSessionManager(gatewayID string) *SessionManager {
	return &SessionManager{
		gatewayID: gatewayID,
		ctx:       pkgredis.Context(),
	}
}

// ==================== 登录/登出 ====================

// Login 用户登录，创建会话
//
// 执行以下 Redis 操作（使用 Pipeline 减少 RTT）：
// 1. HSET user_session:uid {gateway_id, conn_id, login_time}
// 2. EXPIRE user_session:uid 300
// 3. SET user_gateway:uid gateway_id
// 4. EXPIRE user_gateway:uid 300
func (m *SessionManager) Login(userID string, connID uint64) error {
	client := pkgredis.Client

	// 使用 Pipeline 批量执行，减少网络往返
	pipe := client.Pipeline()

	sessionKey := SessionKeyPrefix + userID
	gatewayKey := GatewayKeyPrefix + userID

	// 存储会话详情（Hash 结构）
	pipe.HSet(m.ctx, sessionKey, map[string]interface{}{
		"gateway_id": m.gatewayID,
		"conn_id":    connID,
		"login_time": time.Now().Unix(),
	})
	pipe.Expire(m.ctx, sessionKey, SessionTTL)

	// 存储网关位置（用于快速路由查询）
	pipe.Set(m.ctx, gatewayKey, m.gatewayID, SessionTTL)

	// 执行 Pipeline
	_, err := pipe.Exec(m.ctx)
	if err != nil {
		return fmt.Errorf("failed to create session: %w", err)
	}

	log.Printf("[Session] User %s logged in on gateway %s", userID, m.gatewayID)
	return nil
}

// Logout 用户登出，删除会话
func (m *SessionManager) Logout(userID string) error {
	client := pkgredis.Client

	pipe := client.Pipeline()
	pipe.Del(m.ctx, SessionKeyPrefix+userID)
	pipe.Del(m.ctx, GatewayKeyPrefix+userID)

	_, err := pipe.Exec(m.ctx)
	if err != nil {
		return fmt.Errorf("failed to remove session: %w", err)
	}

	log.Printf("[Session] User %s logged out", userID)
	return nil
}

// ==================== 心跳 ====================

// Heartbeat 心跳续期
//
// 刷新会话的 TTL，防止过期
// 客户端应该每隔 (SessionTTL / 3) 时间发送一次心跳
// 例如：TTL=5分钟，心跳间隔=100秒
func (m *SessionManager) Heartbeat(userID string) error {
	client := pkgredis.Client

	pipe := client.Pipeline()
	// 刷新两个 Key 的过期时间
	pipe.Expire(m.ctx, SessionKeyPrefix+userID, SessionTTL)
	pipe.Expire(m.ctx, GatewayKeyPrefix+userID, SessionTTL)

	_, err := pipe.Exec(m.ctx)
	return err
}

// ==================== 查询 ====================

// GetUserGateway 获取用户所在的网关
//
// 这是消息路由的核心：
// 1. A 要发消息给 B
// 2. 查询 B 在哪个 Gateway
// 3. 如果在本地，直接推送
// 4. 如果在远程，通过 Pub/Sub 转发
func (m *SessionManager) GetUserGateway(userID string) (string, error) {
	gatewayID, err := pkgredis.Client.Get(m.ctx, GatewayKeyPrefix+userID).Result()
	if err != nil {
		return "", err // 包括 redis.Nil（用户不在线）
	}
	return gatewayID, nil
}

// IsOnline 检查用户是否在线
func (m *SessionManager) IsOnline(userID string) bool {
	exists, _ := pkgredis.Client.Exists(m.ctx, SessionKeyPrefix+userID).Result()
	return exists > 0
}

// GetOnlineUsers 获取所有在线用户（调试用）
// 注意：KEYS 命令在生产环境要谨慎使用，可能阻塞 Redis
func (m *SessionManager) GetOnlineUsers() ([]string, error) {
	keys, err := pkgredis.Client.Keys(m.ctx, SessionKeyPrefix+"*").Result()
	if err != nil {
		return nil, err
	}

	users := make([]string, len(keys))
	for i, key := range keys {
		// 去掉前缀，只保留用户 ID
		users[i] = key[len(SessionKeyPrefix):]
	}
	return users, nil
}
