/*
Go-IM 服务端主程序

=== 程序架构概览 ===

	┌─────────────────────────────────────────────────────────────┐
	│                        App (主程序)                         │
	│  ┌──────────────────────────────────────────────────────┐  │
	│  │                   TCPServer                          │  │
	│  │  - 监听端口，接受连接                                │  │
	│  │  - 管理连接生命周期                                  │  │
	│  │  - 心跳检测                                          │  │
	│  └──────────────────────────────────────────────────────┘  │
	│                          │                                  │
	│                          ▼ 消息分发                         │
	│  ┌──────────────────────────────────────────────────────┐  │
	│  │               MessageHandler                         │  │
	│  │  - 认证处理                                          │  │
	│  │  - 消息路由                                          │  │
	│  │  - ACK 处理                                          │  │
	│  └──────────────────────────────────────────────────────┘  │
	│                          │                                  │
	│          ┌───────────────┼───────────────┐                  │
	│          ▼               ▼               ▼                  │
	│   ┌───────────┐   ┌───────────┐   ┌───────────┐            │
	│   │ Session   │   │  PubSub   │   │ Offline   │            │
	│   │ Manager   │   │  Manager  │   │ Manager   │            │
	│   └───────────┘   └───────────┘   └───────────┘            │
	│          │               │               │                  │
	│          └───────────────┼───────────────┘                  │
	│                          ▼                                  │
	│                 ┌───────────────┐                           │
	│                 │     Redis     │                           │
	│                 └───────────────┘                           │
	└─────────────────────────────────────────────────────────────┘

=== 启动流程 ===

1. 解析命令行参数
2. 初始化 Redis 连接
3. 初始化各个 Service
4. 启动 Pub/Sub 订阅
5. 启动 TCP Server
6. 等待关闭信号
7. 优雅关闭所有组件

=== 命令行参数 ===

	-id     网关 ID（默认: gateway_1）
	-addr   监听地址（默认: :8080）
	-redis  Redis 地址（默认: 127.0.0.1:6379）

示例:

	./server -id gateway_1 -addr :8080 -redis 127.0.0.1:6379
*/
package main

import (
	"encoding/json"
	"flag"
	"go-im/pkg/redis"
	"go-im/protocol"
	"go-im/server"
	"go-im/service"
	"log"
	"os"
	"os/signal"
	"syscall"
)

// ==================== 配置结构 ====================

// Config 服务器配置
type Config struct {
	GatewayID string // 网关唯一标识
	TCPAddr   string // TCP 监听地址
	RedisAddr string // Redis 服务器地址
}

// ==================== 应用程序结构 ====================

// App 应用程序主结构
// 持有所有组件的引用，负责生命周期管理
type App struct {
	config     *Config                  // 配置
	tcpServer  *server.TCPServer        // TCP 服务器
	session    *service.SessionManager  // 会话管理
	pubsub     *service.PubSubManager   // Pub/Sub 管理
	sequence   *service.SequenceManager // 序列号管理
	offline    *service.OfflineManager  // 离线消息管理
	msgHandler *service.MessageHandler  // 消息处理器
}

// NewApp 创建应用实例
func NewApp(config *Config) *App {
	return &App{config: config}
}

// ==================== 初始化 ====================

// Initialize 初始化所有组件
// 创建顺序很重要：Redis → Services → TCP Server
func (a *App) Initialize() error {
	// 1. 初始化 Redis 连接
	// 这是基础设施，其他组件都依赖它
	if err := redis.Init(&redis.Config{
		Addr:     a.config.RedisAddr,
		PoolSize: 100,
	}); err != nil {
		return err
	}

	// 2. 初始化各个 Service
	a.session = service.NewSessionManager(a.config.GatewayID)
	a.pubsub = service.NewPubSubManager(a.config.GatewayID)
	a.sequence = service.NewSequenceManager()
	a.offline = service.NewOfflineManager()

	// 3. 初始化 TCP 服务器
	a.tcpServer = server.NewTCPServer(a.config.TCPAddr, a.config.GatewayID)

	// 4. 初始化消息处理器
	// 注入所有依赖的 Service
	a.msgHandler = service.NewMessageHandler(
		a.config.GatewayID,
		a.tcpServer.ConnManager,
		a.session,
		a.pubsub,
		a.sequence,
		a.offline,
	)

	// 5. 将消息处理器注册到 TCP 服务器
	// TCP 层收到消息后会调用 HandleConnection
	a.tcpServer.SetHandler(a)

	return nil
}

// ==================== 启动和停止 ====================

// Start 启动所有组件
func (a *App) Start() error {
	// 启动 Pub/Sub 订阅
	// 必须在 TCP 服务器之前启动，确保能收到其他节点的消息
	if err := a.pubsub.Start(a.msgHandler.HandlePubSubMessage); err != nil {
		return err
	}

	// 启动 TCP 服务器
	if err := a.tcpServer.Start(); err != nil {
		return err
	}

	return nil
}

// Stop 优雅停止所有组件
// 停止顺序与启动顺序相反：TCP Server → Pub/Sub → Redis
func (a *App) Stop() {
	log.Println("[App] Stopping application...")

	// 1. 停止 TCP 服务器（不再接受新连接，等待现有连接处理完）
	a.tcpServer.Stop()

	// 2. 停止 Pub/Sub
	a.pubsub.Stop()

	// 3. 关闭 Redis 连接
	redis.Close()

	log.Println("[App] Application stopped")
}

// ==================== 消息处理 ====================

// HandleConnection 实现 server.MessageHandler 接口
// TCP 层收到消息后会调用这个方法
// 根据消息类型分发到不同的处理函数
func (a *App) HandleConnection(conn *server.Connection, msg *protocol.Message) {
	switch msg.CmdType {
	case protocol.CmdTypeAuth:
		// 认证请求
		a.handleAuth(conn, msg)

	case protocol.CmdTypeMessage:
		// 聊天消息
		a.handleMessage(conn, msg)

	case protocol.CmdTypeMessageAck:
		// 消息确认
		a.handleMessageAck(conn, msg)

	default:
		log.Printf("[App] Unknown command type: %d", msg.CmdType)
	}
}

// ==================== 认证处理 ====================

// handleAuth 处理认证请求
//
// 流程：
// 1. 解析请求中的 Token
// 2. 验证 Token（JWT 签名、过期时间）
// 3. 绑定用户到连接
// 4. 在 Redis 中创建会话
// 5. 发送响应
// 6. 投递离线消息
func (a *App) handleAuth(conn *server.Connection, msg *protocol.Message) {
	// 解析请求
	var authReq struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(msg.Body, &authReq); err != nil {
		a.sendAuthResponse(conn, false, "Invalid request")
		return
	}

	// 验证 Token
	claims, err := service.ValidateToken(authReq.Token)
	if err != nil {
		a.sendAuthResponse(conn, false, err.Error())
		return
	}

	// 绑定用户到连接
	// 这样后续可以通过 UserID 找到这个连接
	a.tcpServer.ConnManager.BindUser(claims.UserID, conn)

	// 在 Redis 中创建会话
	if err := a.session.Login(claims.UserID, conn.ID); err != nil {
		log.Printf("[App] Failed to create session: %v", err)
	}

	// 发送认证成功响应
	a.sendAuthResponse(conn, true, claims.UserID)

	// 异步投递离线消息（不阻塞认证流程）
	go a.msgHandler.DeliverOfflineMessages(claims.UserID, conn)

	log.Printf("[App] User %s authenticated on conn-%d", claims.UserID, conn.ID)
}

// sendAuthResponse 发送认证响应
func (a *App) sendAuthResponse(conn *server.Connection, success bool, message string) {
	resp := map[string]interface{}{
		"success": success,
		"message": message,
	}
	data, _ := json.Marshal(resp)
	conn.Send(&protocol.Message{
		CmdType: protocol.CmdTypeAuthAck,
		Body:    data,
	})
}

// ==================== 消息处理 ====================

// handleMessage 处理聊天消息
func (a *App) handleMessage(conn *server.Connection, msg *protocol.Message) {
	// 检查用户是否已认证
	userID := conn.GetUserID()
	if userID == "" {
		log.Printf("[App] Unauthenticated message from conn-%d", conn.ID)
		return
	}

	// 解析消息内容
	var chatMsg struct {
		ToUserID string `json:"to_user_id"`
		Content  string `json:"content"`
	}
	if err := json.Unmarshal(msg.Body, &chatMsg); err != nil {
		log.Printf("[App] Invalid message format: %v", err)
		return
	}

	// 路由消息
	if err := a.msgHandler.SendPrivateMessage(userID, chatMsg.ToUserID, []byte(chatMsg.Content)); err != nil {
		log.Printf("[App] Failed to send message: %v", err)
	}
}

// ==================== ACK 处理 ====================

// handleMessageAck 处理消息确认
//
// 当客户端确认收到消息时，删除已确认的离线消息
// 这确保消息不会重复推送
func (a *App) handleMessageAck(conn *server.Connection, msg *protocol.Message) {
	userID := conn.GetUserID()
	if userID == "" {
		return
	}

	// 解析 ACK 内容
	var ackMsg struct {
		SeqID int64 `json:"seq_id"`
	}
	if err := json.Unmarshal(msg.Body, &ackMsg); err != nil {
		return
	}

	// 删除已确认的离线消息
	a.offline.Remove(userID, ackMsg.SeqID)
}

// ==================== 主函数 ====================

func main() {
	// 解析命令行参数
	gatewayID := flag.String("id", "gateway_1", "Gateway ID")
	tcpAddr := flag.String("addr", ":8080", "TCP listen address")
	redisAddr := flag.String("redis", "127.0.0.1:6379", "Redis address")
	flag.Parse()

	// 构造配置
	config := &Config{
		GatewayID: *gatewayID,
		TCPAddr:   *tcpAddr,
		RedisAddr: *redisAddr,
	}

	// 创建并初始化应用
	app := NewApp(config)
	if err := app.Initialize(); err != nil {
		log.Fatalf("Failed to initialize: %v", err)
	}

	// 启动应用
	if err := app.Start(); err != nil {
		log.Fatalf("Failed to start: %v", err)
	}

	// 等待中断信号（Ctrl+C 或 kill）
	// 这是 Go 程序优雅关闭的标准模式
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	// 收到信号，优雅关闭
	app.Stop()
}
