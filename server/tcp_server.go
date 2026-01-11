/*
Package server - TCP 服务器实现

=== Go 的高并发网络模型 ===

本项目采用 Goroutine-per-Connection 模型：
- 每个客户端连接分配一个独立的 Goroutine 处理
- 代码写起来像同步阻塞，但底层是非阻塞的
- Go 运行时的 Netpoller 负责将阻塞调用转换为高效的 epoll 事件

对比传统模型：

1. 多线程模型 (每连接一线程)
  - 问题：线程创建开销大，1万连接 = 1万线程 = 内存爆炸
  - C10K 问题的根源

2. 事件驱动模型 (Reactor/epoll 回调)
  - 如：Nginx、Redis、Netty
  - 问题：代码复杂，需要手动管理状态机

3. Goroutine 模型 (本项目) ✓
  - 每连接一 Goroutine，但 Goroutine 很轻量（2KB 初始栈）
  - 1万连接 = 1万 Goroutine = 约 20MB 内存
  - 底层自动使用 epoll，开发者无感知

=== 架构图 ===

	┌─────────────────────────────────────────────────┐
	│                   TCP Server                     │
	│  ┌─────────────────────────────────────────┐    │
	│  │            Accept Loop                   │    │
	│  │    listener.Accept() → 新连接            │    │
	│  └──────────────────┬──────────────────────┘    │
	│                     │ 每个连接启动一个 Goroutine  │
	│        ┌────────────┼────────────┐              │
	│        ▼            ▼            ▼              │
	│   ┌─────────┐  ┌─────────┐  ┌─────────┐        │
	│   │ Conn 1  │  │ Conn 2  │  │ Conn N  │        │
	│   │Goroutine│  │Goroutine│  │Goroutine│        │
	│   └─────────┘  └─────────┘  └─────────┘        │
	│        │            │            │              │
	│   ┌────┴────────────┴────────────┴────┐        │
	│   │        Connection Manager          │        │
	│   │       (UID → Connection 映射)      │        │
	│   └────────────────────────────────────┘        │
	└─────────────────────────────────────────────────┘
*/
package server

import (
	"bufio"
	"fmt"
	"go-im/protocol"
	"log"
	"net"
	"sync"
	"sync/atomic"
)

// ==================== 接口定义 ====================

// MessageHandler 消息处理器接口
// 使用接口实现 TCP 层与业务层的解耦
// TCP 层只负责网络 I/O，具体业务逻辑由实现此接口的对象处理
type MessageHandler interface {
	HandleConnection(conn *Connection, msg *protocol.Message)
}

// ==================== TCP 服务器结构体 ====================

// TCPServer TCP 服务器
// 职责：监听端口、接受连接、管理连接生命周期
type TCPServer struct {
	// addr 监听地址，如 ":8080" 或 "0.0.0.0:8080"
	addr string

	// gatewayID 网关唯一标识
	// 在分布式部署中区分不同的网关节点
	gatewayID string

	// listener TCP 监听器
	// 调用 Accept() 接受新连接
	listener net.Listener

	// quit 关闭信号通道
	// 实现优雅关闭：close(quit) 通知所有 Goroutine 退出
	quit chan struct{}

	// wg WaitGroup 用于等待所有 Goroutine 结束
	// 确保优雅关闭时不丢失正在处理的消息
	wg sync.WaitGroup

	// connID 连接 ID 计数器
	// 使用 atomic 保证并发安全的自增
	connID uint64

	// ConnManager 连接管理器
	// 维护 UID → Connection 的映射，用于消息路由
	ConnManager *ConnectionManager

	// handler 消息处理器
	// 收到消息后委托给它处理（依赖注入）
	handler MessageHandler
}

// ==================== 构造函数 ====================

// NewTCPServer 创建新的 TCP 服务器实例
// 参数:
//   - addr: 监听地址，如 ":8080"
//   - gatewayID: 网关标识，如 "gateway_1"
func NewTCPServer(addr, gatewayID string) *TCPServer {
	return &TCPServer{
		addr:        addr,
		gatewayID:   gatewayID,
		quit:        make(chan struct{}), // 无缓冲通道，用于广播关闭信号
		ConnManager: NewConnectionManager(),
	}
}

// SetHandler 设置消息处理器（依赖注入）
func (s *TCPServer) SetHandler(handler MessageHandler) {
	s.handler = handler
}

// ==================== 服务器生命周期 ====================

// Start 启动 TCP 服务器
// 这是一个非阻塞调用，实际的监听在单独的 Goroutine 中进行
func (s *TCPServer) Start() error {
	// 创建 TCP 监听器
	// net.Listen 会绑定端口，但还不会接受连接
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.addr, err)
	}
	s.listener = listener
	log.Printf("[Server] Gateway %s started on %s", s.gatewayID, s.addr)

	// 启动接受连接的 Goroutine
	s.wg.Add(1)
	go s.acceptLoop()

	return nil
}

// Stop 优雅关闭服务器
// 优雅关闭 (Graceful Shutdown) 的步骤：
// 1. 停止接受新连接
// 2. 等待现有连接处理完成
// 3. 通知客户端重连（可选）
// 4. 关闭所有资源
//
// 为什么需要优雅关闭？
// - 防止消息丢失
// - 让客户端有机会重连到其他节点
// - 在滚动更新时保证服务可用性
func (s *TCPServer) Stop() {
	log.Println("[Server] Initiating graceful shutdown...")

	// 步骤 1: 关闭 quit 通道，广播关闭信号
	// 所有 select 监听 quit 的 Goroutine 都会收到通知
	close(s.quit)

	// 步骤 2: 关闭 listener，使 Accept() 返回错误
	if s.listener != nil {
		s.listener.Close()
	}

	// 步骤 3: 等待所有 Goroutine 结束
	log.Println("[Server] Waiting for existing connections to finish...")
	s.wg.Wait()

	log.Println("[Server] Server stopped gracefully")
}

// ==================== 连接接受循环 ====================

// acceptLoop 持续接受新的客户端连接
// 这是服务器的主循环，运行在独立的 Goroutine 中
func (s *TCPServer) acceptLoop() {
	defer s.wg.Done()

	for {
		// Accept 会阻塞直到有新连接到来
		// Go 运行时会在底层使用 epoll 等待，不会占用 CPU
		conn, err := s.listener.Accept()
		if err != nil {
			// 检查是否是关闭信号导致的错误
			select {
			case <-s.quit:
				// 正常关闭，退出循环
				return
			default:
				// 其他错误，记录日志继续
				log.Printf("[Server] Accept error: %v", err)
				continue
			}
		}

		// 为新连接分配唯一 ID
		// atomic.AddUint64 保证并发安全
		connID := atomic.AddUint64(&s.connID, 1)

		// 每个连接启动一个 Goroutine 处理
		// 这就是 Goroutine-per-Connection 模型
		s.wg.Add(1)
		go s.handleConnection(conn, connID)
	}
}

// ==================== 连接处理 ====================

// handleConnection 处理单个客户端连接
// 这是每个连接的主循环，负责：
// 1. 读取消息
// 2. 分发给业务处理器
// 3. 管理连接生命周期
func (s *TCPServer) handleConnection(netConn net.Conn, connID uint64) {
	defer s.wg.Done()

	clientAddr := netConn.RemoteAddr().String()
	log.Printf("[Conn-%d] New connection from %s", connID, clientAddr)

	// 创建连接包装器
	// Connection 提供了更高级的抽象：用户绑定、异步写入等
	conn := NewConnection(connID, netConn)
	s.ConnManager.Add(conn)

	// ★★★ 关键：启动写入协程 ★★★
	// Connection 使用通道实现异步写入
	// 必须启动 writeLoop 才能真正发送消息
	go conn.writeLoop()

	// 确保连接关闭时清理资源
	defer func() {
		s.ConnManager.Remove(conn)
		conn.Close()
		log.Printf("[Conn-%d] Connection closed", connID)
	}()

	// 创建带缓冲的 Reader
	// bufio.Reader 提供缓冲，减少系统调用次数
	reader := bufio.NewReader(netConn)

	// 连接的读取循环
	for {
		// 检查关闭信号
		select {
		case <-s.quit:
			// 服务器关闭，发送重连指令
			s.sendReconnectInstruction(conn)
			return
		case <-conn.closeChan:
			// 连接已关闭
			return
		default:
			// 继续处理
		}

		// 读取并解析消息
		// Unpack 会阻塞直到读取到完整消息
		msg, err := protocol.Unpack(reader)
		if err != nil {
			if err.Error() != "EOF" {
				log.Printf("[Conn-%d] Read error: %v", connID, err)
			}
			return
		}

		// 心跳消息直接处理，不走业务逻辑
		if msg.CmdType == protocol.CmdTypeHeartbeat {
			s.handleHeartbeat(conn)
			continue
		}

		// 其他消息委托给业务处理器
		if s.handler != nil {
			s.handler.HandleConnection(conn, msg)
		}
	}
}

// ==================== 心跳处理 ====================

// handleHeartbeat 处理心跳请求
// 心跳的作用：
// 1. 保持连接活跃（NAT 穿透、防止被中间设备断开）
// 2. 检测连接是否存活
// 3. 服务端可以据此更新用户在线状态
func (s *TCPServer) handleHeartbeat(conn *Connection) {
	// 回复 pong
	ack := &protocol.Message{
		CmdType: protocol.CmdTypeHeartbeat,
		Body:    []byte("pong"),
	}
	conn.Send(ack)
}

// ==================== 优雅关闭辅助 ====================

// sendReconnectInstruction 发送重连指令
// 在服务器关闭时通知客户端：
// - 服务器即将关闭
// - 请重新连接到其他节点
// 这是优雅关闭的重要组成部分
func (s *TCPServer) sendReconnectInstruction(conn *Connection) {
	msg := &protocol.Message{
		CmdType: protocol.CmdTypeKick,
		Body:    []byte(`{"reason":"server_restart","reconnect":true}`),
	}
	conn.Send(msg)
}

// GetGatewayID 获取网关 ID
func (s *TCPServer) GetGatewayID() string {
	return s.gatewayID
}
