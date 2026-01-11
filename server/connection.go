/*
Package server - 连接管理模块

=== 连接抽象的必要性 ===

为什么不直接使用 net.Conn？

1. 需要绑定用户信息（UserID）
2. 需要实现异步写入（避免写入阻塞读取）
3. 需要连接状态管理（活跃时间、关闭状态）
4. 需要全局连接查找（根据 UserID 找到 Connection）

=== 读写分离架构 ===

每个连接有两个 Goroutine：

	┌──────────────────────────────────────┐
	│            Connection                 │
	│                                      │
	│   ┌─────────┐      ┌─────────┐      │
	│   │ 读协程  │      │ 写协程  │      │
	│   │readLoop │      │writeLoop│      │
	│   └────┬────┘      └────▲────┘      │
	│        │                │           │
	│        ▼                │           │
	│   ┌─────────────────────────┐       │
	│   │      writeChan          │       │
	│   │   (带缓冲的通道)         │       │
	│   └─────────────────────────┘       │
	└──────────────────────────────────────┘

为什么要读写分离？
- 避免写入阻塞读取（网络慢时写入可能阻塞）
- 异步发送，提高吞吐量
- 通过通道安全地在 Goroutine 间传递数据
*/
package server

import (
	"bufio"
	"go-im/protocol"
	"log"
	"net"
	"sync"
	"time"
)

// ==================== 连接结构体 ====================

// Connection 表示一个客户端连接
// 封装了 net.Conn，提供更高级的功能
type Connection struct {
	// ID 连接唯一标识，由服务器分配
	ID uint64

	// UserID 绑定的用户 ID
	// 用户认证后会设置这个字段
	// 消息路由时通过 UserID 找到对应的 Connection
	UserID string

	// Conn 底层的 TCP 连接
	Conn net.Conn

	// reader 带缓冲的读取器
	// bufio.Reader 减少系统调用，提高读取效率
	reader *bufio.Reader

	// writeChan 写入通道（带缓冲）
	// 发送消息时先放入通道，由 writeLoop 实际发送
	// 缓冲大小 256：允许短时间内积累一定数量的消息
	writeChan chan []byte

	// closeChan 关闭信号通道
	// close(closeChan) 会通知所有监听者连接已关闭
	closeChan chan struct{}

	// closeOnce 确保只关闭一次
	// 防止多次调用 Close() 导致 panic
	closeOnce sync.Once

	// lastActive 最后活跃时间
	// 用于心跳检测和空闲连接清理
	lastActive time.Time

	// mu 读写锁，保护共享字段
	mu sync.RWMutex
}

// ==================== 构造函数 ====================

// NewConnection 创建新的连接包装器
func NewConnection(id uint64, conn net.Conn) *Connection {
	return &Connection{
		ID:         id,
		Conn:       conn,
		reader:     bufio.NewReader(conn),
		writeChan:  make(chan []byte, 256), // 带缓冲通道
		closeChan:  make(chan struct{}),    // 无缓冲，用于广播信号
		lastActive: time.Now(),
	}
}

// ==================== 读写循环 ====================

// Start 启动读写循环
// handler: 消息处理回调函数
func (c *Connection) Start(handler func(*Connection, *protocol.Message)) {
	go c.readLoop(handler)
	go c.writeLoop()
}

// readLoop 读取循环
// 持续从连接读取消息，调用 handler 处理
// 注意：当前实现中，tcp_server.go 直接在 handleConnection 中读取
//
//	这个方法是备用的更优雅的实现
func (c *Connection) readLoop(handler func(*Connection, *protocol.Message)) {
	defer c.Close()

	for {
		// 检查关闭信号
		select {
		case <-c.closeChan:
			return
		default:
		}

		// 设置读取超时
		// 90秒内没有数据（包括心跳）则认为连接死亡
		c.Conn.SetReadDeadline(time.Now().Add(90 * time.Second))

		// 读取消息
		msg, err := protocol.Unpack(c.reader)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				log.Printf("[Conn-%d] Read timeout, closing connection", c.ID)
			} else if err.Error() != "EOF" {
				log.Printf("[Conn-%d] Read error: %v", c.ID, err)
			}
			return
		}

		// 更新活跃时间
		c.updateLastActive()

		// 调用处理器
		handler(c, msg)
	}
}

// writeLoop 写入循环
// 从 writeChan 读取数据并发送到网络
//
// 为什么用单独的协程写入？
// 1. 解耦：发送方不需要等待网络 I/O
// 2. 性能：可以批量发送缓冲区中的数据
// 3. 安全：通道保证了并发安全
func (c *Connection) writeLoop() {
	defer c.Close()

	for {
		select {
		case <-c.closeChan:
			// 连接关闭，退出循环
			return

		case data := <-c.writeChan:
			// 设置写入超时，防止网络阻塞
			c.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))

			// 实际写入网络
			if _, err := c.Conn.Write(data); err != nil {
				log.Printf("[Conn-%d] Write error: %v", c.ID, err)
				return
			}
		}
	}
}

// ==================== 发送消息 ====================

// Send 发送消息（异步）
// 消息会被放入 writeChan，由 writeLoop 实际发送
//
// 返回值：
//   - nil: 消息已放入队列（不代表已发送成功）
//   - error: 连接已关闭或通道已满
func (c *Connection) Send(msg *protocol.Message) error {
	// 序列化消息
	data, err := protocol.Pack(msg)
	if err != nil {
		return err
	}

	// 非阻塞发送
	select {
	case c.writeChan <- data:
		// 成功放入通道
		return nil

	case <-c.closeChan:
		// 连接已关闭
		return net.ErrClosed

	default:
		// 通道已满，说明客户端处理不过来
		// 这里选择丢弃消息而不是阻塞
		// 在生产环境可能需要更复杂的处理（如：断开连接）
		log.Printf("[Conn-%d] Write channel full, dropping message", c.ID)
		return nil
	}
}

// ==================== 连接生命周期 ====================

// Close 关闭连接
// 使用 sync.Once 保证只执行一次
func (c *Connection) Close() {
	c.closeOnce.Do(func() {
		// 关闭信号通道，通知所有监听者
		close(c.closeChan)
		// 关闭底层连接
		c.Conn.Close()
	})
}

// IsClosed 检查连接是否已关闭
func (c *Connection) IsClosed() bool {
	select {
	case <-c.closeChan:
		return true
	default:
		return false
	}
}

// ==================== 活跃时间管理 ====================

// updateLastActive 更新最后活跃时间
func (c *Connection) updateLastActive() {
	c.mu.Lock()
	c.lastActive = time.Now()
	c.mu.Unlock()
}

// GetLastActive 获取最后活跃时间
func (c *Connection) GetLastActive() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastActive
}

// ==================== 用户绑定 ====================

// SetUserID 绑定用户 ID
// 在用户认证成功后调用
func (c *Connection) SetUserID(uid string) {
	c.mu.Lock()
	c.UserID = uid
	c.mu.Unlock()
}

// GetUserID 获取用户 ID
func (c *Connection) GetUserID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.UserID
}

// ==================================================================
// ConnectionManager - 连接管理器
// ==================================================================

/*
ConnectionManager 管理所有活跃连接

核心数据结构：

 1. connections: ConnID → Connection
    用于按连接 ID 查找

 2. userConns: UserID → Connection
    用于消息路由（根据用户 ID 找到其连接）

为什么用 sync.Map 而不是 map + mutex？
- sync.Map 对读多写少的场景优化
- 连接查找（读）远多于连接建立/断开（写）
- 内置并发安全，代码更简洁
*/
type ConnectionManager struct {
	// connections 连接 ID → 连接对象
	connections sync.Map

	// userConns 用户 ID → 连接对象
	// 用于消息路由：知道用户 ID，需要找到其连接
	userConns sync.Map
}

// NewConnectionManager 创建连接管理器
func NewConnectionManager() *ConnectionManager {
	return &ConnectionManager{}
}

// Add 添加连接
func (m *ConnectionManager) Add(conn *Connection) {
	m.connections.Store(conn.ID, conn)
}

// Remove 移除连接
func (m *ConnectionManager) Remove(conn *Connection) {
	// 从连接表中移除
	m.connections.Delete(conn.ID)

	// 如果已绑定用户，也要从用户表中移除
	if uid := conn.GetUserID(); uid != "" {
		m.userConns.Delete(uid)
	}
}

// BindUser 绑定用户到连接
// 在用户认证成功后调用
// 这使得后续可以通过 UserID 快速找到连接
func (m *ConnectionManager) BindUser(uid string, conn *Connection) {
	conn.SetUserID(uid)
	m.userConns.Store(uid, conn)
}

// GetByUserID 根据用户 ID 获取连接
// 这是消息路由的核心：知道要发送给谁，找到他的连接
func (m *ConnectionManager) GetByUserID(uid string) *Connection {
	if v, ok := m.userConns.Load(uid); ok {
		return v.(*Connection)
	}
	return nil
}

// GetByConnID 根据连接 ID 获取连接
func (m *ConnectionManager) GetByConnID(id uint64) *Connection {
	if v, ok := m.connections.Load(id); ok {
		return v.(*Connection)
	}
	return nil
}

// Count 获取当前连接数
func (m *ConnectionManager) Count() int {
	count := 0
	m.connections.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	return count
}

// Broadcast 广播消息给所有连接
// 用于系统公告等场景
func (m *ConnectionManager) Broadcast(msg *protocol.Message) {
	m.connections.Range(func(_, v interface{}) bool {
		conn := v.(*Connection)
		conn.Send(msg)
		return true
	})
}
