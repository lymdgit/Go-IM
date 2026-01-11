/*
Package redis - Redis 客户端封装

=== Redis 在 IM 系统中的作用 ===

Redis 作为高性能的内存数据库，在本项目中承担多种角色：

1. 用户状态存储（user_session:uid）
  - 存储用户登录状态
  - EXPIRE 实现自动过期

2. 消息路由（user_gateway:uid）
  - 记录用户连接在哪个 Gateway
  - 支持跨节点消息转发

3. 消息队列（Pub/Sub）
  - 跨 Gateway 的消息广播
  - 实时消息推送

4. 离线消息（msg_box:uid）
  - ZSet 存储离线消息
  - 支持按序列号排序

5. 消息序列号（seq:conversation）
  - INCR 原子操作
  - 保证消息时序

=== 连接池的重要性 ===

	┌─────────────────────────────────────────┐
	│           Connection Pool               │
	│  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐       │
	│  │Conn1│ │Conn2│ │Conn3│ │ConnN│       │
	│  └──┬──┘ └──┬──┘ └──┬──┘ └──┬──┘       │
	│     │       │       │       │           │
	│     └───────┴───────┴───────┘           │
	│              复用连接                    │
	└─────────────────────────────────────────┘

为什么需要连接池？
- 避免频繁创建/销毁 TCP 连接的开销
- 复用连接，减少延迟
- 控制最大连接数，防止资源耗尽
*/
package redis

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

// ==================== 全局变量 ====================

var (
	// Client Redis 客户端实例（全局单例）
	Client *redis.Client

	// ctx 默认上下文
	ctx = context.Background()
)

// ==================== 配置结构 ====================

// Config Redis 连接配置
type Config struct {
	// Addr Redis 服务器地址，如 "127.0.0.1:6379"
	Addr string

	// Password Redis 密码（可选）
	Password string

	// DB 数据库编号（0-15）
	DB int

	// PoolSize 连接池大小
	// 根据并发量调整，默认 100
	PoolSize int
}

// ==================== 初始化函数 ====================

// Init 初始化 Redis 客户端
// 这是项目启动时必须调用的函数
func Init(cfg *Config) error {
	// 默认连接池大小
	if cfg.PoolSize == 0 {
		cfg.PoolSize = 100
	}

	// 创建 Redis 客户端
	Client = redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,

		// === 连接池配置 ===
		PoolSize:     cfg.PoolSize, // 最大连接数
		MinIdleConns: 10,           // 最小空闲连接

		// === 超时配置 ===
		DialTimeout:  5 * time.Second, // 连接超时
		ReadTimeout:  3 * time.Second, // 读取超时
		WriteTimeout: 3 * time.Second, // 写入超时
	})

	// 测试连接
	// PING 命令验证 Redis 是否可达
	if err := Client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis connection failed: %w", err)
	}

	log.Println("[Redis] Connected successfully")
	return nil
}

// Close 关闭 Redis 连接
// 在程序退出时调用
func Close() {
	if Client != nil {
		Client.Close()
	}
}

// ==================== Pipeline 批量操作 ====================

/*
Pipeline 执行批量命令

什么是 Pipeline？
- 将多个命令打包成一次网络请求发送
- 减少网络往返次数 (RTT)

普通模式 vs Pipeline:

普通模式（3次 RTT）:

	Client ----GET----> Redis
	Client <---value--- Redis
	Client ----SET----> Redis
	Client <---OK------ Redis
	Client ----INCR---> Redis
	Client <---1------- Redis

Pipeline 模式（1次 RTT）:

	Client --[GET,SET,INCR]--> Redis
	Client <-[value,OK,1]----- Redis

使用示例:

	err := Pipeline(func(pipe redis.Pipeliner) error {
	    pipe.Set(ctx, "key1", "value1", 0)
	    pipe.Set(ctx, "key2", "value2", 0)
	    pipe.Incr(ctx, "counter")
	    return nil
	})
*/
func Pipeline(fn func(pipe redis.Pipeliner) error) error {
	_, err := Client.Pipelined(ctx, fn)
	return err
}

// Context 获取默认上下文
func Context() context.Context {
	return ctx
}
