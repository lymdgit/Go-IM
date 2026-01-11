/*
Package protocol - 自定义二进制协议实现

=== 为什么需要自定义协议？===

TCP 是流式协议，数据像水流一样连续传输，没有消息边界。
这会导致两个经典问题：

 1. 粘包 (Sticky Packet)：多个消息粘在一起
    发送: [消息A][消息B] → 接收: [消息A消息B]

 2. 拆包 (Half Packet)：一个消息被拆成多次接收
    发送: [消息A] → 接收: [消息A前半部分] + [消息A后半部分]

=== 解决方案：长度前缀协议 (Length-Prefixed Protocol) ===

消息格式:
+----------+----------+----------+-----------------+
|  Length  | Version  | CmdType  |      Body       |
|  4字节   |  2字节   |  2字节   |     N字节       |
+----------+----------+----------+-----------------+
|<-------- 固定 8 字节头部 ------->|<-- 变长消息体 -->|

解包步骤：
1. 先读取固定 8 字节头部
2. 从头部解析出 Length 字段，得知消息体长度
3. 再精确读取 N 字节的消息体
4. 这样就能准确切分每个消息，解决粘包/拆包问题

=== 与其他方案的对比 ===

| 方案            | 优点           | 缺点                |
|-----------------|----------------|---------------------|
| 长度前缀 (本项目)| 高效、精确     | 需要自己实现        |
| 分隔符 (\n)     | 简单           | 消息体不能包含分隔符|
| HTTP/JSON       | 通用           | 开销大、解析慢      |
| Protobuf        | 高效、跨语言   | 需要预定义 schema   |
*/
package protocol

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
)

// ==================== 协议常量定义 ====================

const (
	// HeaderLength 协议头部固定长度
	// Length(4) + Version(2) + CmdType(2) = 8 字节
	HeaderLength = 8

	// MaxPayloadLength 消息体最大长度限制 (1MB)
	// 这是一个重要的安全措施：
	// - 防止恶意客户端发送超大包，导致服务器 OOM (内存溢出)
	// - 参考了 Redis 的 query buffer 限制设计
	MaxPayloadLength = 1024 * 1024

	// ProtocolVersion 当前协议版本号
	// 用于后续协议升级时的兼容性处理
	ProtocolVersion = 1
)

// ==================== 命令类型定义 ====================
// 类似 HTTP 的请求方法，定义了不同的操作类型

const (
	// CmdTypeHeartbeat 心跳包
	// 客户端定期发送，用于：
	// 1. 保持连接活跃（防止被中间设备如 NAT 断开）
	// 2. 检测连接是否存活
	CmdTypeHeartbeat = iota + 1

	// CmdTypeAuth 认证请求
	// 客户端连接后发送，携带 JWT Token
	CmdTypeAuth

	// CmdTypeAuthAck 认证响应
	// 服务端返回认证结果
	CmdTypeAuthAck

	// CmdTypeMessage 聊天消息
	// 客户端发送的聊天内容
	CmdTypeMessage

	// CmdTypeMessageAck 消息确认
	// 客户端收到消息后回复，用于可靠性保证
	CmdTypeMessageAck

	// CmdTypeKick 踢出通知
	// 服务端通知客户端断开（如：重复登录、服务器重启）
	CmdTypeKick
)

// ==================== 消息结构体 ====================

/*
Message 表示一个完整的协议消息

内存布局示例 (发送 "Hello"):
+--------+--------+--------+-------------------+
| 00 00  | 00 01  | 00 04  | H  e  l  l  o     |
| 00 09  |        |        |                   |
+--------+--------+--------+-------------------+

	Length   Version  CmdType      Body
	= 9       = 1      = 4      = "Hello"

Length = 4 (Version+CmdType) + 5 (Body长度) = 9
*/
type Message struct {
	// Length 消息长度（不包括 Length 字段本身的 4 字节）
	// Length = 2(Version) + 2(CmdType) + len(Body)
	Length uint32

	// Version 协议版本号，用于兼容性
	Version uint16

	// CmdType 命令类型，见上方常量定义
	CmdType uint16

	// Body 消息体，实际内容（通常是 JSON）
	Body []byte
}

// ==================== 错误定义 ====================

var (
	// ErrPayloadTooLarge 消息体过大错误
	ErrPayloadTooLarge = errors.New("payload exceeds maximum allowed size")

	// ErrInvalidHeader 无效的消息头
	ErrInvalidHeader = errors.New("invalid message header")
)

// ==================== 封包函数 ====================

/*
Pack 将 Message 结构体序列化为字节数组（封包）

使用场景：发送消息前，将结构体转换为字节流

示例：

	msg := &Message{CmdType: CmdTypeMessage, Body: []byte("Hello")}
	data, err := Pack(msg)
	conn.Write(data)  // 发送到网络

字节序：使用大端序 (Big Endian)
- 网络传输的标准字节序
- 高位字节在前，符合人类阅读习惯
- 例如：数字 258 → [0x01, 0x02] 而非 [0x02, 0x01]
*/
func Pack(msg *Message) ([]byte, error) {
	bodyLen := len(msg.Body)

	// 安全检查：防止发送过大的消息
	if bodyLen > MaxPayloadLength {
		return nil, ErrPayloadTooLarge
	}

	// 计算 Length 字段值
	// Length = Version(2字节) + CmdType(2字节) + Body(N字节)
	msg.Length = uint32(4 + bodyLen)
	msg.Version = ProtocolVersion

	// 分配缓冲区：头部(8字节) + 消息体(N字节)
	data := make([]byte, HeaderLength+bodyLen)

	// 写入头部（使用大端序）
	// binary.BigEndian 确保跨平台的字节序一致性
	binary.BigEndian.PutUint32(data[0:4], msg.Length)  // 字节 0-3: Length
	binary.BigEndian.PutUint16(data[4:6], msg.Version) // 字节 4-5: Version
	binary.BigEndian.PutUint16(data[6:8], msg.CmdType) // 字节 6-7: CmdType

	// 写入消息体
	if bodyLen > 0 {
		copy(data[HeaderLength:], msg.Body)
	}

	return data, nil
}

// ==================== 解包函数 ====================

/*
Unpack 从 Reader 中读取并解析一个完整的 Message（解包）

这是解决 TCP 粘包/拆包的核心函数！

关键技术：io.ReadFull
- 普通的 Read() 可能只读取部分数据就返回
- ReadFull() 保证读取指定数量的字节，否则返回错误
- 这确保了我们能精确读取完整的消息

解包流程图：

	┌─────────────────────────────────────────┐
	│          TCP 字节流                      │
	│  [消息1头部][消息1体][消息2头部][消息2体]... │
	└─────────────────────────────────────────┘
	                ↓ ReadFull(8字节)
	┌─────────────┐
	│  消息1头部   │ → 解析得到 Length=100
	└─────────────┘
	                ↓ ReadFull(96字节)  // 100-4=96
	┌─────────────┐
	│  消息1体     │ → 得到完整消息1
	└─────────────┘
	                ↓ 循环处理消息2...

参数：
- reader: bufio.Reader 包装的网络连接，提供缓冲读取

返回：
- *Message: 解析出的完整消息
- error: 错误信息（EOF 表示连接关闭）
*/
func Unpack(reader *bufio.Reader) (*Message, error) {
	// ========== 步骤 1: 读取固定长度的头部 (8 字节) ==========
	header := make([]byte, HeaderLength)
	_, err := io.ReadFull(reader, header)
	if err != nil {
		// EOF: 对端正常关闭连接
		// ErrUnexpectedEOF: 读取中途连接断开
		return nil, err
	}

	// ========== 步骤 2: 解析头部字段 ==========
	msg := &Message{
		Length:  binary.BigEndian.Uint32(header[0:4]),
		Version: binary.BigEndian.Uint16(header[4:6]),
		CmdType: binary.BigEndian.Uint16(header[6:8]),
	}

	// ========== 步骤 3: 计算并验证消息体长度 ==========
	// bodyLen = Length - Version(2) - CmdType(2)
	bodyLen := int(msg.Length) - 4

	// 安全检查 1: 防止负数长度（可能是协议错误或攻击）
	if bodyLen < 0 {
		return nil, ErrInvalidHeader
	}

	// 安全检查 2: 防止恶意大包攻击
	// 如果不检查，攻击者可以发送 Length=0xFFFFFFFF
	// 导致服务器尝试分配 4GB 内存，造成 OOM
	if bodyLen > MaxPayloadLength {
		return nil, ErrPayloadTooLarge
	}

	// ========== 步骤 4: 读取消息体 ==========
	if bodyLen > 0 {
		msg.Body = make([]byte, bodyLen)
		_, err = io.ReadFull(reader, msg.Body)
		if err != nil {
			return nil, err
		}
	}

	return msg, nil
}
