/*
Package service - 用户认证服务

=== JWT (JSON Web Token) 简介 ===

JWT 是一种无状态的认证机制，由三部分组成：

	Header.Payload.Signature
	(头部).(载荷).(签名)

示例 Token:

		eyJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoiYWxpY2UifQ.xxxxx

	 1. Header（头部）: 算法类型
	    {"alg": "HS256", "typ": "JWT"}

	 2. Payload（载荷）: 用户数据
	    {"user_id": "alice", "exp": 1699999999}

	 3. Signature（签名）: 防篡改
	    HMAC-SHA256(Header + Payload, 密钥)

=== 为什么用 JWT？===

对比 Session 方式：

Session 方式:

	Client ──────────────────────> Gateway-1
	          SessionID: abc123
	                 │
	                 ▼
	           ┌──────────┐
	           │  Redis   │  需要查询 Session
	           │ abc123→  │
	           │ {user:x} │
	           └──────────┘

JWT 方式:

	Client ──────────────────────> Gateway-1
	          Token: eyJxxx...
	                 │
	                 ▼
	           本地验证签名即可，无需查询 Redis ✓

优势：
- 无状态：不需要在服务端存储 Session
- 可扩展：任何 Gateway 都能验证，天然支持分布式
- 性能高：本地验证，不需要网络请求
*/
package service

import (
	"errors"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// ==================== 配置常量 ====================

var (
	// JWTSecret 签名密钥
	// 警告：生产环境必须使用复杂的随机字符串，并从配置文件读取
	JWTSecret = []byte("go-im-secret-key-change-in-production")

	// TokenExpireDuration Token 过期时间
	TokenExpireDuration = 24 * time.Hour
)

// ==================== 错误定义 ====================

var (
	// ErrInvalidToken Token 无效（格式错误或签名不匹配）
	ErrInvalidToken = errors.New("invalid token")

	// ErrTokenExpired Token 已过期
	ErrTokenExpired = errors.New("token expired")
)

// ==================== Claims 结构 ====================

// Claims JWT 载荷
// 继承 jwt.RegisteredClaims 获得标准字段（过期时间等）
type Claims struct {
	// UserID 用户唯一标识
	UserID string `json:"user_id"`

	// Username 用户名（可选，用于显示）
	Username string `json:"username"`

	// RegisteredClaims 标准字段
	// - ExpiresAt: 过期时间
	// - IssuedAt: 签发时间
	// - Issuer: 签发者
	jwt.RegisteredClaims
}

// ==================== Token 生成 ====================

// GenerateToken 生成 JWT Token
//
// 参数:
//   - userID: 用户唯一标识
//   - username: 用户名
//
// 返回:
//   - string: 生成的 Token 字符串
//   - error: 错误信息
//
// 示例:
//
//	token, err := GenerateToken("user123", "Alice")
//	// token = "eyJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoidXNlcjEyMyJ9.xxxxx"
func GenerateToken(userID, username string) (string, error) {
	// 构造 Claims
	claims := &Claims{
		UserID:   userID,
		Username: username,
		RegisteredClaims: jwt.RegisteredClaims{
			// 过期时间
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(TokenExpireDuration)),
			// 签发时间
			IssuedAt: jwt.NewNumericDate(time.Now()),
			// 签发者
			Issuer: "go-im",
		},
	}

	// 创建 Token 对象
	// HS256 = HMAC + SHA256，是最常用的对称加密算法
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)

	// 使用密钥签名，生成最终的 Token 字符串
	return token.SignedString(JWTSecret)
}

// ==================== Token 验证 ====================

// ValidateToken 验证 JWT Token
//
// 验证过程：
// 1. 解析 Token 字符串
// 2. 验证签名（使用相同的密钥）
// 3. 检查是否过期
// 4. 返回解析出的用户信息
//
// 参数:
//   - tokenString: 要验证的 Token
//
// 返回:
//   - *Claims: 解析出的用户信息
//   - error: 验证失败的原因
func ValidateToken(tokenString string) (*Claims, error) {
	// 解析并验证 Token
	token, err := jwt.ParseWithClaims(
		tokenString,
		&Claims{},
		func(token *jwt.Token) (interface{}, error) {
			// 返回签名密钥，用于验证签名
			return JWTSecret, nil
		},
	)

	if err != nil {
		// 检查是否是过期错误
		if errors.Is(err, jwt.ErrTokenExpired) {
			return nil, ErrTokenExpired
		}
		return nil, ErrInvalidToken
	}

	// 类型断言，提取 Claims
	if claims, ok := token.Claims.(*Claims); ok && token.Valid {
		return claims, nil
	}

	return nil, ErrInvalidToken
}
