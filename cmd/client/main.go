package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"go-im/protocol"
	"go-im/service"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

func main() {
	// Parse flags
	serverAddr := flag.String("server", "127.0.0.1:8080", "Server address")
	userID := flag.String("user", "user1", "User ID")
	flag.Parse()

	// Connect to server
	conn, err := net.Dial("tcp", *serverAddr)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	log.Printf("Connected to server as %s", *userID)

	// Generate token for this user
	token, err := service.GenerateToken(*userID, *userID)
	if err != nil {
		log.Fatalf("Failed to generate token: %v", err)
	}

	// Start receiver goroutine
	go receiveMessages(conn)

	// Send auth request
	sendAuth(conn, token)

	// Start heartbeat
	go heartbeat(conn)

	// Read commands from stdin
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("\nCommands:")
	fmt.Println("  send <user_id> <message> - Send message to user")
	fmt.Println("  quit - Exit")
	fmt.Println()

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, " ", 3)

		if len(parts) == 0 {
			continue
		}

		switch parts[0] {
		case "quit":
			fmt.Println("Exiting...")
			return
		case "send":
			if len(parts) < 3 {
				fmt.Println("Usage: send <user_id> <message>")
				continue
			}
			sendMessage(conn, parts[1], parts[2])
		default:
			fmt.Println("Unknown command. Use 'send <user_id> <message>' or 'quit'")
		}
	}
}

func receiveMessages(conn net.Conn) {
	reader := bufio.NewReader(conn)
	for {
		msg, err := protocol.Unpack(reader)
		if err != nil {
			log.Printf("Receive error: %v", err)
			return
		}

		switch msg.CmdType {
		case protocol.CmdTypeAuthAck:
			var resp map[string]interface{}
			json.Unmarshal(msg.Body, &resp)
			if resp["success"] == true {
				log.Printf("✓ Authentication successful")
			} else {
				log.Printf("✗ Authentication failed: %v", resp["message"])
			}

		case protocol.CmdTypeMessage:
			var chatMsg struct {
				FromUserID string `json:"from_user_id"`
				Content    string `json:"content"`
				SeqID      int64  `json:"seq_id"`
			}
			json.Unmarshal(msg.Body, &chatMsg)
			fmt.Printf("\n[%s] → %s\n", chatMsg.FromUserID, chatMsg.Content)

			// Send ACK
			sendAck(conn, chatMsg.SeqID)

		case protocol.CmdTypeHeartbeat:
			// Heartbeat response received

		case protocol.CmdTypeKick:
			log.Printf("Server requested reconnect: %s", string(msg.Body))

		default:
			log.Printf("Unknown message type: %d", msg.CmdType)
		}
	}
}

func sendAuth(conn net.Conn, token string) {
	data, _ := json.Marshal(map[string]string{"token": token})
	msg := &protocol.Message{
		CmdType: protocol.CmdTypeAuth,
		Body:    data,
	}
	sendPacket(conn, msg)
}

func sendMessage(conn net.Conn, toUserID, content string) {
	data, _ := json.Marshal(map[string]string{
		"to_user_id": toUserID,
		"content":    content,
	})
	msg := &protocol.Message{
		CmdType: protocol.CmdTypeMessage,
		Body:    data,
	}
	sendPacket(conn, msg)
	log.Printf("→ [%s] %s", toUserID, content)
}

func sendAck(conn net.Conn, seqID int64) {
	data, _ := json.Marshal(map[string]int64{"seq_id": seqID})
	msg := &protocol.Message{
		CmdType: protocol.CmdTypeMessageAck,
		Body:    data,
	}
	sendPacket(conn, msg)
}

func heartbeat(conn net.Conn) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		msg := &protocol.Message{
			CmdType: protocol.CmdTypeHeartbeat,
			Body:    []byte("ping"),
		}
		if err := sendPacket(conn, msg); err != nil {
			return
		}
	}
}

func sendPacket(conn net.Conn, msg *protocol.Message) error {
	data, err := protocol.Pack(msg)
	if err != nil {
		return err
	}
	_, err = conn.Write(data)
	return err
}
