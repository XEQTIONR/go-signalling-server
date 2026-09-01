package main

import (
	"encoding/json"
	"log"
	"slices"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 8192
)

type Client struct {
	hub       *Hub
	rdb       *redis.Client
	conn      *websocket.Conn
	send      chan []byte
	id        string
	closeOnce sync.Once
}

// closeSend closes the send channel exactly once so that multiple
// disconnect paths (broadcast eviction, unregister, shutdown) cannot
// race and panic on a double close.
func (c *Client) closeSend() {
	c.closeOnce.Do(func() {
		close(c.send)
	})
}

// WebRTC signaling message types
type SignalMessage struct {
	Type    string          `json:"type"`
	From    string          `json:"from,omitempty"`
	To      string          `json:"to,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

func (c *Client) readPump() {
	defer func() {
		select {
		case c.hub.unregister <- c:
		case <-c.hub.stop:
		}
		c.conn.Close()
	}()

	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("WebSocket error: %v", err)
			}
			break
		}

		var msg SignalMessage
		if err := json.Unmarshal(message, &msg); err != nil {
			log.Printf("Invalid JSON: %v", err)
			continue
		}

		c.handleMessage(msg)
	}
}

func (c *Client) handleMessage(msg SignalMessage) {
	switch msg.Type {
	case "register":
		// Client sends: {"type":"register","payload":{"userId":"alice123"}}
		var registerData struct {
			UserID  string `json:"userId"`
			ClassID string `json:"classId"`
		}

		if err := json.Unmarshal(msg.Payload, &registerData); err != nil {
			c.sendError("Invalid register payload")
			return
		}

		if c.id != "" {
			c.sendError("Already registered")
			return
		}

		if !c.hub.RegisterClient(c, registerData.UserID, registerData.ClassID) {
			c.sendError("User already connected")
			return
		}

		// bookkeeping
		classData, err := GetValues(registerData.ClassID)
		if err != nil {
			c.sendError("Failed to get class data")
			return
		}

		if !slices.Contains(classData, registerData.UserID) {
			SetValues(registerData.ClassID, append(classData, registerData.UserID))
			SetValue(registerData.UserID, registerData.ClassID)

			for _, userID := range classData {
				if userID != registerData.UserID {
					payload, err := json.Marshal(map[string]any{
						"class_id": registerData.ClassID,
						"user_id":  userID,
					})
					if err != nil {
						log.Printf("failed to marshal new_user_joined payload: %v", err)
						continue
					}
					data, _ := json.Marshal(SignalMessage{
						Type:    "new_user_joined",
						From:    registerData.UserID,
						Payload: payload,
					})
					c.hub.SendToUser(userID, data)
				}
			}
		}

		c.sendSuccess("Registered successfully")

	case "call":
		// {"type":"call","to":"bob456","payload":{"sdp":"..."}}
		if c.id == "" {
			c.sendError("Not registered")
			return
		}

		if msg.To == "" {
			c.sendError("Missing target user")
			return
		}

		// Forward offer to target user
		forwardMsg := SignalMessage{
			Type:    "incoming_call",
			From:    c.id,
			Payload: msg.Payload,
		}

		data, _ := json.Marshal(forwardMsg)
		if !c.hub.SendToUser(msg.To, data) {
			c.sendError("User not connected: " + msg.To)
		}

	case "answer":
		// {"type":"answer","to":"alice123","payload":{"sdp":"..."}}
		if c.id == "" {
			c.sendError("Not registered")
			return
		}

		forwardMsg := SignalMessage{
			Type:    "call_answered",
			From:    c.id,
			Payload: msg.Payload,
		}

		data, _ := json.Marshal(forwardMsg)
		c.hub.SendToUser(msg.To, data)

	case "ice-candidate":
		// {"type":"ice-candidate","to":"bob456","payload":{"candidate":"..."}}
		if c.id == "" {
			c.sendError("Not registered")
			return
		}

		forwardMsg := SignalMessage{
			Type:    "ice-candidate",
			From:    c.id,
			Payload: msg.Payload,
		}

		data, _ := json.Marshal(forwardMsg)
		c.hub.SendToUser(msg.To, data)

	case "hangup":
		// The person you were on a call with hung up
		// @TODO: Rework this
		// {"type":"hangup","to":"bob456"}
		if c.id == "" {
			return
		}

		forwardMsg := SignalMessage{
			Type: "hangup",
			From: c.id,
		}

		data, _ := json.Marshal(forwardMsg)
		c.hub.SendToUser(msg.To, data)

	default:
		log.Printf("Unknown message type: %s", msg.Type)
		c.sendError("Unknown message type")
	}
}

func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
				return
			}

		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (c *Client) sendStatus(msgType, message string) {
	payload, err := json.Marshal(message)
	if err != nil {
		log.Printf("failed to marshal %s payload: %v", msgType, err)
		return
	}
	data, err := json.Marshal(SignalMessage{Type: msgType, Payload: payload})
	if err != nil {
		log.Printf("failed to marshal %s message: %v", msgType, err)
		return
	}
	select {
	case c.send <- data:
	default:
	}
}

func (c *Client) sendError(message string) {
	c.sendStatus("error", message)
}

func (c *Client) sendSuccess(message string) {
	c.sendStatus("success", message)
}
