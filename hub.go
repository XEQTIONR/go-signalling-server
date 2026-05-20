package main

import (
	"encoding/json"
	"log"
	"sync"
)

type Hub struct {
	// Registered clients by user ID
	clients   map[string]*Client
	clientsMu sync.RWMutex

	// Register requests
	register chan *Client

	// Unregister requests
	unregister chan *Client

	// Broadcast to all (for room broadcasts)
	broadcast chan []byte

	stop chan struct{}
}

type Message struct {
	Type string          `json:"type"`
	From string          `json:"from,omitempty"`
	To   string          `json:"to,omitempty"`
	Data json.RawMessage `json:"data,omitempty"`
}

func NewHub() *Hub {
	return &Hub{
		clients:    make(map[string]*Client),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		broadcast:  make(chan []byte),
		stop:       make(chan struct{}),
	}
}

func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			// Wait for client to register with an ID
			// Client will send a register message
			log.Printf("Client %s registered", client.id)
			continue

		case client := <-h.unregister:
			h.clientsMu.Lock()
			if _, ok := h.clients[client.id]; ok {
				delete(h.clients, client.id)
				log.Printf("Client %s disconnected", client.id)
			}
			h.clientsMu.Unlock()
			close(client.send)

		case message := <-h.broadcast:
			// Broadcast to all clients (useful for rooms)
			h.clientsMu.RLock()
			for _, client := range h.clients {
				select {
				case client.send <- message:
				default:
					close(client.send)
					delete(h.clients, client.id)
				}
			}
			h.clientsMu.RUnlock()

		case <-h.stop:
			return
		}
	}
}

func (h *Hub) RegisterClient(client *Client, userID string) bool {
	h.clientsMu.Lock()
	defer h.clientsMu.Unlock()

	// Check if user already connected
	if existing, ok := h.clients[userID]; ok && existing != client {
		return false // Already connected
	}

	client.id = userID
	h.clients[userID] = client
	log.Printf("Client %s registered", userID)
	return true
}

func (h *Hub) UnregisterClient(userID string) {
	h.clientsMu.Lock()
	defer h.clientsMu.Unlock()

	if client, ok := h.clients[userID]; ok {
		delete(h.clients, userID)
		close(client.send)
		log.Printf("Client %s unregistered", userID)
	}
}

func (h *Hub) SendToUser(userID string, message []byte) bool {
	h.clientsMu.RLock()
	client, ok := h.clients[userID]
	h.clientsMu.RUnlock()

	if !ok {
		return false
	}

	select {
	case client.send <- message:
		return true
	default:
		go h.UnregisterClient(userID)
		return false
	}
}

func (h *Hub) Stop() {
	close(h.stop)
}
