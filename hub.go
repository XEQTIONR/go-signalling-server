package main

import (
	"log"
	"sync"
	"time"
)

type Hub struct {
	// Registered clients by user ID
	clients   map[string]*Client
	clientsMu sync.RWMutex

	// Unregister requests (the only path that drives the unregister case
	// in Run; closing of client.send is funneled through Client.closeSend
	// so it cannot happen twice).
	unregister chan *Client

	// Broadcast to all (for room broadcasts)
	broadcast chan []byte

	stop chan struct{}
}

func NewHub() *Hub {
	return &Hub{
		clients:    make(map[string]*Client),
		unregister: make(chan *Client, 32),
		broadcast:  make(chan []byte, 32),
		stop:       make(chan struct{}),
	}
}

func (h *Hub) Run() {
	for {
		select {
		case client := <-h.unregister:

			h.removeClient(client)

			id := client.id
			classId, err := GetValue(id)

			if err != nil {
				log.Printf("Error getting class ID for client %s: %v", id, err)
				continue
			}

			if err := RemoveFromSet(classId, id); err != nil {
				log.Printf("Error removing client %s from class %s: %v", id, classId, err)
			}

			if err := DeleteValue(id); err != nil {
				log.Printf("Error deleting value for client %s: %v", id, err)
			}

		case message := <-h.broadcast:
			h.broadcastMessage(message)

		case <-h.stop:
			return
		}
	}
}

// removeClient deletes the client from the registry (if present) and
// closes its send channel via the idempotent helper on Client.
func (h *Hub) removeClient(client *Client) {
	h.clientsMu.Lock()
	if client.id != "" {
		if existing, ok := h.clients[client.id]; ok && existing == client {
			delete(h.clients, client.id)
			log.Printf("Client %s disconnected", client.id)
		}
	}
	h.clientsMu.Unlock()
	client.closeSend()
}

// broadcastMessage delivers message to every connected client.
// Clients whose send buffer is full are evicted; deletions happen under
// a write lock rather than the read lock used during the send loop.
func (h *Hub) broadcastMessage(message []byte) {
	var dead []*Client

	h.clientsMu.RLock()
	for _, client := range h.clients {
		select {
		case client.send <- message:
		default:
			dead = append(dead, client)
		}
	}
	h.clientsMu.RUnlock()

	if len(dead) == 0 {
		return
	}

	h.clientsMu.Lock()
	for _, client := range dead {
		if existing, ok := h.clients[client.id]; ok && existing == client {
			delete(h.clients, client.id)
		}
	}
	h.clientsMu.Unlock()

	for _, client := range dead {
		client.closeSend()
	}
}

func (h *Hub) RegisterClient(client *Client, userID string, classID string) bool {
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

// UnregisterClient asks the hub to remove the named user. The actual
// map mutation and channel close happen on the hub's own goroutine via
// the unregister channel so that all close paths are serialized.
func (h *Hub) UnregisterClient(userID string) {
	h.clientsMu.RLock()
	client, ok := h.clients[userID]
	h.clientsMu.RUnlock()
	if !ok {
		return
	}

	select {
	case h.unregister <- client:
	case <-h.stop:
	}
}

// SendToUser delivers a message to a specific user. It blocks up to
// writeWait so that a momentarily-busy client does not lose signaling
// messages, but evicts the client if the buffer stays full beyond that.
func (h *Hub) SendToUser(userID string, message []byte) bool {
	h.clientsMu.RLock()
	client, ok := h.clients[userID]
	h.clientsMu.RUnlock()

	if !ok {
		return false
	}

	timer := time.NewTimer(writeWait)
	defer timer.Stop()

	select {
	case client.send <- message:
		return true
	case <-timer.C:
		select {
		case h.unregister <- client:
		default:
		}
		return false
	case <-h.stop:
		return false
	}
}

// Stop signals Run to exit and tears down all connected clients so that
// any goroutine blocked on client.send unblocks and the WebSocket loops
// can return cleanly.
func (h *Hub) Stop() {
	close(h.stop)

	h.clientsMu.Lock()
	clients := make([]*Client, 0, len(h.clients))
	for id, client := range h.clients {
		clients = append(clients, client)
		delete(h.clients, id)
	}
	h.clientsMu.Unlock()

	for _, client := range clients {
		client.closeSend()
		_ = client.conn.Close()
	}
}
