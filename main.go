package main

import (
	//"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

var dbConfig = redis.Options{
	Addr:     "localhost:6379", // Redis host and port
	Password: "",               // No password by default
	DB:       0,                // Default database
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins (configure properly in production)
	},
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

func main() {
	hub := NewHub()
	go hub.Run()

	rdb := GetClient()

	if rdb == nil {
		InitRedis(dbConfig.Addr, dbConfig.Password, dbConfig.DB)
		rdb = GetClient()
	}

	// HTTP endpoint for WebSocket upgrades
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		handleWebSocket(hub, rdb, w, r)
	})

	// Health check endpoint
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// Start server
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	server := &http.Server{
		Addr: ":" + port,
	}

	go func() {
		log.Printf("Signaling server starting on :%s", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("Server failed:", err)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")
	hub.Stop()
	server.Close()
}

func handleWebSocket(hub *Hub, rdb *redis.Client, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("WebSocket upgrade failed:", err)
		return
	}

	client := &Client{
		hub:       hub,
		rdb:       rdb,
		conn:      conn,
		send:      make(chan []byte, 256),
		id:        "", // Will be set when client registers
		closeOnce: sync.Once{},
	}

	// The client is added to the hub only once it sends a "register"
	// message with a valid user ID (handled in client.handleMessage).
	go client.writePump()
	go client.readPump()
}
