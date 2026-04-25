package main

import (
	"bytes"
	"log"
	"net/http"
	"sync"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type Message struct {
	SenderID string
	Data     []byte
}

type Client struct {
	hub  *Hub
	id   string
	conn *websocket.Conn
	send chan []byte
}

type Hub struct {
	clients    map[*Client]bool
	broadcast  chan Message
	register   chan *Client
	unregister chan *Client
	mu         sync.Mutex
	headers    map[string][]byte // Store the first chunk of each client's stream
}

func newHub() *Hub {
	return &Hub{
		broadcast:  make(chan Message),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		clients:    make(map[*Client]bool),
		headers:    make(map[string][]byte),
	}
}

func (h *Hub) run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			// Send existing stream headers to the new client
			for id, header := range h.headers {
				if id != client.id {
					client.send <- prepareMessage(id, header)
				}
			}
			h.mu.Unlock()
			log.Printf("Client %s registered", client.id)

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				delete(h.headers, client.id)
				close(client.send)
				log.Printf("Client %s unregistered", client.id)
			}
			h.mu.Unlock()

		case message := <-h.broadcast:
			h.mu.Lock()
			// Store header if it's the first chunk from this sender
			if _, ok := h.headers[message.SenderID]; !ok {
				h.headers[message.SenderID] = message.Data
			}

			payload := prepareMessage(message.SenderID, message.Data)

			for client := range h.clients {
				if client.id == message.SenderID {
					continue // Don't send back to sender
				}
				select {
				case client.send <- payload:
				default:
					close(client.send)
					delete(h.clients, client)
					delete(h.headers, client.id)
				}
			}
			h.mu.Unlock()
		}
	}
}

// prepareMessage prefixes the data with the SenderID length and the SenderID itself
func prepareMessage(senderID string, data []byte) []byte {
	var buf bytes.Buffer
	buf.WriteByte(byte(len(senderID)))
	buf.WriteString(senderID)
	buf.Write(data)
	return buf.Bytes()
}

func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()
	for {
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			break
		}
		c.hub.broadcast <- Message{SenderID: c.id, Data: data}
	}
}

func (c *Client) writePump() {
	defer func() {
		c.conn.Close()
	}()
	for {
		select {
		case message, ok := <-c.send:
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			err := c.conn.WriteMessage(websocket.BinaryMessage, message)
			if err != nil {
				return
			}
		}
	}
}

func serveWs(hub *Hub, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}
	id := uuid.New().String()
	client := &Client{hub: hub, id: id, conn: conn, send: make(chan []byte, 256)}
	client.hub.register <- client

	go client.writePump()
	go client.readPump()
}

func main() {
	hub := newHub()
	go hub.run()

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWs(hub, w, r)
	})

	// Serve static files from the Angular build output
	fs := http.FileServer(http.Dir("../frontend/dist/frontend/browser"))
	http.Handle("/", fs)

	log.Println("Starting server on :8080")
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
