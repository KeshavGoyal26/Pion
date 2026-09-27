package server

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/gorilla/websocket"

	"pion-sfu/internal/sfu"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = 25 * time.Second
	joinWait       = 10 * time.Second
	maxMessageSize = 1 << 20 // SDP grows with the number of forwarded tracks
	sendQueueSize  = 256
	maxNameLen     = 64
)

var roomIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// wsClient is the Signaler for one WebSocket connection. Sends are queued
// and written by a single goroutine; a client that can't keep up is dropped
// rather than blocking the SFU.
type wsClient struct {
	conn *websocket.Conn
	log  *slog.Logger

	mu     sync.Mutex
	send   chan []byte
	closed bool
	done   chan struct{}
}

func newWSClient(conn *websocket.Conn, log *slog.Logger) *wsClient {
	return &wsClient{conn: conn, log: log, send: make(chan []byte, sendQueueSize), done: make(chan struct{})}
}

func (c *wsClient) Send(msg any) {
	b, err := json.Marshal(msg)
	if err != nil {
		c.log.Error("marshal message", "err", err)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	select {
	case c.send <- b:
	default:
		c.log.Warn("send queue full, dropping client")
		c.closeLocked()
	}
}

func (c *wsClient) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closeLocked()
}

func (c *wsClient) closeLocked() {
	if !c.closed {
		c.closed = true
		close(c.send) // writer flushes queued messages, then closes the conn
	}
}

func (c *wsClient) writeLoop() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		_ = c.conn.Close()
		close(c.done)
	}()
	for {
		select {
		case b, ok := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				_ = c.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, b); err != nil {
				return
			}
		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (s *Server) newUpgrader() *websocket.Upgrader {
	u := &websocket.Upgrader{ReadBufferSize: 4096, WriteBufferSize: 4096}
	switch {
	case len(s.cfg.AllowedOrigins) == 0:
		// gorilla's default: same-origin only.
	case len(s.cfg.AllowedOrigins) == 1 && s.cfg.AllowedOrigins[0] == "*":
		u.CheckOrigin = func(*http.Request) bool { return true }
	default:
		allowed := map[string]bool{}
		for _, o := range s.cfg.AllowedOrigins {
			allowed[strings.TrimSuffix(o, "/")] = true
		}
		u.CheckOrigin = func(r *http.Request) bool { return allowed[r.Header.Get("Origin")] }
	}
	return u
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.log.Debug("websocket upgrade", "err", err, "remote", r.RemoteAddr)
		return // Upgrade already wrote the HTTP error
	}
	conn.SetReadLimit(maxMessageSize)

	log := s.log.With("remote", r.RemoteAddr)
	client := newWSClient(conn, log)
	go client.writeLoop()
	defer func() {
		client.Close()
		<-client.done
	}()

	peer, err := s.join(conn, client)
	if err != nil {
		log.Info("join rejected", "err", err)
		client.Send(sfu.ErrorMessage{Type: sfu.MsgError, Message: err.Error()})
		return
	}
	defer peer.Close()

	_ = conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error { return conn.SetReadDeadline(time.Now().Add(pongWait)) })

	for {
		var msg sfu.ClientMessage
		if err := conn.ReadJSON(&msg); err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				log.Debug("websocket read", "err", err)
			}
			return
		}
		switch msg.Type {
		case sfu.MsgAnswer:
			if err := peer.HandleAnswer(msg.SDP); err != nil {
				log.Warn("handle answer", "peer", peer.ID, "err", err)
			}
		case sfu.MsgCandidate:
			if msg.Candidate == nil || msg.Candidate.Candidate == "" {
				continue // end-of-candidates
			}
			if err := peer.HandleCandidate(*msg.Candidate); err != nil {
				log.Debug("add candidate", "peer", peer.ID, "err", err)
			}
		case sfu.MsgLeave:
			return
		default:
			client.Send(sfu.ErrorMessage{Type: sfu.MsgError, Message: "unknown message type: " + msg.Type})
		}
	}
}

// join reads and validates the first message, which must be a join.
func (s *Server) join(conn *websocket.Conn, client *wsClient) (*sfu.Peer, error) {
	_ = conn.SetReadDeadline(time.Now().Add(joinWait))
	var msg sfu.ClientMessage
	if err := conn.ReadJSON(&msg); err != nil {
		return nil, errors.New("expected join message")
	}
	if msg.Type != sfu.MsgJoin {
		return nil, errors.New("first message must be join")
	}
	if !roomIDPattern.MatchString(msg.Room) {
		return nil, errors.New("invalid room id: use 1-64 of [A-Za-z0-9_-]")
	}
	if !msg.Role.Valid() {
		return nil, errors.New(`role must be "presenter" or "viewer"`)
	}
	name := strings.TrimSpace(msg.Name)
	if !utf8.ValidString(name) {
		return nil, errors.New("invalid name")
	}
	if name == "" {
		name = "anonymous"
	}
	if utf8.RuneCountInString(name) > maxNameLen {
		name = string([]rune(name)[:maxNameLen])
	}
	return s.mgr.Join(msg.Room, name, msg.Role, client)
}
