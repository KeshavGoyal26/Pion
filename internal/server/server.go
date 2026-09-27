// Package server exposes the SFU over HTTP: WebSocket signaling, a small
// JSON API and the embedded demo client.
package server

import (
	"encoding/json"
	"io/fs"
	"log/slog"
	"net/http"

	"github.com/gorilla/websocket"

	"pion-sfu/internal/config"
	"pion-sfu/internal/sfu"
)

type Server struct {
	cfg      *config.Config
	mgr      *sfu.Manager
	log      *slog.Logger
	upgrader *websocket.Upgrader
}

func New(cfg *config.Config, mgr *sfu.Manager, log *slog.Logger) *Server {
	s := &Server{cfg: cfg, mgr: mgr, log: log}
	s.upgrader = s.newUpgrader()
	return s
}

// Handler returns the HTTP routes. static is served at "/".
func (s *Server) Handler(static fs.FS) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ws", s.handleWS)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /api/rooms", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(s.mgr.Rooms())
	})
	mux.Handle("GET /", http.FileServerFS(static))
	return mux
}
