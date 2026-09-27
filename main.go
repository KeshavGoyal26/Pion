// Command pion-sfu runs a multi-presenter WebRTC SFU. Every participant uses
// a single PeerConnection over which all presenters' tracks are multiplexed.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"pion-sfu/internal/config"
	"pion-sfu/internal/server"
	"pion-sfu/internal/sfu"
	"pion-sfu/web"
)

func main() {
	cfg, err := config.Load(os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		slog.Error("config", "err", err)
		os.Exit(2)
	}

	var level slog.Level
	if err := level.UnmarshalText([]byte(cfg.LogLevel)); err != nil {
		slog.Error("invalid -log-level", "value", cfg.LogLevel)
		os.Exit(2)
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	if err := run(cfg, log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(cfg *config.Config, log *slog.Logger) error {
	api, closeAPI, err := sfu.NewAPI(cfg)
	if err != nil {
		return err
	}
	defer func() { _ = closeAPI() }()

	mgr := sfu.NewManager(cfg, api, log)
	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           server.New(cfg, mgr, log).Handler(web.Static()),
		ReadHeaderTimeout: 10 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		log.Info("listening",
			"addr", cfg.Addr, "tls", cfg.TLSCert != "",
			"udp_port", cfg.UDPPort, "nat1to1", cfg.NAT1To1IPs,
			"max_presenters", cfg.MaxPresenters, "max_viewers", cfg.MaxViewers)
		if cfg.TLSCert != "" {
			errCh <- srv.ListenAndServeTLS(cfg.TLSCert, cfg.TLSKey)
		} else {
			errCh <- srv.ListenAndServe()
		}
	}()

	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
		log.Info("shutting down")
	}

	// Hijacked WebSocket connections aren't tracked by Shutdown, so close
	// peers explicitly first.
	mgr.Shutdown()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}
