// Package config loads server configuration from flags, falling back to
// environment variables (flag > env > default).
package config

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// ICEServer is an ICE server as handed to browsers (RTCIceServer shape).
type ICEServer struct {
	URLs       []string `json:"urls"`
	Username   string   `json:"username,omitempty"`
	Credential string   `json:"credential,omitempty"`
}

type Config struct {
	Addr    string
	TLSCert string
	TLSKey  string

	// AllowedOrigins for the WebSocket upgrade. Empty = same-origin only, "*" = any.
	AllowedOrigins []string

	// ICEServers are sent to clients. The server's own PeerConnections only use
	// the STUN entries (see ServerSTUNURLs) - it never needs a TURN relay itself.
	ICEServers []ICEServer

	// NAT1To1IPs are public IPs advertised as host candidates instead of the
	// local interface addresses (server behind 1:1 NAT, e.g. a cloud VM).
	NAT1To1IPs []string

	// UDPPort > 0 multiplexes all ICE traffic over this single UDP port.
	UDPPort int
	// UDPPortMin/Max restrict the ephemeral port range (ignored when UDPPort > 0).
	UDPPortMin uint16
	UDPPortMax uint16

	MaxPresenters int // per room, 0 = unlimited
	MaxViewers    int // per room, 0 = unlimited

	// NegotiationTimeout is how long a client has to answer an offer before
	// it is disconnected.
	NegotiationTimeout time.Duration

	LogLevel string
}

// ServerSTUNURLs returns only the stun:/stuns: URLs from ICEServers.
func (c *Config) ServerSTUNURLs() []string {
	var out []string
	for _, s := range c.ICEServers {
		for _, u := range s.URLs {
			if strings.HasPrefix(u, "stun:") || strings.HasPrefix(u, "stuns:") {
				out = append(out, u)
			}
		}
	}
	return out
}

func Load(args []string) (*Config, error) {
	fs := flag.NewFlagSet("pion-sfu", flag.ContinueOnError)
	var (
		cfg            Config
		origins        string
		iceURLs        string
		turnUser       string
		turnCredential string
		natIPs         string
		portMin        int
		portMax        int
	)

	fs.StringVar(&cfg.Addr, "addr", env("SFU_ADDR", ":8080"), "HTTP listen address [SFU_ADDR]")
	fs.StringVar(&cfg.TLSCert, "tls-cert", env("SFU_TLS_CERT", ""), "TLS certificate file; enables HTTPS [SFU_TLS_CERT]")
	fs.StringVar(&cfg.TLSKey, "tls-key", env("SFU_TLS_KEY", ""), "TLS key file [SFU_TLS_KEY]")
	fs.StringVar(&origins, "allowed-origins", env("SFU_ALLOWED_ORIGINS", ""), "comma-separated WebSocket origins; empty = same-origin, * = any [SFU_ALLOWED_ORIGINS]")
	fs.StringVar(&iceURLs, "ice-urls", env("SFU_ICE_URLS", "stun:stun.l.google.com:19302"), "comma-separated STUN/TURN URLs [SFU_ICE_URLS]")
	fs.StringVar(&turnUser, "turn-username", env("SFU_TURN_USERNAME", ""), "username for turn:/turns: URLs [SFU_TURN_USERNAME]")
	fs.StringVar(&turnCredential, "turn-credential", env("SFU_TURN_CREDENTIAL", ""), "credential for turn:/turns: URLs [SFU_TURN_CREDENTIAL]")
	fs.StringVar(&natIPs, "nat1to1-ips", env("SFU_NAT1TO1_IPS", ""), "comma-separated public IPs to advertise as host candidates [SFU_NAT1TO1_IPS]")
	fs.IntVar(&cfg.UDPPort, "udp-port", envInt("SFU_UDP_PORT", 0), "single UDP port for all ICE traffic, 0 = disabled [SFU_UDP_PORT]")
	fs.IntVar(&portMin, "udp-port-min", envInt("SFU_UDP_PORT_MIN", 0), "min ephemeral UDP port [SFU_UDP_PORT_MIN]")
	fs.IntVar(&portMax, "udp-port-max", envInt("SFU_UDP_PORT_MAX", 0), "max ephemeral UDP port [SFU_UDP_PORT_MAX]")
	fs.IntVar(&cfg.MaxPresenters, "max-presenters", envInt("SFU_MAX_PRESENTERS", 0), "max presenters per room, 0 = unlimited [SFU_MAX_PRESENTERS]")
	fs.IntVar(&cfg.MaxViewers, "max-viewers", envInt("SFU_MAX_VIEWERS", 0), "max viewers per room, 0 = unlimited [SFU_MAX_VIEWERS]")
	fs.DurationVar(&cfg.NegotiationTimeout, "negotiation-timeout", envDuration("SFU_NEGOTIATION_TIMEOUT", 15*time.Second), "max time for a client to answer an offer [SFU_NEGOTIATION_TIMEOUT]")
	fs.StringVar(&cfg.LogLevel, "log-level", env("SFU_LOG_LEVEL", "info"), "debug|info|warn|error [SFU_LOG_LEVEL]")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	cfg.AllowedOrigins = splitList(origins)
	cfg.NAT1To1IPs = splitList(natIPs)

	var stun, turn []string
	for _, u := range splitList(iceURLs) {
		switch {
		case strings.HasPrefix(u, "stun:"), strings.HasPrefix(u, "stuns:"):
			stun = append(stun, u)
		case strings.HasPrefix(u, "turn:"), strings.HasPrefix(u, "turns:"):
			turn = append(turn, u)
		default:
			return nil, fmt.Errorf("invalid ICE URL %q: must start with stun:, stuns:, turn: or turns:", u)
		}
	}
	if len(stun) > 0 {
		cfg.ICEServers = append(cfg.ICEServers, ICEServer{URLs: stun})
	}
	if len(turn) > 0 {
		if turnUser == "" || turnCredential == "" {
			return nil, errors.New("turn URLs require -turn-username and -turn-credential")
		}
		cfg.ICEServers = append(cfg.ICEServers, ICEServer{URLs: turn, Username: turnUser, Credential: turnCredential})
	}

	if (cfg.TLSCert == "") != (cfg.TLSKey == "") {
		return nil, errors.New("-tls-cert and -tls-key must be set together")
	}
	if cfg.UDPPort < 0 || cfg.UDPPort > 65535 {
		return nil, fmt.Errorf("invalid -udp-port %d", cfg.UDPPort)
	}
	if portMin != 0 || portMax != 0 {
		if portMin <= 0 || portMax > 65535 || portMin > portMax {
			return nil, fmt.Errorf("invalid UDP port range %d-%d", portMin, portMax)
		}
		cfg.UDPPortMin, cfg.UDPPortMax = uint16(portMin), uint16(portMax)
	}
	if cfg.NegotiationTimeout <= 0 {
		return nil, errors.New("-negotiation-timeout must be positive")
	}
	return &cfg, nil
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(key); ok {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
