package sfu

import (
	"github.com/pion/webrtc/v4"

	"pion-sfu/internal/config"
)

// Role of a participant in a room.
type Role string

const (
	RolePresenter Role = "presenter" // publishes audio/video, receives other presenters
	RoleViewer    Role = "viewer"    // receive-only
)

func (r Role) Valid() bool { return r == RolePresenter || r == RoleViewer }

// Signaling message types.
const (
	// client -> server
	MsgJoin      = "join"
	MsgAnswer    = "answer"
	MsgCandidate = "candidate" // both directions
	MsgLeave     = "leave"

	// server -> client
	MsgWelcome = "welcome"
	MsgOffer   = "offer"
	MsgRoom    = "room"
	MsgError   = "error"
)

// ClientMessage is any message received from a client.
type ClientMessage struct {
	Type      string                   `json:"type"`
	Room      string                   `json:"room,omitempty"`
	Name      string                   `json:"name,omitempty"`
	Role      Role                     `json:"role,omitempty"`
	SDP       string                   `json:"sdp,omitempty"`
	Candidate *webrtc.ICECandidateInit `json:"candidate,omitempty"`
}

type WelcomeMessage struct {
	Type       string             `json:"type"`
	PeerID     string             `json:"peerId"`
	Room       string             `json:"room"`
	Role       Role               `json:"role"`
	ICEServers []config.ICEServer `json:"iceServers"`
}

type SDPMessage struct {
	Type string `json:"type"`
	SDP  string `json:"sdp"`
}

type CandidateMessage struct {
	Type      string                  `json:"type"`
	Candidate webrtc.ICECandidateInit `json:"candidate"`
}

// PresenterInfo describes a presenter. Its ID equals the MediaStream id of
// every track forwarded from that presenter, so clients can group tracks.
type PresenterInfo struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type RoomMessage struct {
	Type       string          `json:"type"`
	Presenters []PresenterInfo `json:"presenters"`
	Viewers    int             `json:"viewers"`
}

type ErrorMessage struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

// Signaler delivers messages to a single client. Implementations must be
// safe for concurrent use and must not block.
type Signaler interface {
	Send(msg any)
	Close()
}
