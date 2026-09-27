package sfu

import (
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/pion/webrtc/v4"

	"pion-sfu/internal/config"
)

var (
	ErrRoomFull       = errors.New("room is full")
	ErrTooManyPresent = errors.New("presenter limit reached for this room")
	ErrShuttingDown   = errors.New("server is shutting down")
)

// Room holds participants and the publications being forwarded between them.
type Room struct {
	ID string

	mgr *Manager
	log *slog.Logger

	mu     sync.RWMutex
	peers  map[string]*Peer
	pubs   map[string]*Publication
	closed bool // set by the manager once removed; no more joins
}

func (r *Room) publications() []*Publication {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Publication, 0, len(r.pubs))
	for _, p := range r.pubs {
		out = append(out, p)
	}
	return out
}

func (r *Room) snapshotPeers() []*Peer {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Peer, 0, len(r.peers))
	for _, p := range r.peers {
		out = append(out, p)
	}
	return out
}

// State returns the presenter list and viewer count.
func (r *Room) State() RoomMessage {
	r.mu.RLock()
	defer r.mu.RUnlock()
	msg := RoomMessage{Type: MsgRoom, Presenters: []PresenterInfo{}}
	for _, p := range r.peers {
		if p.Role == RolePresenter {
			msg.Presenters = append(msg.Presenters, p.Info())
		} else {
			msg.Viewers++
		}
	}
	slices.SortFunc(msg.Presenters, func(a, b PresenterInfo) int { return strings.Compare(a.Name+a.ID, b.Name+b.ID) })
	return msg
}

// admit adds a peer if limits allow. Called with the manager lock held.
func (r *Room) admit(p *Peer, maxPresenters, maxViewers int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrShuttingDown
	}
	presenters, viewers := 0, 0
	for _, q := range r.peers {
		if q.Role == RolePresenter {
			presenters++
		} else {
			viewers++
		}
	}
	if p.Role == RolePresenter && maxPresenters > 0 && presenters >= maxPresenters {
		return ErrTooManyPresent
	}
	if p.Role == RoleViewer && maxViewers > 0 && viewers >= maxViewers {
		return ErrRoomFull
	}
	r.peers[p.ID] = p
	return nil
}

func (r *Room) leave(p *Peer) {
	r.mu.Lock()
	if _, ok := r.peers[p.ID]; !ok {
		r.mu.Unlock()
		return
	}
	delete(r.peers, p.ID)
	removed := 0
	for id, pub := range r.pubs {
		if pub.Publisher == p {
			delete(r.pubs, id)
			removed++
		}
	}
	empty := len(r.peers) == 0
	r.mu.Unlock()

	r.log.Info("peer left", "peer", p.ID, "name", p.Name, "role", p.Role, "publications_removed", removed)
	if empty {
		r.mgr.removeIfEmpty(r)
		return
	}
	r.broadcastState()
	if removed > 0 {
		r.resyncAll()
	}
}

func (r *Room) addPublication(pub *Publication) {
	r.mu.Lock()
	if _, ok := r.peers[pub.Publisher.ID]; !ok {
		r.mu.Unlock() // publisher already left
		return
	}
	r.pubs[pub.ID] = pub
	r.mu.Unlock()
	r.resyncAll()
}

func (r *Room) removePublication(id string) {
	r.mu.Lock()
	_, ok := r.pubs[id]
	delete(r.pubs, id)
	r.mu.Unlock()
	if ok {
		r.resyncAll()
	}
}

// resyncAll renegotiates every peer whose subscriptions may have changed.
func (r *Room) resyncAll() {
	for _, p := range r.snapshotPeers() {
		go p.Negotiate()
	}
}

func (r *Room) broadcastState() {
	state := r.State()
	for _, p := range r.snapshotPeers() {
		p.sig.Send(state)
	}
}

// Manager owns all rooms and the shared WebRTC API.
type Manager struct {
	cfg   *config.Config
	api   *webrtc.API
	pcCfg webrtc.Configuration
	log   *slog.Logger

	mu       sync.Mutex
	rooms    map[string]*Room
	shutdown bool
}

func NewManager(cfg *config.Config, api *webrtc.API, log *slog.Logger) *Manager {
	pcCfg := webrtc.Configuration{}
	if stun := cfg.ServerSTUNURLs(); len(stun) > 0 && len(cfg.NAT1To1IPs) == 0 {
		// Only useful to discover a server-reflexive address; with explicit
		// NAT 1:1 IPs the public address is already known.
		pcCfg.ICEServers = []webrtc.ICEServer{{URLs: stun}}
	}
	return &Manager{cfg: cfg, api: api, pcCfg: pcCfg, log: log, rooms: map[string]*Room{}}
}

// Join creates a peer in the given room (creating the room if needed) and
// kicks off the first negotiation. The welcome message is sent before the
// first offer.
func (m *Manager) Join(roomID, name string, role Role, sig Signaler) (*Peer, error) {
	id := uuid.NewString()

	// The PeerConnection is created under the manager lock so a concurrently
	// emptied room can't be removed between creation and admission.
	// NewPeerConnection does no network I/O, so this is cheap.
	m.mu.Lock()
	if m.shutdown {
		m.mu.Unlock()
		return nil, ErrShuttingDown
	}
	room, existed := m.rooms[roomID]
	if !existed {
		room = &Room{
			ID:    roomID,
			mgr:   m,
			log:   m.log.With("room", roomID),
			peers: map[string]*Peer{},
			pubs:  map[string]*Publication{},
		}
	}
	peer, err := newPeer(id, name, role, room, m.api, m.pcCfg, sig, m.cfg.NegotiationTimeout, room.log)
	if err == nil {
		if err = room.admit(peer, m.cfg.MaxPresenters, m.cfg.MaxViewers); err != nil {
			peer.discard()
		}
	}
	if err == nil && !existed {
		m.rooms[roomID] = room
		room.log.Info("room created")
	}
	m.mu.Unlock()
	if err != nil {
		return nil, err
	}

	room.log.Info("peer joined", "peer", id, "name", name, "role", role)
	sig.Send(WelcomeMessage{Type: MsgWelcome, PeerID: id, Room: roomID, Role: role, ICEServers: m.cfg.ICEServers})
	room.broadcastState()
	peer.started.Store(true)
	peer.Negotiate()
	return peer, nil
}

func (m *Manager) removeIfEmpty(r *Room) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.peers) == 0 && m.rooms[r.ID] == r {
		delete(m.rooms, r.ID)
		r.closed = true
		r.log.Info("room closed")
	}
}

// RoomSummary is the public view of a room for the HTTP API.
type RoomSummary struct {
	ID         string          `json:"id"`
	Presenters []PresenterInfo `json:"presenters"`
	Viewers    int             `json:"viewers"`
}

func (m *Manager) Rooms() []RoomSummary {
	m.mu.Lock()
	rooms := make([]*Room, 0, len(m.rooms))
	for _, r := range m.rooms {
		rooms = append(rooms, r)
	}
	m.mu.Unlock()

	out := make([]RoomSummary, 0, len(rooms))
	for _, r := range rooms {
		s := r.State()
		out = append(out, RoomSummary{ID: r.ID, Presenters: s.Presenters, Viewers: s.Viewers})
	}
	slices.SortFunc(out, func(a, b RoomSummary) int { return strings.Compare(a.ID, b.ID) })
	return out
}

// Shutdown rejects new joins and closes every peer.
func (m *Manager) Shutdown() {
	m.mu.Lock()
	m.shutdown = true
	rooms := make([]*Room, 0, len(m.rooms))
	for _, r := range m.rooms {
		rooms = append(rooms, r)
	}
	m.mu.Unlock()

	for _, r := range rooms {
		for _, p := range r.snapshotPeers() {
			p.Close()
		}
	}
}
