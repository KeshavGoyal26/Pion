package sfu

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
)

var errNotAwaitingAnswer = errors.New("unexpected answer: no offer outstanding")

// Peer is one participant with exactly one PeerConnection. The server is
// always the offerer, so there is never glare: subscription changes are
// batched while an offer is outstanding and renegotiated once it is answered.
//
// Presenters: two recvonly transceivers (audio+video) for publishing, plus a
// sendonly transceiver per subscribed track of every other presenter.
// Viewers: only the sendonly subscription transceivers.
type Peer struct {
	ID   string
	Name string
	Role Role

	room *Room
	pc   *webrtc.PeerConnection
	sig  Signaler
	log  *slog.Logger

	negotiationTimeout time.Duration

	// Negotiation state, guarded by negMu.
	negMu          sync.Mutex
	negotiatedOnce bool
	awaitingAnswer bool
	pending        bool   // resync needed once the current answer arrives
	offerGen       uint64 // identifies the outstanding offer for its timeout
	answerTimer    *time.Timer
	subs           map[string]*webrtc.RTPSender // publication id -> sender
	justAdded      []*Publication               // subscriptions in the outstanding offer

	candMu            sync.Mutex
	pendingCandidates []webrtc.ICECandidateInit

	trackSeq atomic.Uint32
	started  atomic.Bool // set once the client has been welcomed; gates offers
	closed   atomic.Bool
}

func newPeer(id, name string, role Role, room *Room, api *webrtc.API, pcCfg webrtc.Configuration,
	sig Signaler, negotiationTimeout time.Duration, log *slog.Logger,
) (*Peer, error) {
	pc, err := api.NewPeerConnection(pcCfg)
	if err != nil {
		return nil, fmt.Errorf("new peer connection: %w", err)
	}
	p := &Peer{
		ID:                 id,
		Name:               name,
		Role:               role,
		room:               room,
		pc:                 pc,
		sig:                sig,
		log:                log.With("peer", id, "role", role),
		negotiationTimeout: negotiationTimeout,
		subs:               map[string]*webrtc.RTPSender{},
	}

	// Guarantees the SDP always has at least one m-line, so ICE/DTLS come up
	// immediately even for a viewer joining an empty room.
	if _, err := pc.CreateDataChannel("sfu", nil); err != nil {
		_ = pc.Close()
		return nil, fmt.Errorf("create data channel: %w", err)
	}

	if role == RolePresenter {
		for _, kind := range []webrtc.RTPCodecType{webrtc.RTPCodecTypeAudio, webrtc.RTPCodecTypeVideo} {
			_, err := pc.AddTransceiverFromKind(kind, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly})
			if err != nil {
				_ = pc.Close()
				return nil, fmt.Errorf("add %s transceiver: %w", kind, err)
			}
		}
		pc.OnTrack(p.onTrack)
	}

	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		p.sig.Send(CandidateMessage{Type: MsgCandidate, Candidate: c.ToJSON()})
	})

	pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		p.log.Info("connection state", "state", s.String())
		switch s {
		case webrtc.PeerConnectionStateFailed, webrtc.PeerConnectionStateClosed:
			go p.Close()
		}
	})

	return p, nil
}

func (p *Peer) Info() PresenterInfo { return PresenterInfo{ID: p.ID, Name: p.Name} }

// onTrack runs in its own goroutine per incoming presenter track and blocks
// for the track's lifetime.
func (p *Peer) onTrack(remote *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
	id := fmt.Sprintf("%s-%s-%d", p.ID, remote.Kind(), p.trackSeq.Add(1))
	pub, err := newPublication(id, p, remote)
	if err != nil {
		p.log.Error("create publication", "err", err)
		return
	}
	if p.closed.Load() {
		return
	}
	pub.log.Info("publishing")
	p.room.addPublication(pub)
	pub.forward()
	pub.log.Info("unpublished")
	p.room.removePublication(pub.ID)
}

// Negotiate brings this peer's subscriptions in line with the room's
// publications and sends an offer if anything changed. Safe to call from any
// goroutine at any time.
func (p *Peer) Negotiate() {
	p.negMu.Lock()
	defer p.negMu.Unlock()

	// Before start, the initial Negotiate (after the welcome) will pick up
	// the latest room state anyway.
	if p.closed.Load() || !p.started.Load() {
		return
	}
	if p.awaitingAnswer {
		p.pending = true
		return
	}

	added, changed, err := p.syncSubscriptions()
	if err != nil {
		p.log.Error("sync subscriptions", "err", err)
	}
	if !changed && p.negotiatedOnce {
		return
	}

	offer, err := p.pc.CreateOffer(nil)
	if err != nil {
		p.fail("create offer", err)
		return
	}
	if err := p.pc.SetLocalDescription(offer); err != nil {
		p.fail("set local description", err)
		return
	}

	p.negotiatedOnce = true
	p.awaitingAnswer = true
	p.justAdded = added
	p.offerGen++
	gen := p.offerGen
	p.answerTimer = time.AfterFunc(p.negotiationTimeout, func() { p.onAnswerTimeout(gen) })

	p.sig.Send(SDPMessage{Type: MsgOffer, SDP: p.pc.LocalDescription().SDP})
}

// syncSubscriptions must be called with negMu held.
func (p *Peer) syncSubscriptions() (added []*Publication, changed bool, err error) {
	want := map[string]*Publication{}
	for _, pub := range p.room.publications() {
		if pub.Publisher != p {
			want[pub.ID] = pub
		}
	}

	var errs []error
	for id, sender := range p.subs {
		if _, ok := want[id]; ok {
			continue
		}
		if err := p.pc.RemoveTrack(sender); err != nil {
			errs = append(errs, fmt.Errorf("remove %s: %w", id, err))
		}
		delete(p.subs, id)
		changed = true
	}

	for id, pub := range want {
		if _, ok := p.subs[id]; ok {
			continue
		}
		// Deliberately not pc.AddTrack: it may recycle a presenter's recvonly
		// publish transceiver into sendrecv, mixing publish and subscribe on
		// one m-line. A dedicated sendonly transceiver keeps them separate.
		tr, err := p.pc.AddTransceiverFromTrack(pub.Local, webrtc.RTPTransceiverInit{
			Direction: webrtc.RTPTransceiverDirectionSendonly,
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("add %s: %w", id, err))
			continue
		}
		sender := tr.Sender()
		p.subs[id] = sender
		added = append(added, pub)
		changed = true
		go p.readSenderRTCP(sender, pub)
	}
	return added, changed, errors.Join(errs...)
}

// readSenderRTCP drains RTCP from a subscriber (required for the NACK
// interceptor) and relays keyframe requests to the publisher. It exits when
// the sender is stopped.
func (p *Peer) readSenderRTCP(sender *webrtc.RTPSender, pub *Publication) {
	for {
		pkts, _, err := sender.ReadRTCP()
		if err != nil {
			return
		}
		for _, pkt := range pkts {
			switch pkt.(type) {
			case *rtcp.PictureLossIndication, *rtcp.FullIntraRequest:
				pub.RequestKeyframe()
			}
		}
	}
}

// HandleAnswer applies the client's answer to the outstanding offer.
func (p *Peer) HandleAnswer(sdp string) error {
	p.negMu.Lock()
	if !p.awaitingAnswer {
		p.negMu.Unlock()
		return errNotAwaitingAnswer
	}
	if p.answerTimer != nil {
		p.answerTimer.Stop()
	}
	err := p.pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: sdp})
	p.awaitingAnswer = false
	added := p.justAdded
	p.justAdded = nil
	pending := p.pending
	p.pending = false
	p.negMu.Unlock()

	if err != nil {
		p.fail("set remote description", err)
		return err
	}

	p.flushCandidates()

	// New subscriber: it can't decode until the next keyframe.
	for _, pub := range added {
		pub.RequestKeyframe()
	}
	if pending {
		p.Negotiate()
	}
	return nil
}

// HandleCandidate adds a remote ICE candidate, buffering it until the first
// answer has been applied.
func (p *Peer) HandleCandidate(c webrtc.ICECandidateInit) error {
	p.candMu.Lock()
	if p.pc.RemoteDescription() == nil {
		p.pendingCandidates = append(p.pendingCandidates, c)
		p.candMu.Unlock()
		return nil
	}
	p.candMu.Unlock()
	return p.pc.AddICECandidate(c)
}

func (p *Peer) flushCandidates() {
	p.candMu.Lock()
	cands := p.pendingCandidates
	p.pendingCandidates = nil
	p.candMu.Unlock()
	for _, c := range cands {
		if err := p.pc.AddICECandidate(c); err != nil {
			p.log.Debug("add buffered candidate", "err", err)
		}
	}
}

func (p *Peer) onAnswerTimeout(gen uint64) {
	p.negMu.Lock()
	timedOut := p.awaitingAnswer && p.offerGen == gen
	p.negMu.Unlock()
	if timedOut {
		p.log.Warn("no answer to offer, disconnecting", "timeout", p.negotiationTimeout)
		p.sig.Send(ErrorMessage{Type: MsgError, Message: "negotiation timeout"})
		p.Close()
	}
}

func (p *Peer) fail(op string, err error) {
	p.log.Error(op, "err", err)
	p.sig.Send(ErrorMessage{Type: MsgError, Message: op + " failed"})
	go p.Close()
}

// discard releases a peer that was never admitted to its room, without
// touching the room or the client's signaling channel.
func (p *Peer) discard() {
	p.closed.Store(true) // keeps the PC's Closed state callback from calling Close
	_ = p.pc.Close()
}

// Close removes the peer from its room and tears everything down. Idempotent.
func (p *Peer) Close() {
	if !p.closed.CompareAndSwap(false, true) {
		return
	}
	p.negMu.Lock()
	if p.answerTimer != nil {
		p.answerTimer.Stop()
	}
	p.negMu.Unlock()

	p.room.leave(p)
	if err := p.pc.Close(); err != nil {
		p.log.Debug("close peer connection", "err", err)
	}
	p.sig.Close()
	p.log.Info("peer closed")
}
