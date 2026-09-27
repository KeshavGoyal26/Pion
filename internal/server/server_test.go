package server

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"

	"pion-sfu/internal/config"
	"pion-sfu/internal/sfu"
)

// testClient is a minimal Pion-based client speaking the signaling protocol.
type testClient struct {
	t      *testing.T
	name   string
	role   sfu.Role
	ws     *websocket.Conn
	wsMu   sync.Mutex
	pc     *webrtc.PeerConnection
	peerID chan string
	rooms  chan sfu.RoomMessage
	tracks chan *webrtc.TrackRemote
	ended  chan string // stream id of a remote track whose reads ended
	stop   chan struct{}
	done   chan struct{} // closed when readLoop exits
}

func newTestServer(t *testing.T, cfg *config.Config) *httptest.Server {
	t.Helper()
	api, closer, err := sfu.NewAPI(cfg)
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := sfu.NewManager(cfg, api, log)
	ts := httptest.NewServer(New(cfg, mgr, log).Handler(fstest.MapFS{}))
	t.Cleanup(func() {
		mgr.Shutdown()
		ts.Close()
		_ = closer()
	})
	return ts
}

func dial(t *testing.T, ts *httptest.Server, room, name string, role sfu.Role) *testClient {
	t.Helper()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(ts.URL, "http")+"/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	c := &testClient{
		t: t, name: name, role: role, ws: ws,
		peerID: make(chan string, 1),
		rooms:  make(chan sfu.RoomMessage, 64),
		tracks: make(chan *webrtc.TrackRemote, 16),
		ended:  make(chan string, 16),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	c.send(sfu.ClientMessage{Type: sfu.MsgJoin, Room: room, Name: name, Role: role})
	go c.readLoop()
	t.Cleanup(c.close)
	return c
}

func (c *testClient) send(v any) {
	c.wsMu.Lock()
	defer c.wsMu.Unlock()
	_ = c.ws.WriteJSON(v)
}

func (c *testClient) close() {
	select {
	case <-c.stop:
		return
	default:
	}
	close(c.stop)
	_ = c.ws.Close()
	<-c.done
	if c.pc != nil {
		_ = c.pc.Close()
	}
}

// errorf reports a client failure unless it was caused by closing the client.
func (c *testClient) errorf(format string, args ...any) {
	select {
	case <-c.stop:
	default:
		c.t.Errorf(format, args...)
	}
}

func (c *testClient) readLoop() {
	defer close(c.done)
	var pending []webrtc.ICECandidateInit
	for {
		_, data, err := c.ws.ReadMessage()
		if err != nil {
			return
		}
		var m struct {
			Type string `json:"type"`
			sfu.WelcomeMessage
			SDP       string                   `json:"sdp"`
			Candidate *webrtc.ICECandidateInit `json:"candidate"`
			Message   string                   `json:"message"`
		}
		if err := json.Unmarshal(data, &m); err != nil {
			c.errorf("%s: bad message: %v", c.name, err)
			return
		}
		switch m.Type {
		case sfu.MsgWelcome:
			c.setupPC()
			c.peerID <- m.PeerID
		case sfu.MsgOffer:
			if err := c.pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: m.SDP}); err != nil {
				c.errorf("%s: set offer: %v", c.name, err)
				return
			}
			for _, cand := range pending {
				_ = c.pc.AddICECandidate(cand)
			}
			pending = nil
			answer, err := c.pc.CreateAnswer(nil)
			if err != nil {
				c.errorf("%s: create answer: %v", c.name, err)
				return
			}
			if err := c.pc.SetLocalDescription(answer); err != nil {
				c.errorf("%s: set answer: %v", c.name, err)
				return
			}
			c.send(sfu.SDPMessage{Type: sfu.MsgAnswer, SDP: c.pc.LocalDescription().SDP})
		case sfu.MsgCandidate:
			if c.pc.RemoteDescription() == nil {
				pending = append(pending, *m.Candidate)
			} else {
				_ = c.pc.AddICECandidate(*m.Candidate)
			}
		case sfu.MsgRoom:
			var rm sfu.RoomMessage
			_ = json.Unmarshal(data, &rm)
			c.rooms <- rm
		case sfu.MsgError:
			c.t.Logf("%s: server error: %s", c.name, m.Message)
		}
	}
}

func (c *testClient) setupPC() {
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		c.t.Fatal(err)
	}
	c.pc = pc
	pc.OnICECandidate(func(cand *webrtc.ICECandidate) {
		if cand != nil {
			c.send(sfu.CandidateMessage{Type: sfu.MsgCandidate, Candidate: cand.ToJSON()})
		}
	})
	pc.OnTrack(func(tr *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		c.tracks <- tr
		for {
			if _, _, err := tr.ReadRTP(); err != nil {
				c.ended <- tr.StreamID()
				return
			}
		}
	})

	if c.role != sfu.RolePresenter {
		return
	}
	for _, codec := range []webrtc.RTPCodecCapability{
		{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2},
		{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000},
	} {
		track, err := webrtc.NewTrackLocalStaticRTP(codec, "t-"+codec.MimeType, "local-"+c.name)
		if err != nil {
			c.t.Fatal(err)
		}
		if _, err := pc.AddTrack(track); err != nil {
			c.t.Fatal(err)
		}
		go func() { // dummy media: the SFU forwards without parsing payloads
			tick := time.NewTicker(20 * time.Millisecond)
			defer tick.Stop()
			var seq uint16
			for {
				select {
				case <-c.stop:
					return
				case <-tick.C:
					seq++
					_ = track.WriteRTP(&rtp.Packet{
						Header:  rtp.Header{Version: 2, SequenceNumber: seq, Timestamp: uint32(seq) * 960},
						Payload: []byte{0x10, 0x00, 0x00, 0x00},
					})
				}
			}
		}()
	}
}

func (c *testClient) id() string {
	select {
	case id := <-c.peerID:
		c.peerID <- id
		return id
	case <-time.After(5 * time.Second):
		c.t.Fatalf("%s: no welcome", c.name)
		return ""
	}
}

// waitTracks collects n remote tracks and returns counts per stream id.
func (c *testClient) waitTracks(n int) map[string]int {
	c.t.Helper()
	got := map[string]int{}
	timeout := time.After(15 * time.Second)
	for i := 0; i < n; i++ {
		select {
		case tr := <-c.tracks:
			got[tr.StreamID()]++
		case <-timeout:
			c.t.Fatalf("%s: got %d/%d tracks: %v", c.name, i, n, got)
		}
	}
	return got
}

func TestMultiPresenterSingleConnection(t *testing.T) {
	testMultiPresenter(t, &config.Config{NegotiationTimeout: 10 * time.Second})
}

func TestMultiPresenterSingleUDPPort(t *testing.T) {
	testMultiPresenter(t, &config.Config{NegotiationTimeout: 10 * time.Second, UDPPort: 47811})
}

func testMultiPresenter(t *testing.T, cfg *config.Config) {
	ts := newTestServer(t, cfg)

	p1 := dial(t, ts, "r1", "alice", sfu.RolePresenter)
	p2 := dial(t, ts, "r1", "bob", sfu.RolePresenter)
	v := dial(t, ts, "r1", "viewer", sfu.RoleViewer)
	id1, id2 := p1.id(), p2.id()

	// Viewer: audio+video from both presenters over its one PeerConnection.
	got := v.waitTracks(4)
	if got[id1] != 2 || got[id2] != 2 || len(got) != 2 {
		t.Fatalf("viewer tracks by stream = %v, want 2 each from %s and %s", got, id1, id2)
	}

	// Presenters see each other but never themselves.
	if got := p1.waitTracks(2); got[id2] != 2 || len(got) != 1 {
		t.Fatalf("alice got %v, want 2 tracks from bob", got)
	}
	if got := p2.waitTracks(2); got[id1] != 2 || len(got) != 1 {
		t.Fatalf("bob got %v, want 2 tracks from alice", got)
	}

	// Bob leaves: the viewer is renegotiated and bob's tracks end.
	p2.close()
	ended := map[string]int{}
	timeout := time.After(15 * time.Second)
	for ended[id2] < 2 {
		select {
		case s := <-v.ended:
			ended[s]++
		case <-timeout:
			t.Fatalf("viewer: bob's tracks did not end, ended=%v", ended)
		}
	}
	if ended[id1] != 0 {
		t.Fatalf("alice's tracks ended too: %v", ended)
	}
	waitRoom(t, v, func(m sfu.RoomMessage) bool { return len(m.Presenters) == 1 && m.Presenters[0].ID == id1 })

	// Carol joins later and the viewer picks her up on the same PC.
	p3 := dial(t, ts, "r1", "carol", sfu.RolePresenter)
	id3 := p3.id()
	if got := v.waitTracks(2); got[id3] != 2 {
		t.Fatalf("viewer after carol joined got %v", got)
	}
}

func TestPresenterLimit(t *testing.T) {
	cfg := &config.Config{NegotiationTimeout: 10 * time.Second, MaxPresenters: 1}
	ts := newTestServer(t, cfg)
	p1 := dial(t, ts, "r2", "alice", sfu.RolePresenter)
	p1.id()

	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(ts.URL, "http")+"/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	_ = ws.WriteJSON(sfu.ClientMessage{Type: sfu.MsgJoin, Room: "r2", Name: "bob", Role: sfu.RolePresenter})
	var m sfu.ErrorMessage
	if err := ws.ReadJSON(&m); err != nil {
		t.Fatal(err)
	}
	if m.Type != sfu.MsgError || m.Message != sfu.ErrTooManyPresent.Error() {
		t.Fatalf("got %+v, want presenter limit error", m)
	}
}

func waitRoom(t *testing.T, c *testClient, ok func(sfu.RoomMessage) bool) {
	t.Helper()
	timeout := time.After(5 * time.Second)
	for {
		select {
		case m := <-c.rooms:
			if ok(m) {
				return
			}
		case <-timeout:
			t.Fatalf("%s: room state never matched", c.name)
		}
	}
}
