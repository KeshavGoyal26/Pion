package sfu

import (
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
)

// minKeyframeInterval rate-limits PLIs sent upstream to a presenter; many
// subscribers asking at once must not flood the publisher.
const minKeyframeInterval = 500 * time.Millisecond

// Publication is one track published by a presenter, fanned out to every
// subscriber through a single TrackLocalStaticRTP (one binding per subscriber PC).
type Publication struct {
	ID        string
	Publisher *Peer
	Local     *webrtc.TrackLocalStaticRTP

	remote  *webrtc.TrackRemote
	lastPLI atomic.Int64
	log     *slog.Logger
}

func newPublication(id string, publisher *Peer, remote *webrtc.TrackRemote) (*Publication, error) {
	// StreamID = presenter peer id, so a presenter's audio and video land in
	// the same MediaStream on the client.
	local, err := webrtc.NewTrackLocalStaticRTP(remote.Codec().RTPCodecCapability, id, publisher.ID)
	if err != nil {
		return nil, err
	}
	return &Publication{
		ID:        id,
		Publisher: publisher,
		Local:     local,
		remote:    remote,
		log:       publisher.log.With("pub", id, "kind", remote.Kind().String(), "codec", remote.Codec().MimeType),
	}, nil
}

func (p *Publication) Kind() webrtc.RTPCodecType { return p.remote.Kind() }

// RequestKeyframe asks the publisher for a new keyframe (video only).
func (p *Publication) RequestKeyframe() {
	if p.remote.Kind() != webrtc.RTPCodecTypeVideo {
		return
	}
	now := time.Now().UnixNano()
	last := p.lastPLI.Load()
	if now-last < int64(minKeyframeInterval) || !p.lastPLI.CompareAndSwap(last, now) {
		return
	}
	err := p.Publisher.pc.WriteRTCP([]rtcp.Packet{
		&rtcp.PictureLossIndication{MediaSSRC: uint32(p.remote.SSRC())},
	})
	if err != nil && !errors.Is(err, io.ErrClosedPipe) {
		p.log.Debug("send PLI", "err", err)
	}
}

// forward copies RTP from the presenter to all bound subscribers until the
// remote track ends.
func (p *Publication) forward() {
	buf := make([]byte, 1500)
	for {
		n, _, err := p.remote.Read(buf)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				p.log.Debug("remote track read ended", "err", err)
			}
			return
		}
		// A failed write to one subscriber must not stop forwarding to the
		// others; ErrClosedPipe just means there are no bindings right now.
		if _, err := p.Local.Write(buf[:n]); err != nil && !errors.Is(err, io.ErrClosedPipe) {
			p.log.Debug("write rtp", "err", err)
		}
	}
}
