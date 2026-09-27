package sfu

import (
	"fmt"

	"github.com/pion/ice/v4"
	"github.com/pion/interceptor"
	"github.com/pion/webrtc/v4"

	"pion-sfu/internal/config"
)

// NewAPI builds the shared webrtc.API (codecs, interceptors, ICE settings).
// The returned closer releases the UDP mux, if one was opened.
func NewAPI(cfg *config.Config) (*webrtc.API, func() error, error) {
	m := &webrtc.MediaEngine{}
	if err := m.RegisterDefaultCodecs(); err != nil {
		return nil, nil, fmt.Errorf("register codecs: %w", err)
	}

	// NACK, RTCP reports, TWCC feedback, etc.
	ir := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptors(m, ir); err != nil {
		return nil, nil, fmt.Errorf("register interceptors: %w", err)
	}

	se := webrtc.SettingEngine{}
	closer := func() error { return nil }

	if len(cfg.NAT1To1IPs) > 0 {
		err := se.SetICEAddressRewriteRules(webrtc.ICEAddressRewriteRule{
			External:        cfg.NAT1To1IPs,
			AsCandidateType: webrtc.ICECandidateTypeHost,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("nat1to1: %w", err)
		}
	}

	switch {
	case cfg.UDPPort > 0:
		mux, err := ice.NewMultiUDPMuxFromPort(cfg.UDPPort)
		if err != nil {
			return nil, nil, fmt.Errorf("udp mux on port %d: %w", cfg.UDPPort, err)
		}
		se.SetICEUDPMux(mux)
		closer = mux.Close
	case cfg.UDPPortMin > 0:
		if err := se.SetEphemeralUDPPortRange(cfg.UDPPortMin, cfg.UDPPortMax); err != nil {
			return nil, nil, fmt.Errorf("udp port range: %w", err)
		}
	}

	api := webrtc.NewAPI(
		webrtc.WithMediaEngine(m),
		webrtc.WithInterceptorRegistry(ir),
		webrtc.WithSettingEngine(se),
	)
	return api, closer, nil
}
