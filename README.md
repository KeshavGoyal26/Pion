# Pion SFU

A multi-presenter WebRTC SFU written in Go on [Pion](https://github.com/pion/webrtc) v4.

- **Any number of presenters per room.** Each one publishes audio and video.
- **One PeerConnection per participant.** Every presenter's tracks are multiplexed onto the same connection. Tracks are added and removed through server-driven renegotiation.
  - **Viewers** receive only.
  - **Presenters** publish and also receive every *other* presenter.
- **Single-layer forwarding.** RTP is fanned out without transcoding. PLI/FIR from subscribers is relayed to the publisher (rate-limited). A keyframe is also requested for each new subscriber.
- **ICE config:** STUN/TURN for clients, NAT 1:1 public IPs, single-port UDP mux or an ephemeral port range.
- **Demo browser client** is embedded and served at `/`.

## Run

```sh
go run .                       # http://localhost:8080
go test -race ./...            # end-to-end tests with real Pion clients
go build -o bin/pion-sfu .
```

Open `http://localhost:8080` in a few tabs. Join the same room as presenters or viewers.
`?room=demo&role=viewer` pre-fills the form.

Browsers only allow camera access on `https://` or `localhost`. For LAN or remote testing, pass `-tls-cert` / `-tls-key`.

## Configuration

Every option can be set as a flag or as an environment variable. Flags take precedence.

| Flag | Env | Default | |
|---|---|---|---|
| `-addr` | `SFU_ADDR` | `:8080` | HTTP listen address |
| `-tls-cert`, `-tls-key` | `SFU_TLS_CERT`, `SFU_TLS_KEY` | | Enable HTTPS |
| `-allowed-origins` | `SFU_ALLOWED_ORIGINS` | same-origin | Comma list of WebSocket origins, or `*` |
| `-ice-urls` | `SFU_ICE_URLS` | `stun:stun.l.google.com:19302` | Comma list of `stun:` / `turn:` / `turns:` URLs |
| `-turn-username`, `-turn-credential` | `SFU_TURN_USERNAME`, `SFU_TURN_CREDENTIAL` | | Credentials for the TURN URLs |
| `-nat1to1-ips` | `SFU_NAT1TO1_IPS` | | Public IPs to advertise as host candidates |
| `-udp-port` | `SFU_UDP_PORT` | `0` | Carry all ICE traffic on this one UDP port |
| `-udp-port-min`, `-udp-port-max` | `SFU_UDP_PORT_MIN`, `SFU_UDP_PORT_MAX` | | Ephemeral port range (ignored when `-udp-port` is set) |
| `-max-presenters` | `SFU_MAX_PRESENTERS` | `0` (unlimited) | Maximum presenters per room |
| `-max-viewers` | `SFU_MAX_VIEWERS` | `0` (unlimited) | Maximum viewers per room |
| `-negotiation-timeout` | `SFU_NEGOTIATION_TIMEOUT` | `15s` | How long a client has to answer an offer before it is dropped |
| `-log-level` | `SFU_LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error` |

The client receives all ICE servers. The server's own PeerConnections use only the STUN URLs, and only when `-nat1to1-ips` is not set, because the server never needs a TURN relay itself.

Typical cloud VM setup:

```sh
./pion-sfu -addr :443 -tls-cert cert.pem -tls-key key.pem \
  -nat1to1-ips 203.0.113.10 -udp-port 3478 \
  -ice-urls stun:stun.l.google.com:19302,turn:turn.example.com:3478 \
  -turn-username user -turn-credential pass
```

Open TCP 443 and UDP 3478, or whichever `-udp-port` you choose.

## HTTP endpoints

- `GET /ws` — WebSocket signaling
- `GET /api/rooms` — rooms with their presenters and viewer counts. This endpoint has no authentication.
- `GET /healthz` — liveness check

## Signaling protocol

JSON over WebSocket. The first message must be `join`. The server is always the offerer, so glare cannot happen.

| Direction | Message |
|---|---|
| → | `{"type":"join","room":"demo","name":"Alice","role":"presenter"\|"viewer"}` |
| ← | `{"type":"welcome","peerId","room","role","iceServers":[...]}` |
| ← | `{"type":"offer","sdp"}` (initial offer and every renegotiation) |
| → | `{"type":"answer","sdp"}` |
| ↔ | `{"type":"candidate","candidate":{RTCIceCandidateInit}}` |
| ← | `{"type":"room","presenters":[{"id","name"}],"viewers":N}` (on every join or leave) |
| ← | `{"type":"error","message"}` |
| → | `{"type":"leave"}` |

The `id` in the presenters list is the **MediaStream id** of all that presenter's forwarded tracks. In `ontrack`, `event.streams[0].id` tells you whose track it is.

Presenter clients call `getUserMedia` and `addTrack` **before** handling the first offer. The server's offer begins with a recvonly audio m-line and a recvonly video m-line, and the browser attaches the local tracks to those.

## Layout

```
main.go                     flags, HTTP server, graceful shutdown
internal/config             flag/env configuration
internal/sfu/engine.go      webrtc.API: codecs, interceptors, ICE settings
internal/sfu/room.go        Room + Manager: membership, limits, publications
internal/sfu/peer.go        Peer: one PC, negotiation state machine, subscriptions
internal/sfu/publication.go one published track, fanned out via TrackLocalStaticRTP
internal/server             WebSocket signaling, /api, static files, e2e tests
web/static                  demo client
```

## Known limitations

- **No authentication.** Anyone who can reach `/ws` can join any room as a presenter. Put a token check in front of `/ws` before exposing the server publicly.
- **No simulcast or bandwidth adaptation.** Every viewer receives the full quality the presenter sends.
- **Transceivers are not reused.** When a presenter leaves, the subscribers' m-lines go inactive and stay in the SDP. Pion does not reuse a transceiver that has already sent. The SDP therefore grows with presenter churn over a very long-lived connection. Rejoining resets it.
- **One process only.** Rooms live in memory, so the server cannot be scaled horizontally as-is.
