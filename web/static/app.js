'use strict';

// Signaling protocol (JSON over WebSocket at /ws):
//   -> join {room, name, role}         <- welcome {peerId, iceServers}
//   <- offer {sdp}                      -> answer {sdp}
//   <-> candidate {candidate}           <- room {presenters: [{id, name}], viewers}
//   <- error {message}                  -> leave
// The server is always the offerer. Every remote track's MediaStream id is
// the presenter's peer id, so tracks are grouped into one tile per presenter.

const $ = (s) => document.querySelector(s);

let ws = null;
let pc = null;
let localStream = null;
let myRole = null;
let pendingCandidates = [];
let queue = Promise.resolve(); // processes signaling messages strictly in order
const presenterNames = new Map(); // presenter id -> name
const tiles = new Map();          // stream/presenter id -> tile element

function setStatus(text) { $('#status').textContent = text; }

function send(msg) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg));
}

// ---- tiles ----------------------------------------------------------------

function ensureTile(id, label, stream, local = false) {
  let tile = tiles.get(id);
  if (!tile) {
    tile = document.createElement('div');
    tile.className = 'tile' + (local ? ' local' : '');
    const video = document.createElement('video');
    video.autoplay = true;
    video.playsInline = true;
    video.muted = local; // never play back our own mic
    const span = document.createElement('span');
    span.className = 'label';
    tile.append(video, span);
    tiles.set(id, tile);
    if (local) $('#grid').prepend(tile); else $('#grid').append(tile);
  }
  const video = tile.querySelector('video');
  if (stream && video.srcObject !== stream) {
    video.srcObject = stream;
    video.play().catch(() => setStatus('Click anywhere to enable playback'));
  }
  tile.querySelector('.label').textContent = label;
  updateEmpty();
  return tile;
}

function removeTile(id) {
  const tile = tiles.get(id);
  if (!tile) return;
  tile.querySelector('video').srcObject = null;
  tile.remove();
  tiles.delete(id);
  updateEmpty();
}

function updateEmpty() {
  const remote = [...tiles.keys()].filter((id) => id !== 'local').length;
  $('#empty').classList.toggle('hidden', !ws || remote > 0);
}

document.addEventListener('click', () => {
  document.querySelectorAll('video').forEach((v) => v.paused && v.srcObject && v.play().catch(() => {}));
});

// ---- signaling --------------------------------------------------------------

async function handle(msg) {
  switch (msg.type) {
    case 'welcome':
      setStatus(`Joined "${msg.room}" as ${msg.role}`);
      createPeerConnection(msg.iceServers || []);
      break;

    case 'offer': {
      await pc.setRemoteDescription({ type: 'offer', sdp: msg.sdp });
      for (const c of pendingCandidates) await addCandidate(c);
      pendingCandidates = [];
      await pc.setLocalDescription(await pc.createAnswer());
      send({ type: 'answer', sdp: pc.localDescription.sdp });
      break;
    }

    case 'candidate':
      if (!pc || !pc.remoteDescription) pendingCandidates.push(msg.candidate);
      else await addCandidate(msg.candidate);
      break;

    case 'room':
      renderRoom(msg);
      break;

    case 'error':
      setStatus('Error: ' + msg.message);
      break;
  }
}

async function addCandidate(c) {
  try { await pc.addIceCandidate(c); } catch (e) { console.warn('addIceCandidate', e); }
}

function createPeerConnection(iceServers) {
  pc = new RTCPeerConnection({ iceServers });

  // Presenter tracks attach to the server's recvonly audio/video m-lines,
  // which always come first in the server's offer.
  if (localStream) {
    for (const track of localStream.getTracks()) pc.addTrack(track, localStream);
  }

  pc.onicecandidate = (e) => {
    if (e.candidate && e.candidate.candidate) send({ type: 'candidate', candidate: e.candidate.toJSON() });
  };

  pc.ontrack = (e) => {
    const stream = e.streams[0];
    if (!stream) return;
    ensureTile(stream.id, presenterNames.get(stream.id) || 'Presenter', stream);
    stream.onremovetrack = () => {
      if (stream.getTracks().length === 0) removeTile(stream.id);
    };
  };

  pc.onconnectionstatechange = () => {
    setStatus('Connection: ' + pc.connectionState);
    if (pc.connectionState === 'failed') leave('Connection failed');
  };
}

function renderRoom(msg) {
  presenterNames.clear();
  for (const p of msg.presenters) presenterNames.set(p.id, p.name);

  for (const id of [...tiles.keys()]) {
    if (id === 'local') continue;
    if (!presenterNames.has(id)) removeTile(id);
    else tiles.get(id).querySelector('.label').textContent = presenterNames.get(id);
  }
  const n = msg.presenters.length;
  $('#room-info').textContent =
    `${n} presenter${n === 1 ? '' : 's'} · ${msg.viewers} viewer${msg.viewers === 1 ? '' : 's'}`;
}

// ---- join / leave -----------------------------------------------------------

async function join(e) {
  e.preventDefault();
  const room = $('#room').value.trim();
  const name = $('#name').value.trim();
  myRole = $('#role').value;
  $('#join-btn').disabled = true;

  if (myRole === 'presenter') {
    try {
      localStream = await navigator.mediaDevices.getUserMedia({
        audio: true,
        video: { width: { ideal: 1280 }, height: { ideal: 720 } },
      });
    } catch (err) {
      setStatus('Camera/mic unavailable: ' + err.message);
      $('#join-btn').disabled = false;
      return;
    }
    ensureTile('local', (name || 'You') + ' (you)', localStream, true);
  }

  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  ws = new WebSocket(`${proto}://${location.host}/ws`);
  ws.onopen = () => send({ type: 'join', room, name, role: myRole });
  ws.onmessage = (ev) => {
    const msg = JSON.parse(ev.data);
    queue = queue.then(() => handle(msg)).catch((err) => console.error('signaling', err));
  };
  ws.onclose = () => leave();

  $('#join-form').classList.add('hidden');
  $('#controls').classList.remove('hidden');
  document.querySelectorAll('.presenter-only').forEach((b) => b.classList.toggle('hidden', myRole !== 'presenter'));
  history.replaceState(null, '', `?room=${encodeURIComponent(room)}`);
  setStatus('Connecting…');
}

function leave(reason) {
  if (!ws && !pc) return;
  send({ type: 'leave' });
  const sock = ws;
  ws = null;
  if (sock) { sock.onclose = null; sock.close(); }
  if (pc) { pc.close(); pc = null; }
  if (localStream) { localStream.getTracks().forEach((t) => t.stop()); localStream = null; }
  for (const id of [...tiles.keys()]) removeTile(id);
  pendingCandidates = [];
  queue = Promise.resolve();
  presenterNames.clear();

  $('#join-form').classList.remove('hidden');
  $('#controls').classList.add('hidden');
  $('#join-btn').disabled = false;
  $('#mic-btn').textContent = 'Mute mic'; $('#mic-btn').classList.remove('off');
  $('#cam-btn').textContent = 'Stop camera'; $('#cam-btn').classList.remove('off');
  setStatus(reason || 'Disconnected');
}

function toggle(kind, btn, onLabel, offLabel) {
  if (!localStream) return;
  const tracks = kind === 'audio' ? localStream.getAudioTracks() : localStream.getVideoTracks();
  const enabled = !tracks.every((t) => !t.enabled);
  tracks.forEach((t) => { t.enabled = !enabled; });
  btn.textContent = enabled ? offLabel : onLabel;
  btn.classList.toggle('off', enabled);
}

$('#join-form').addEventListener('submit', join);
$('#leave-btn').addEventListener('click', () => leave('Left the room'));
$('#mic-btn').addEventListener('click', (e) => toggle('audio', e.target, 'Mute mic', 'Unmute mic'));
$('#cam-btn').addEventListener('click', (e) => toggle('video', e.target, 'Stop camera', 'Start camera'));
window.addEventListener('beforeunload', () => leave());

const params = new URLSearchParams(location.search);
if (params.get('room')) $('#room').value = params.get('room');
if (params.get('role') === 'viewer') $('#role').value = 'viewer';
