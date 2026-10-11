/* stompbox-lab-v2-server.js
   Stompbox Lab sync and loop server. Runs on Railway from the backend folder.

   ORANGE PHONE: stage one, WebSocket rooms, board relay, server clock pings.
   ORANGE PHONE: stage two, loop state in Redis, hand written grooves, bar line changes.

   One WebSocket per phone. The address carries app, room and device in the query string.
   Every message is one JSON object with a type field.

   Board messages, relayed between the phones of one room:
     hello, change, snapshot, request   (format is in stompbox-lab-v2-sync.js)
   Clock messages:
     phone sends   {type:"ping", t0}
     server sends  {type:"pong", t0, t1}          t1 is the server clock in milliseconds
   Loop messages:
     phone sends   {type:"transport", action:"start"|"stop"|"tempo"|"groove", tempo, groove}
     server sends  {type:"loop", state, grooves, now}

   Loop state:
     playing    true while the loop runs
     tempo      beats per minute, four beats to the bar
     epoch      server time in milliseconds where bar number barOffset begins
     barOffset  bar number that begins at epoch
     groove     name of the groove in use
     slots      one pattern per instrument slot, in mini notation, one bar per cycle
     pending    null, or {atBar, tempo, groove, slots} that every phone switches to on that bar line

   Environment variables:
     PORT             set by Railway
     REDIS_URL        optional, without it state lives in memory only
     REDIS_PREFIX     optional, key prefix, default "stompbox:"
     ALLOWED_ORIGINS  optional, comma separated list of page origins allowed to connect */
"use strict";

const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;
const PREFIX = process.env.REDIS_PREFIX || "stompbox:";
const TTL_SECONDS = 60 * 60 * 24 * 7;
const ORIGINS = (process.env.ALLOWED_ORIGINS || "").split(",").map(s => s.trim()).filter(Boolean);
const MAX_PHONES_PER_ROOM = 16;
const MAX_MESSAGES_PER_SECOND = 80;
const MAX_BOARD_PATHS = 500;
const TEMPO_MIN = 50, TEMPO_MAX = 200;
const SLOT_IDS = ["drums", "bass", "lead", "pad"];

/* ORANGE PHONE: hand written grooves. The pad slot exists in every groove and starts silent. */
const GROOVES = {
  "Click": {
    drums: "ck cl cl cl",
    bass: "~", lead: "~", pad: "~"
  },
  "Pocket": {
    drums: "bd ~ [~ bd] ~, ~ sd ~ sd, hh*8",
    bass: "<[a1 ~ a1 ~ ~ a1 c2 ~] [f1 ~ f1 ~ ~ f1 g1 ~]>",
    lead: "<[~ e4 ~ c4 ~ a3 ~ ~] [~ d4 ~ c4 ~ a3 g3 ~]>",
    pad: "~"
  },
  "Four Floor": {
    drums: "bd*4, ~ cp ~ cp, [~ oh]*4",
    bass: "<[~ e1]*4 [~ e1]*4 [~ g1]*4 [~ d1]*4>",
    lead: "<[e4 ~ g4 ~ b4 ~ g4 ~] [d4 ~ g4 ~ b4 ~ a4 ~]>",
    pad: "~"
  },
  "Half Time": {
    drums: "bd ~ ~ ~ ~ ~ bd ~, ~ ~ ~ ~ sd ~ ~ ~, hh*4",
    bass: "<[d1 ~ ~ ~ ~ ~ d1 ~] [bb0 ~ ~ ~ ~ ~ c1 ~]>",
    lead: "<[~ ~ a4 ~ f4 ~ ~ d4] [~ ~ g4 ~ f4 ~ e4 ~]>",
    pad: "<[d3,f3,a3] [bb2,d3,f3]>"
  }
};
const DEFAULT_GROOVE = "Pocket";

/* ---------- redis, optional ---------- */
let redis = null;
if (process.env.REDIS_URL) {
  try {
    const Redis = require("ioredis");
    redis = new Redis(process.env.REDIS_URL, { family: 0, maxRetriesPerRequest: 2, enableOfflineQueue: false });
    let complained = false;
    redis.on("error", e => { if (!complained) { complained = true; console.log("redis error:", e.message) } });
    redis.on("ready", () => { complained = false; console.log("redis ready") });
  } catch (e) { console.log("redis unavailable:", e.message); redis = null }
}
function save(key, value) {
  if (!redis) return;
  redis.set(PREFIX + key, JSON.stringify(value), "EX", TTL_SECONDS).catch(() => {});
}
async function load(key) {
  if (!redis) return null;
  try { const text = await redis.get(PREFIX + key); return text ? JSON.parse(text) : null } catch (e) { return null }
}

/* ---------- loop timeline ---------- */
const barMs = tempo => 240000 / tempo;
function barTime(s, bar) {
  const p = s.pending;
  if (p && bar >= p.atBar) {
    const at = s.epoch + (p.atBar - s.barOffset) * barMs(s.tempo);
    return at + (bar - p.atBar) * barMs(p.tempo);
  }
  return s.epoch + (bar - s.barOffset) * barMs(s.tempo);
}
function barAt(s, t) {
  const p = s.pending;
  if (p) {
    const at = s.epoch + (p.atBar - s.barOffset) * barMs(s.tempo);
    if (t >= at) return p.atBar + Math.floor((t - at) / barMs(p.tempo));
  }
  return s.barOffset + Math.floor((t - s.epoch) / barMs(s.tempo));
}
/* Once the bar line of a pending change has passed, the change becomes the current state. */
function fold(s, now) {
  const p = s.pending;
  if (!p || now < barTime(s, p.atBar)) return;
  s.epoch = barTime(s, p.atBar);
  s.barOffset = p.atBar;
  s.tempo = p.tempo; s.groove = p.groove; s.slots = p.slots;
  s.pending = null;
}
function newLoop() {
  return { playing: false, tempo: 100, epoch: Date.now(), barOffset: 0, groove: DEFAULT_GROOVE,
    slots: Object.assign({}, GROOVES[DEFAULT_GROOVE]), pending: null };
}
function validLoop(s) {
  return s && typeof s.tempo === "number" && typeof s.epoch === "number" && s.slots && typeof s.slots === "object";
}

/* ---------- rooms ---------- */
const rooms = new Map();
async function getRoom(app, name) {
  const id = app + ":" + name;
  let room = rooms.get(id);
  if (!room) {
    room = { id: id, app: app, sockets: new Set(), loop: newLoop(), board: { snap: null, changes: {} }, boardTimer: null };
    rooms.set(id, room);
    room.ready = (async () => {
      const [loop, board] = await Promise.all([load("loop:" + id), load("board:" + id)]);
      if (validLoop(loop)) room.loop = loop;
      if (board && typeof board === "object") room.board = { snap: board.snap || null, changes: board.changes || {} };
    })();
  }
  await room.ready;
  return room;
}
function sendTo(ws, msg) { if (ws.readyState === 1) ws.send(JSON.stringify(msg)) }
function broadcast(room, msg, except) {
  const text = JSON.stringify(msg);
  room.sockets.forEach(ws => { if (ws !== except && ws.readyState === 1) ws.send(text) });
}
function loopMessage(room) {
  fold(room.loop, Date.now());
  return { type: "loop", app: room.app, state: room.loop, grooves: Object.keys(GROOVES), now: Date.now() };
}
function saveBoardSoon(room) {
  if (room.boardTimer) return;
  room.boardTimer = setTimeout(() => { room.boardTimer = null; save("board:" + room.id, room.board) }, 1000);
}

/* ---------- loop control ---------- */
function transport(room, m) {
  const s = room.loop, now = Date.now();
  fold(s, now);
  if (m.action === "start") {
    if (s.playing) return false;
    s.playing = true; s.epoch = now + 300; s.barOffset = 0; s.pending = null;
    return true;
  }
  if (m.action === "stop") {
    if (!s.playing) return false;
    s.playing = false; s.pending = null;
    return true;
  }
  const next = s.pending
    ? { tempo: s.pending.tempo, groove: s.pending.groove, slots: s.pending.slots }
    : { tempo: s.tempo, groove: s.groove, slots: s.slots };
  if (m.action === "tempo") {
    const t = Math.round(+m.tempo);
    if (!(t >= TEMPO_MIN && t <= TEMPO_MAX)) return false;
    next.tempo = t;
  } else if (m.action === "groove") {
    if (!Object.prototype.hasOwnProperty.call(GROOVES, m.groove)) return false;
    next.groove = m.groove; next.slots = Object.assign({}, GROOVES[m.groove]);
  } else return false;
  if (!s.playing) { s.tempo = next.tempo; s.groove = next.groove; s.slots = next.slots; s.pending = null; return true }
  /* A change while playing lands on a bar line. Tempo never changes inside a bar. */
  next.atBar = barAt(s, now + 400) + 1;
  s.pending = next;
  return true;
}

/* ---------- messages ---------- */
function onMessage(room, ws, text) {
  const second = Math.floor(Date.now() / 1000);
  if (ws.rateSecond !== second) { ws.rateSecond = second; ws.rateCount = 0 }
  if (++ws.rateCount > MAX_MESSAGES_PER_SECOND) return;
  let m; try { m = JSON.parse(text) } catch (e) { return }
  if (!m || typeof m !== "object" || typeof m.type !== "string") return;
  if (typeof m.schema === "number") ws.schema = m.schema;
  m.app = room.app; m.device = ws.device;

  if (m.type === "ping") { sendTo(ws, { type: "pong", app: room.app, t0: m.t0, t1: Date.now() }); return }
  if (m.type === "transport") {
    if (transport(room, m)) { save("loop:" + room.id, room.loop); broadcast(room, loopMessage(room)) }
    return;
  }
  if (m.type === "hello") { broadcast(room, m, ws); return }
  if (m.type === "change") {
    if (typeof m.path !== "string" || m.path.length > 64) return;
    if (m.path in room.board.changes || Object.keys(room.board.changes).length < MAX_BOARD_PATHS) room.board.changes[m.path] = m.value;
    saveBoardSoon(room); broadcast(room, m, ws); return;
  }
  if (m.type === "snapshot") {
    if (!m.state || typeof m.state !== "object") return;
    room.board = { snap: m, changes: {} };
    saveBoardSoon(room); broadcast(room, m, ws); return;
  }
  if (m.type === "request") { broadcast(room, m, ws); return }
}

/* A phone that joins gets the loop first, then the board the room already holds. */
function welcome(room, ws) {
  sendTo(ws, loopMessage(room));
  const b = room.board;
  if (b.snap) {
    sendTo(ws, b.snap);
    Object.keys(b.changes).forEach(path => sendTo(ws, { type: "change", app: room.app, schema: b.snap.schema, room: b.snap.room, device: "server", seq: 0, ts: Date.now(), path: path, value: b.changes[path] }));
    return;
  }
  for (const other of room.sockets) {
    if (other !== ws && other.readyState === 1 && typeof other.schema === "number") {
      sendTo(other, { type: "request", app: room.app, schema: other.schema, device: "server", seq: 0, ts: Date.now() });
      return;
    }
  }
}

/* ---------- http and websocket ---------- */
const clean = (v, max, pattern) => String(v || "").toLowerCase().replace(pattern, "").slice(0, max);

const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    let phones = 0; rooms.forEach(r => { phones += r.sockets.size });
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ ok: true, rooms: rooms.size, phones: phones, redis: redis ? redis.status : "off" }));
    return;
  }
  res.writeHead(200, { "content-type": "text/plain" });
  res.end("stompbox-lab sync server\n");
});

const wss = new WebSocketServer({ server: server, maxPayload: 64 * 1024 });
wss.on("connection", async (ws, req) => {
  ws.on("error", () => {});
  if (ORIGINS.length && ORIGINS.indexOf(req.headers.origin) < 0) { ws.close(1008, "origin"); return }
  let q;
  try { q = new URL(req.url, "http://x").searchParams } catch (e) { ws.close(1008, "address"); return }
  const app = clean(q.get("app"), 32, /[^a-z0-9-]/g);
  const name = clean(q.get("room"), 24, /[^a-z0-9]/g);
  ws.device = clean(q.get("device"), 64, /[^a-z0-9-]/g);
  if (!app || !name || !ws.device) { ws.close(1008, "app, room and device are required"); return }

  /* Messages that arrive while the room loads from Redis wait in a short queue. */
  const early = []; let room = null;
  ws.on("message", data => { const text = data.toString(); if (room) onMessage(room, ws, text); else if (early.length < 20) early.push(text) });
  ws.alive = true; ws.on("pong", () => { ws.alive = true });

  const loaded = await getRoom(app, name);
  if (ws.readyState !== 1) return;
  if (loaded.sockets.size >= MAX_PHONES_PER_ROOM) { ws.close(1013, "room full"); return }
  loaded.sockets.add(ws);
  ws.on("close", () => {
    loaded.sockets.delete(ws);
    if (!loaded.sockets.size) {
      if (loaded.boardTimer) { clearTimeout(loaded.boardTimer); loaded.boardTimer = null; save("board:" + loaded.id, loaded.board) }
      if (redis) rooms.delete(loaded.id);
    }
  });
  room = loaded;
  early.forEach(text => onMessage(room, ws, text));
  welcome(room, ws);
});

/* Drop phones that stopped answering. */
const sweep = setInterval(() => {
  wss.clients.forEach(ws => { if (!ws.alive) { ws.terminate(); return } ws.alive = false; try { ws.ping() } catch (e) {} });
}, 25000);

server.listen(PORT, () => console.log("stompbox-lab server on port " + PORT + ", redis " + (redis ? "on" : "off")));
process.on("SIGTERM", () => { clearInterval(sweep); wss.clients.forEach(ws => ws.close(1001)); server.close(() => process.exit(0)); setTimeout(() => process.exit(0), 2000) });
