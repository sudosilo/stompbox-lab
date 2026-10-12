/* stompbox-lab-v5-server.js
   Stompbox Lab sync and loop server. Runs on Railway from the backend folder.

   ORANGE PHONE: stage one, WebSocket rooms, board relay, server clock pings.
   ORANGE PHONE: stage two, loop state in Redis, hand written grooves, bar line changes.
   ORANGE PHONE: stage three, Supabase login check, player heartbeats, heartbeat gate on the loop.
   ORANGE PHONE: v4, one room holds every app. The loop, the clock and the heartbeats are shared by all
   phones that typed the same room code, whatever app they run. Board messages only travel between
   phones running the same app, and each app has its own stored board inside the room.
   ORANGE PHONE: v5 seats. Every phone gets a seat number among the phones of its own app in its room,
   the lowest number not taken. A phone keeps its seat until it leaves. Apps use the seat to pick a
   different voice for each phone. server sends {type:"seat", seat, count}

   One WebSocket per phone. The address carries app, room and device in the query string.
   Every message is one JSON object with a type field.

   Board messages, relayed between the phones of one room:
     hello, change, snapshot, request   (format is in stompbox-lab-v2-sync.js)
   Clock messages:
     phone sends   {type:"ping", t0}
     server sends  {type:"pong", t0, t1}          t1 is the server clock in milliseconds
   Loop messages:
     phone sends   {type:"transport", action:"start"|"stop"|"tempo"|"groove", tempo, groove}
     server sends  {type:"loop", state, grooves, now, auth, players}
   Login messages, only used while SUPABASE_URL is set:
     phone sends   {type:"auth", token}           first message on every socket, and again after a token refresh
     server sends  {type:"auth", ok, email, reason}
   Heartbeat:
     phone sends   {type:"beat"}                  every 10 seconds while that phone is playing
   The loop only runs while one live heartbeat exists. A heartbeat lives for 20 seconds.
   When the last one runs out the loop stops and state.idle turns true. The next heartbeat starts it again.

   Loop state:
     playing    true while the loop runs
     tempo      beats per minute, four beats to the bar
     epoch      server time in milliseconds where bar number barOffset begins
     barOffset  bar number that begins at epoch
     groove     name of the groove in use
     slots      one pattern per instrument slot, in mini notation, one bar per cycle
     pending    null, or {atBar, tempo, groove, slots} that every phone switches to on that bar line
     idle       true while the loop is stopped because no player is active

   Environment variables:
     PORT             set by Railway
     REDIS_URL        optional, without it state lives in memory only
     REDIS_PREFIX     optional, key prefix, default "stompbox:"
     SUPABASE_URL       optional, with SUPABASE_ANON_KEY it turns login on
     SUPABASE_ANON_KEY  optional, the public Supabase key
     ALLOWED_EMAILS     optional, comma separated list of emails allowed to play, blank allows every signed in player
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
const BEAT_SECONDS = 20;
const START_GRACE_MS = 15000;
const AUTH_WAIT_MS = 15000;

/* ---------- login, optional ---------- */
const SUPA_URL = (process.env.SUPABASE_URL || "").replace(/\/+$/, "");
const SUPA_KEY = process.env.SUPABASE_ANON_KEY || "";
const AUTH_ON = !!(SUPA_URL && SUPA_KEY);
const ALLOWED = (process.env.ALLOWED_EMAILS || "").toLowerCase().split(",").map(s => s.trim()).filter(Boolean);
/* ORANGE PHONE: Supabase itself checks the token, so no secret is stored on this server for login. */
async function verify(token) {
  if (typeof token !== "string" || token.length < 20 || token.length > 4000) return { reason: "sign in again" };
  let user;
  try {
    const r = await fetch(SUPA_URL + "/auth/v1/user", { headers: { apikey: SUPA_KEY, authorization: "Bearer " + token }, signal: AbortSignal.timeout(6000) });
    if (!r.ok) return { reason: "sign in again" };
    user = await r.json();
  } catch (e) { return { reason: "login check failed, retrying" } }
  if (!user || !user.id) return { reason: "sign in again" };
  const email = String(user.email || "").toLowerCase();
  if (ALLOWED.length && ALLOWED.indexOf(email) < 0) return { reason: "this account is not on the player list" };
  let exp = Date.now() + 600000;
  try { exp = JSON.parse(Buffer.from(token.split(".")[1], "base64url").toString()).exp * 1000 || exp } catch (e) {}
  return { user: { id: String(user.id), email: email, exp: exp } };
}

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
async function getRoom(name) {
  let room = rooms.get(name);
  if (!room) {
    room = { id: name, sockets: new Set(), loop: newLoop(), boards: {}, boardReady: {}, boardTimers: {}, beats: new Map(), lastStart: Date.now() };
    rooms.set(name, room);
    room.ready = (async () => {
      const loop = await load("loop:" + name);
      if (validLoop(loop)) room.loop = loop;
    })();
  }
  await room.ready;
  return room;
}
/* The stored board of one app inside a room, loaded from Redis the first time a phone of that app joins. */
async function getBoard(room, app) {
  if (!room.boardReady[app]) {
    room.boards[app] = { snap: null, changes: {} };
    room.boardReady[app] = (async () => {
      const board = await load("board:" + app + ":" + room.id);
      if (board && typeof board === "object") room.boards[app] = { snap: board.snap || null, changes: board.changes || {} };
    })();
  }
  await room.boardReady[app];
  return room.boards[app];
}
function sendTo(ws, msg) { if (ws.readyState === 1) ws.send(JSON.stringify(msg)) }
/* Hands out the lowest free seat to a phone, then tells every phone of that app its seat and how many share the app. */
function seatUp(room, app) {
  const peers = [];
  room.sockets.forEach(ws => { if (ws.app === app) peers.push(ws); });
  const taken = new Set(peers.filter(ws => typeof ws.seat === "number").map(ws => ws.seat));
  peers.forEach(ws => {
    if (typeof ws.seat === "number") return;
    let n = 0; while (taken.has(n)) n++;
    ws.seat = n; taken.add(n);
  });
  peers.forEach(ws => sendTo(ws, { type: "seat", app: app, seat: ws.seat, count: peers.length }));
}

/* Board messages go to the other phones running the same app. */
function relay(room, msg, from) {
  const text = JSON.stringify(msg);
  room.sockets.forEach(ws => { if (ws !== from && ws.app === from.app && ws.readyState === 1) ws.send(text) });
}
function loopMessage(room, app) {
  fold(room.loop, Date.now());
  return { type: "loop", app: app, state: room.loop, grooves: Object.keys(GROOVES), now: Date.now(), auth: AUTH_ON, players: room.beats.size, phones: room.sockets.size };
}
/* The loop goes to every phone in the room. Each phone gets the message stamped with its own app name. */
function broadcastLoop(room) {
  const texts = {};
  room.sockets.forEach(ws => {
    if (ws.readyState !== 1) return;
    if (!texts[ws.app]) texts[ws.app] = JSON.stringify(loopMessage(room, ws.app));
    ws.send(texts[ws.app]);
  });
}
function saveBoardSoon(room, app) {
  if (room.boardTimers[app]) return;
  room.boardTimers[app] = setTimeout(() => { room.boardTimers[app] = null; save("board:" + app + ":" + room.id, room.boards[app]) }, 1000);
}

/* ---------- heartbeat gate ---------- */
function startLoop(room, now) {
  const s = room.loop;
  s.playing = true; s.idle = false; s.epoch = now + 300; s.barOffset = 0; s.pending = null;
  room.lastStart = now;
}
/* ORANGE PHONE: added heartbeat gate. One key per active player, in memory and in Redis with a 20 second expiry. */
function beat(room, ws) {
  const id = ws.user ? ws.user.id : ws.device, now = Date.now();
  room.beats.set(id, now);
  if (redis) redis.set(PREFIX + "beat:" + room.id + ":" + id, "1", "EX", BEAT_SECONDS).catch(() => {});
  if (room.loop.idle && !room.loop.playing) {
    startLoop(room, now);
    save("loop:" + room.id, room.loop); broadcastLoop(room);
  }
}
function gate(room, now) {
  room.beats.forEach((t, id) => { if (now - t > BEAT_SECONDS * 1000) room.beats.delete(id) });
  if (room.loop.playing && !room.beats.size && now - room.lastStart > START_GRACE_MS) {
    room.loop.playing = false; room.loop.pending = null; room.loop.idle = true;
    save("loop:" + room.id, room.loop); broadcastLoop(room);
    console.log("gate: no active player, loop stopped in " + room.id);
  }
}

/* ---------- loop control ---------- */
function transport(room, m) {
  const s = room.loop, now = Date.now();
  fold(s, now);
  if (m.action === "start") {
    if (s.playing) return false;
    startLoop(room, now);
    return true;
  }
  if (m.action === "stop") {
    if (!s.playing) return false;
    s.playing = false; s.pending = null; s.idle = false;
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
  m.app = ws.app; m.device = ws.device;
  const board = room.boards[ws.app];

  if (m.type === "beat") { beat(room, ws); return }
  if (m.type === "transport") {
    if (transport(room, m)) { save("loop:" + room.id, room.loop); broadcastLoop(room) }
    return;
  }
  if (m.type === "hello") { relay(room, m, ws); return }
  if (m.type === "change") {
    if (typeof m.path !== "string" || m.path.length > 64) return;
    if (m.path in board.changes || Object.keys(board.changes).length < MAX_BOARD_PATHS) board.changes[m.path] = m.value;
    saveBoardSoon(room, ws.app); relay(room, m, ws); return;
  }
  if (m.type === "snapshot") {
    if (!m.state || typeof m.state !== "object") return;
    room.boards[ws.app] = { snap: m, changes: {} };
    saveBoardSoon(room, ws.app); relay(room, m, ws); return;
  }
  if (m.type === "request") { relay(room, m, ws); return }
}

/* A phone that joins gets the loop first, then the board the room already holds. */
function welcome(room, ws) {
  sendTo(ws, loopMessage(room, ws.app));
  const b = room.boards[ws.app];
  if (b.snap) {
    sendTo(ws, b.snap);
    Object.keys(b.changes).forEach(path => sendTo(ws, { type: "change", app: ws.app, schema: b.snap.schema, room: b.snap.room, device: "server", seq: 0, ts: Date.now(), path: path, value: b.changes[path] }));
    return;
  }
  for (const other of room.sockets) {
    if (other !== ws && other.app === ws.app && other.readyState === 1 && typeof other.schema === "number") {
      sendTo(other, { type: "request", app: ws.app, schema: other.schema, device: "server", seq: 0, ts: Date.now() });
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
    res.end(JSON.stringify({ ok: true, version: 5, rooms: rooms.size, phones: phones, redis: redis ? redis.status : "off", login: AUTH_ON ? "on" : "off" }));
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
  ws.app = app;
  if (!app || !name || !ws.device) { ws.close(1008, "app, room and device are required"); return }

  /* ORANGE PHONE: a phone enters its room at once while login is off, or after its token checks out while login is on.
     Until then only clock pings and the login message are answered. Other messages wait in a short queue. */
  const early = []; let room = null, entering = false;
  async function enter() {
    if (room || entering) return;
    entering = true;
    const loaded = await getRoom(name);
    await getBoard(loaded, app);
    if (ws.readyState !== 1) return;
    if (loaded.sockets.size >= MAX_PHONES_PER_ROOM) { ws.close(1013, "room full"); return }
    loaded.sockets.add(ws);
    room = loaded;
    early.forEach(text => onMessage(room, ws, text));
    early.length = 0;
    welcome(room, ws);
    seatUp(room, app);
  }
  ws.on("close", () => {
    if (!room) return;
    room.sockets.delete(ws);
    seatUp(room, ws.app);
    if (!room.sockets.size) {
      Object.keys(room.boardTimers).forEach(a => {
        if (room.boardTimers[a]) { clearTimeout(room.boardTimers[a]); room.boardTimers[a] = null; save("board:" + a + ":" + room.id, room.boards[a]) }
      });
      if (redis && !room.loop.playing) rooms.delete(room.id);
    }
  });
  ws.on("message", async data => {
    const text = data.toString();
    let head = null;
    if (text.length < 5000) { try { head = JSON.parse(text) } catch (e) { return } }
    if (head && head.type === "ping") { sendTo(ws, { type: "pong", app: app, t0: head.t0, t1: Date.now() }); return }
    if (head && head.type === "auth") {
      if (!AUTH_ON) return;
      const result = await verify(head.token);
      if (ws.readyState !== 1) return;
      if (!result.user) { sendTo(ws, { type: "auth", app: app, ok: false, reason: result.reason }); return }
      ws.user = result.user;
      sendTo(ws, { type: "auth", app: app, ok: true, email: result.user.email });
      enter();
      return;
    }
    if (room) onMessage(room, ws, text); else if (early.length < 20) early.push(text);
  });
  ws.alive = true; ws.on("pong", () => { ws.alive = true });
  ws.opened = Date.now();
  if (!AUTH_ON) enter();
});

/* Every few seconds: run the heartbeat gate, and close phones whose login is missing or ran out. */
const gateTimer = setInterval(() => {
  const now = Date.now();
  rooms.forEach(room => {
    gate(room, now);
    if (redis && !room.sockets.size && !room.loop.playing) rooms.delete(room.id);
  });
  if (!AUTH_ON) return;
  wss.clients.forEach(ws => {
    if (!ws.user) { if (now - ws.opened > AUTH_WAIT_MS) ws.close(1008, "sign in") }
    else if (now > ws.user.exp + 30000) ws.close(1008, "login expired");
  });
}, 5000);

/* Drop phones that stopped answering. */
const sweep = setInterval(() => {
  wss.clients.forEach(ws => { if (!ws.alive) { ws.terminate(); return } ws.alive = false; try { ws.ping() } catch (e) {} });
}, 25000);

server.listen(PORT, () => console.log("stompbox-lab server on port " + PORT + ", redis " + (redis ? "on" : "off") + ", login " + (AUTH_ON ? "on" : "off")));
process.on("SIGTERM", () => { clearInterval(sweep); clearInterval(gateTimer); wss.clients.forEach(ws => ws.close(1001)); server.close(() => process.exit(0)); setTimeout(() => process.exit(0), 2000) });
