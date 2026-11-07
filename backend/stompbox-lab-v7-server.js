/* stompbox-lab-v7-server.js
   Stompbox Lab sync and loop server. Runs on Railway from the backend folder.

   BLUE PHONE: stage one, WebSocket rooms, board relay, server clock pings.
   BLUE PHONE: stage two, loop state in Redis, hand written grooves, bar line changes.
   BLUE PHONE: stage three, Supabase login check, player heartbeats, heartbeat gate on the loop.
   BLUE PHONE: v4, one room holds every app. The loop, the clock and the heartbeats are shared by all
   phones that typed the same room code, whatever app they run. Board messages only travel between
   phones running the same app, and each app has its own stored board inside the room.
   BLUE PHONE: v5 seats. Every phone gets a seat number among the phones of its own app in its room,
   the lowest number not taken. A phone keeps its seat until it leaves. Apps use the seat to pick a
   different voice for each phone. server sends {type:"seat", seat, count}
   BLUE PHONE: v6 roster. Every phone in a room gets the list of every phone in that room, whatever app it runs:
   server sends {type:"roster", app:"roster", phones:[{app, seat, id, playing}]}
   id is the start of the device id. playing is true while that phone has a live heartbeat.
   The app field reads "roster", so apps that only listen to their own app name drop it untouched.
   Hoofer Sync uses it to put one dancer on the floor for every phone in the band.
   BLUE PHONE: v6 more grooves, House, Jungle, Disco, Funk, Boom Bap, Trap, Reggaeton and Rock, each with its own tempo.
   BLUE PHONE: v6 seat fix. The seat message carries the schema of the phone it goes to, and a phone only gets it
   once its hello has told the server that schema. Apps without a seat handler (Axiom, Looper) treated the untagged
   seat message as a newer schema and showed "update needed", which dropped them out of the band.
   BLUE PHONE: v7 stadium mode. A phone that connects with mode=stadium in its address is a stadium seat.
   A room holding stadium seats takes up to 2500 phones. Stadium seats skip login, auto seats and the roster,
   so a full stadium never floods the room. Each seat phone tells the server the seat it scanned, and a host
   screen unlocked with the STADIUM_HOST_CODE variable sends the seat layout, the light show and the sound flybys.
     seat phone sends  {type:"stadium-seat", seat:"A-3-12"}
     host sends        {type:"stadium-host", code}        server answers {type:"stadium-host", ok, reason}
     host sends        {type:"stadium-layout", layout}    server sends every phone {type:"stadium-layout", layout, v}
     host sends        {type:"stadium-cue", cue}          server sends every phone {type:"stadium-cue", cue}
     host sends        {type:"stadium-sound", sound}      server sends every phone {type:"stadium-sound", sound}
     server sends hosts {type:"stadium-count", phones, seats:[...]} every two seconds while it changes
   The layout and the cue are kept in Redis, so a phone that joins late gets both at once.
   BLUE PHONE: v7 seating plan. A section can set its own seat count for any row (rowSeats) and switch single
   seats off (off, as "row-seat"). Host screens share one seating plan: the guest count, the fill pattern and the
   seats already handed out at the door, in the order they were handed out.
     host sends  {type:"stadium-plan", plan}       server stores it and sends every host {type:"stadium-plan", plan}
     host sends  {type:"stadium-assign", seat}     server adds the seat to the plan unless it is already handed out,
                                                   then sends every host the plan and this host {type:"stadium-assigned", seat, ok}

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
     ALLOWED_ORIGINS  optional, comma separated list of page origins allowed to connect
     STADIUM_HOST_CODE  optional, the code that unlocks a stadium host screen; without it no host can unlock */
"use strict";

const http = require("http");
const crypto = require("crypto");
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
/* BLUE PHONE: v7 stadium limits */
const MAX_STADIUM_PHONES = 2500;
const MAX_STADIUM_JSON = 48000;
const HOST_CODE = process.env.STADIUM_HOST_CODE || "";
const CUE_TYPES = ["hold", "rainbow", "wave", "ripple", "beat", "picture", "check", "dark"];

/* ---------- login, optional ---------- */
const SUPA_URL = (process.env.SUPABASE_URL || "").replace(/\/+$/, "");
const SUPA_KEY = process.env.SUPABASE_ANON_KEY || "";
const AUTH_ON = !!(SUPA_URL && SUPA_KEY);
const ALLOWED = (process.env.ALLOWED_EMAILS || "").toLowerCase().split(",").map(s => s.trim()).filter(Boolean);
/* BLUE PHONE: Supabase itself checks the token, so no secret is stored on this server for login. */
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

/* BLUE PHONE: hand written grooves. The pad slot exists in every groove and starts silent. */
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
  },
  /* BLUE PHONE: v6 more grooves. Each of these sets its own tempo when picked. */
  "House": {
    drums: "bd*4, ~ cp ~ cp, [~ oh]*4, hh*16",
    bass: "<[~ a1 ~ a1 ~ a1 ~ a2] [~ f1 ~ f1 ~ f1 ~ f2] [~ c2 ~ c2 ~ c2 ~ c3] [~ g1 ~ g1 ~ g1 ~ g2]>",
    lead: "<[~ [a4,c5,e5] ~ ~ ~ [a4,c5,e5] ~ ~] [~ [f4,a4,c5] ~ ~ ~ [f4,a4,c5] ~ ~] [~ [c5,e5,g5] ~ ~ ~ [c5,e5,g5] ~ ~] [~ [g4,b4,d5] ~ ~ ~ [g4,b4,d5] ~ ~]>",
    pad: "<[a3,c4,e4] [f3,a3,c4] [c4,e4,g4] [g3,b3,d4]>"
  },
  "Jungle": {
    drums: "bd ~ ~ ~ sd ~ ~ bd ~ ~ bd ~ sd ~ [~ sd] ~, [~ hh]*8, <~ ~ ~ [~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ sd sd sd sd]>",
    bass: "<[d1@3 ~ ~ ~ f1 ~] [a0@3 ~ ~ ~ c1 ~]>",
    lead: "<[~ ~ ~ ~ ~ ~ d4 ~] [~ ~ ~ ~ ~ ~ ~ ~] [~ ~ ~ ~ ~ ~ f4 e4] [~ ~ ~ ~ ~ ~ ~ ~]>",
    pad: "<[d3,f3,a3] [a2,c3,e3]>"
  },
  "Disco": {
    drums: "bd*4, ~ sd ~ sd, [~ oh]*4, hh*8",
    bass: "<[c2 c3]*4 [a1 a2]*4 [f1 f2]*4 [g1 g2]*4>",
    lead: "<[e5 ~ g5 ~ e5 ~ c5 ~] [e5 ~ c5 ~ a4 ~ ~ ~] [f5 ~ a5 ~ f5 ~ c5 ~] [d5 ~ b4 ~ g4 ~ ~ ~]>",
    pad: "<[c4,e4,g4] [a3,c4,e4] [f3,a3,c4] [g3,b3,d4]>"
  },
  "Funk": {
    drums: "bd ~ ~ bd ~ ~ bd ~ ~ ~ bd ~ ~ ~ ~ ~, ~ ~ ~ ~ sd ~ ~ ~ ~ ~ ~ ~ sd ~ ~ sd, hh*16",
    bass: "<[e1 ~ e2 ~ ~ e1 ~ g1 ~ a1 ~ ~ b1 ~ d2 ~] [e1 ~ e2 ~ ~ e1 ~ g1 ~ a1 ~ g1 ~ e1 d1 ~]>",
    lead: "<[~ ~ [e4,g4] ~ ~ [e4,g4] ~ ~ ~ ~ [d4,a4] ~ ~ ~ ~ ~]>",
    pad: "~"
  },
  "Boom Bap": {
    drums: "bd ~ ~ ~ ~ ~ bd ~ ~ bd ~ ~ ~ ~ ~ ~, ~ ~ ~ ~ sd ~ ~ ~ ~ ~ ~ ~ sd ~ ~ ~, [hh hh hh oh]*2",
    bass: "<[c2 ~ ~ ~ ~ ~ c2 ~ ~ eb2 ~ ~ ~ ~ ~ ~] [ab1 ~ ~ ~ ~ ~ ab1 ~ ~ bb1 ~ ~ ~ ~ ~ ~]>",
    lead: "<[~ ~ ~ ~ ~ ~ ~ ~ g4 ~ eb4 ~ ~ ~ ~ ~] [~ ~ ~ ~ ~ ~ ~ ~ f4 ~ c4 ~ ~ ~ ~ ~]>",
    pad: "<[c4,eb4,g4] [ab3,c4,eb4]>"
  },
  "Trap": {
    drums: "bd ~ ~ ~ ~ ~ ~ bd ~ ~ bd ~ ~ ~ ~ ~, ~ ~ ~ ~ ~ ~ ~ ~ cp ~ ~ ~ ~ ~ ~ ~, <hh*8 [hh hh hh hh hh hh [hh hh hh] [hh hh hh]]>",
    bass: "<[c1 ~ ~ ~ ~ ~ ~ c1 ~ ~ eb1 ~ ~ ~ ~ ~] [ab0 ~ ~ ~ ~ ~ ~ ab0 ~ ~ bb0 ~ ~ ~ ~ ~]>",
    lead: "<[~ g4 ~ ~ eb4 ~ ~ ~ ~ g4 ~ ~ c5 ~ ~ ~] [~ ab4 ~ ~ eb4 ~ ~ ~ ~ ab4 ~ ~ bb4 ~ ~ ~]>",
    pad: "~"
  },
  "Reggaeton": {
    drums: "bd*4, [~ ~ ~ sd ~ ~ sd ~]*2, hh*8",
    bass: "<[a1 ~ ~ a1 ~ ~ a1 ~] [f1 ~ ~ f1 ~ ~ f1 ~] [c2 ~ ~ c2 ~ ~ c2 ~] [g1 ~ ~ g1 ~ ~ g1 ~]>",
    lead: "<[~ e5 ~ ~ c5 ~ ~ ~] [~ c5 ~ ~ a4 ~ ~ ~] [~ e5 ~ ~ g5 ~ ~ ~] [~ d5 ~ ~ b4 ~ ~ ~]>",
    pad: "<[a3,c4,e4] [f3,a3,c4] [c4,e4,g4] [g3,b3,d4]>"
  },
  "Rock": {
    drums: "bd [~ bd] bd ~, ~ sd ~ sd, hh*8",
    bass: "<[e1 e1 e1 e1 e1 e1 e1 e1] [e1 e1 e1 e1 g1 g1 a1 a1]>",
    lead: "<[e4 ~ ~ ~ g4 ~ a4 ~] [e4 ~ ~ ~ d4 ~ e4 ~]>",
    pad: "~"
  }
};
const GROOVE_TEMPO = { "House": 124, "Jungle": 172, "Disco": 118, "Funk": 100, "Boom Bap": 90, "Trap": 140, "Reggaeton": 96, "Rock": 120 };
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
  room.sockets.forEach(ws => { if (ws.app === app && !ws.stadium) peers.push(ws); });
  const taken = new Set(peers.filter(ws => typeof ws.seat === "number").map(ws => ws.seat));
  peers.forEach(ws => {
    if (typeof ws.seat === "number") return;
    let n = 0; while (taken.has(n)) n++;
    ws.seat = n; taken.add(n);
  });
  peers.forEach(ws => { if (typeof ws.schema === "number") sendTo(ws, { type: "seat", app: app, schema: ws.schema, seat: ws.seat, count: peers.length }) });
}

/* Board messages go to the other phones running the same app. */
/* BLUE PHONE: v6 roster. Sent when a phone joins or leaves, and when a phone starts or stops playing. */
function rosterOf(room) {
  const now = Date.now(), list = [];
  room.sockets.forEach(ws => {
    if (ws.stadium) return;
    const id = ws.user ? ws.user.id : ws.device, t = room.beats.get(id);
    list.push({ app: ws.app, seat: typeof ws.seat === "number" ? ws.seat : 0, id: String(ws.device).slice(0, 8), playing: !!(t && now - t <= BEAT_SECONDS * 1000) });
  });
  list.sort((a, b) => a.app < b.app ? -1 : a.app > b.app ? 1 : a.seat - b.seat || (a.id < b.id ? -1 : 1));
  return list;
}
function rosterUp(room, force) {
  const list = rosterOf(room), sig = JSON.stringify(list);
  if (!force && sig === room.rosterSig) return;
  room.rosterSig = sig;
  const text = JSON.stringify({ type: "roster", app: "roster", phones: list, now: Date.now() });
  room.sockets.forEach(ws => { if (ws.readyState === 1 && !ws.stadium) ws.send(text) });
}

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

/* ---------- stadium ---------- */
/* BLUE PHONE: v7 stadium state per room. The seat math lives in rainbow-run-stadium-v1-core.js; the server only
   counts seats to hold a layout under the cap. */
function stadiumOf(room) {
  if (!room.st) room.st = { layout: null, v: 0, cue: null, plan: null, phones: 0, hosts: new Set(), countSig: "", loaded: false };
  return room.st;
}
async function stadiumLoad(room) {
  const st = stadiumOf(room);
  if (st.loaded) return st;
  st.loaded = true;
  const saved = await load("stadium:" + room.id);
  if (saved && typeof saved === "object") { st.layout = saved.layout || null; st.v = saved.v || 0; st.cue = saved.cue || null; st.plan = saved.plan || null; }
  return st;
}
function stadiumSave(room) { const st = stadiumOf(room); save("stadium:" + room.id, { layout: st.layout, v: st.v, cue: st.cue, plan: st.plan }); }
const cleanSeatId = x => String(x || "").toUpperCase().replace(/[^A-Z0-9-]/g, "").slice(0, 16);
/* Cleans a seating plan from a host. */
function cleanPlan(P) {
  if (!P || typeof P !== "object") return null;
  const seen = new Set(), assigned = [];
  (Array.isArray(P.assigned) ? P.assigned : []).slice(0, MAX_STADIUM_PHONES).forEach(x => { const id = cleanSeatId(x); if (id && !seen.has(id)) { seen.add(id); assigned.push(id); } });
  return { guests: Math.round(num(P.guests, 0, MAX_STADIUM_PHONES, 0)), pattern: String(P.pattern || "pack").slice(0, 16),
    shape: String(P.shape || "heart").slice(0, 16), text: String(P.text || "").slice(0, 24), fit: P.fit === "guests" ? "guests" : "venue", assigned: assigned };
}
function sendPlan(st) { st.hosts.forEach(h => sendTo(h, { type: "stadium-plan", app: h.app, plan: st.plan })); }
const num = (v, lo, hi, d) => { v = +v; return Number.isFinite(v) ? Math.min(hi, Math.max(lo, v)) : d; };
/* Cleans a layout from a host and counts its seats. Returns null when it breaks the rules. */
function cleanLayout(L) {
  if (!L || typeof L !== "object" || !Array.isArray(L.sections) || L.sections.length > 64) return null;
  let total = 0;
  const ids = new Set(), out = [];
  for (const s of L.sections) {
    if (!s || typeof s !== "object") return null;
    const id = String(s.id || "").toUpperCase().replace(/[^A-Z0-9]/g, "").slice(0, 4);
    if (!id || ids.has(id)) return null;
    ids.add(id);
    const kind = s.kind === "arc" ? "arc" : "block";
    const rows = Math.round(num(s.rows, 1, 200, 1)), seats = Math.round(num(s.seats, 1, 400, 1)), grow = Math.round(num(s.grow, -20, 40, 0));
    const rowSeats = Array.isArray(s.rowSeats) ? s.rowSeats.slice(0, rows).map(n => n === null || n === undefined || n === "" ? null : Math.round(num(n, 0, 400, 0))) : [];
    const off = Array.isArray(s.off) ? s.off.slice(0, 5000).map(x => String(x).replace(/[^0-9-]/g, "").slice(0, 9)).filter(x => /^\d+-\d+$/.test(x)) : [];
    const offSet = new Set(off);
    for (let r = 0; r < rows; r++) {
      const n = typeof rowSeats[r] === "number" ? rowSeats[r] : Math.max(1, seats + grow * r);
      for (let k = 0; k < n; k++) if (!offSet.has((r + 1) + "-" + (k + 1))) total++;
    }
    out.push({ id: id, kind: kind, rows: rows, seats: seats, grow: grow,
      x: num(s.x, -2000, 2000, 0), y: num(s.y, -2000, 2000, 0), rot: num(s.rot, -360, 360, 0),
      r: num(s.r, 1, 2000, 30), a0: num(s.a0, -720, 720, 0), span: num(s.span, 1, 360, 40),
      seatGap: num(s.seatGap, 0.3, 5, 0.6), rowGap: num(s.rowGap, 0.3, 5, 0.9), rowSeats: rowSeats, off: off });
  }
  if (total > MAX_STADIUM_PHONES) return null;
  return { name: String(L.name || "").slice(0, 40), sections: out, total: total };
}
function sizeOk(o) { try { return JSON.stringify(o).length <= MAX_STADIUM_JSON } catch (e) { return false } }
/* Sends one stadium message to every phone in the room, each stamped with its own app name. */
function broadcastStadium(room, msg) {
  const texts = {};
  room.sockets.forEach(ws => {
    if (ws.readyState !== 1) return;
    if (!texts[ws.app]) texts[ws.app] = JSON.stringify(Object.assign({}, msg, { app: ws.app }));
    ws.send(texts[ws.app]);
  });
}
function hostOk(code) {
  if (!HOST_CODE || typeof code !== "string") return false;
  const a = crypto.createHash("sha256").update(code).digest(), b = crypto.createHash("sha256").update(HOST_CODE).digest();
  return crypto.timingSafeEqual(a, b);
}
function stadiumMessage(room, ws, m) {
  const st = stadiumOf(room);
  if (m.type === "stadium-seat") {
    const seat = String(m.seat || "").toUpperCase().replace(/[^A-Z0-9-]/g, "").slice(0, 16);
    ws.stadiumSeat = seat || "";
    sendTo(ws, { type: "stadium-seated", app: ws.app, seat: ws.stadiumSeat });
    return true;
  }
  if (m.type === "stadium-host") {
    if (ws.host) { sendTo(ws, { type: "stadium-host", app: ws.app, ok: true }); return true; }
    if (!HOST_CODE) { sendTo(ws, { type: "stadium-host", app: ws.app, ok: false, reason: "no host code is set on the server" }); return true; }
    if (!hostOk(m.code)) {
      ws.hostFails = (ws.hostFails || 0) + 1;
      sendTo(ws, { type: "stadium-host", app: ws.app, ok: false, reason: "wrong host code" });
      if (ws.hostFails >= 5) ws.close(1008, "too many host code tries");
      return true;
    }
    ws.host = true; st.hosts.add(ws); st.countSig = "";
    sendTo(ws, { type: "stadium-host", app: ws.app, ok: true });
    sendTo(ws, { type: "stadium-layout", app: ws.app, layout: st.layout, v: st.v });
    if (st.cue) sendTo(ws, { type: "stadium-cue", app: ws.app, cue: st.cue });
    sendTo(ws, { type: "stadium-plan", app: ws.app, plan: st.plan });
    return true;
  }
  if (m.type === "stadium-plan" || m.type === "stadium-assign") {
    if (!ws.host) return true;
    if (m.type === "stadium-plan") {
      const P = cleanPlan(m.plan);
      if (!P) return true;
      st.plan = P; stadiumSave(room); sendPlan(st);
      return true;
    }
    const id = cleanSeatId(m.seat);
    if (!st.plan) st.plan = cleanPlan({});
    const ok = !!id && st.plan.assigned.indexOf(id) < 0 && st.plan.assigned.length < MAX_STADIUM_PHONES;
    if (ok) { st.plan.assigned.push(id); stadiumSave(room); }
    sendTo(ws, { type: "stadium-assigned", app: ws.app, seat: id, ok: ok });
    if (ok) sendPlan(st);
    return true;
  }
  if (m.type === "stadium-layout" || m.type === "stadium-cue" || m.type === "stadium-sound") {
    if (!ws.host) return true;
    if (m.type === "stadium-layout") {
      const L = cleanLayout(m.layout);
      if (!L) { sendTo(ws, { type: "stadium-host", app: ws.app, ok: true, reason: "layout refused, over the seat limit or malformed" }); return true; }
      st.layout = L; st.v++;
      stadiumSave(room); broadcastStadium(room, { type: "stadium-layout", layout: L, v: st.v });
      return true;
    }
    const body = m.type === "stadium-cue" ? m.cue : m.sound;
    if (!body || typeof body !== "object" || !sizeOk(body)) return true;
    /* A cue can keep the start time it already had, so refitting the show to new guests does not restart it. */
    const at = num(body.at, Date.now() - 6 * 3600000, Date.now() + 60000, Date.now() + 600);
    if (m.type === "stadium-cue") {
      if (CUE_TYPES.indexOf(body.type) < 0) return true;
      st.cue = Object.assign({}, body, { at: at });
      stadiumSave(room); broadcastStadium(room, { type: "stadium-cue", cue: st.cue });
    } else {
      broadcastStadium(room, { type: "stadium-sound", sound: Object.assign({}, body, { at: at }) });
    }
    return true;
  }
  return false;
}
/* Every two seconds hosts get how many seat phones are in and which seats they hold, while that changes. */
function stadiumCount(room) {
  const st = room.st;
  if (!st || !st.hosts.size) return;
  const seats = [];
  let phones = 0;
  room.sockets.forEach(ws => { if (ws.stadium) { phones++; if (ws.stadiumSeat) seats.push(ws.stadiumSeat); } });
  seats.sort();
  const sig = phones + "|" + seats.join(",");
  if (sig === st.countSig) return;
  st.countSig = sig;
  st.hosts.forEach(ws => sendTo(ws, { type: "stadium-count", app: ws.app, phones: phones, seats: seats }));
}

/* ---------- heartbeat gate ---------- */
function startLoop(room, now) {
  const s = room.loop;
  s.playing = true; s.idle = false; s.epoch = now + 300; s.barOffset = 0; s.pending = null;
  room.lastStart = now;
}
/* BLUE PHONE: added heartbeat gate. One key per active player, in memory and in Redis with a 20 second expiry. */
function beat(room, ws) {
  const id = ws.user ? ws.user.id : ws.device, now = Date.now();
  const fresh = !room.beats.has(id);
  room.beats.set(id, now);
  if (fresh && !ws.stadium) rosterUp(room);
  if (redis && !ws.stadium) redis.set(PREFIX + "beat:" + room.id + ":" + id, "1", "EX", BEAT_SECONDS).catch(() => {});
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
    if (GROOVE_TEMPO[m.groove]) next.tempo = Math.min(TEMPO_MAX, Math.max(TEMPO_MIN, GROOVE_TEMPO[m.groove]));
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
  if (typeof m.type === "string" && m.type.indexOf("stadium-") === 0) { stadiumMessage(room, ws, m); return }
  const knewSchema = typeof ws.schema === "number";
  if (typeof m.schema === "number") ws.schema = m.schema;
  if (!knewSchema && typeof ws.schema === "number" && !ws.stadium) seatUp(room, ws.app);
  m.app = ws.app; m.device = ws.device;
  const board = room.boards[ws.app];
  /* Stadium seats only listen. They send seats, clock pings and nothing that changes the band. */
  if (ws.stadium) return;

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
  const st = room.st;
  if (st && (ws.stadium || st.layout)) {
    sendTo(ws, { type: "stadium-layout", app: ws.app, layout: st.layout, v: st.v });
    if (st.cue) sendTo(ws, { type: "stadium-cue", app: ws.app, cue: st.cue });
  }
  if (ws.stadium) return;
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
    res.end(JSON.stringify({ ok: true, version: 7, stadium: HOST_CODE ? "host code set" : "no host code", rooms: rooms.size, phones: phones, redis: redis ? redis.status : "off", login: AUTH_ON ? "on" : "off" }));
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
  ws.stadium = q.get("mode") === "stadium";
  /* The host screen connects with mode=stadium-host. It is a normal band phone that also loads the stadium state. */
  ws.stadiumHostPage = q.get("mode") === "stadium-host";
  if (!app || !name || !ws.device) { ws.close(1008, "app, room and device are required"); return }

  /* BLUE PHONE: a phone enters its room at once while login is off, or after its token checks out while login is on.
     Until then only clock pings and the login message are answered. Other messages wait in a short queue. */
  const early = []; let room = null, entering = false;
  async function enter() {
    if (room || entering) return;
    entering = true;
    const loaded = await getRoom(name);
    await getBoard(loaded, app);
    if (ws.stadium || ws.stadiumHostPage || loaded.st) await stadiumLoad(loaded);
    if (ws.readyState !== 1) return;
    /* BLUE PHONE: v7 a room with stadium seats holds up to 2500 phones, band phones included */
    const st = loaded.st;
    /* 2500 seat phones, plus the usual band phones and host screens on top */
    const seatPhones = st ? st.phones : 0, others = loaded.sockets.size - seatPhones;
    if (ws.stadium ? seatPhones >= MAX_STADIUM_PHONES : others >= MAX_PHONES_PER_ROOM && !(st && st.phones > 0 && others < MAX_PHONES_PER_ROOM * 2)) { ws.close(1013, "room full"); return }
    if (ws.stadium) stadiumOf(loaded).phones++;
    loaded.sockets.add(ws);
    room = loaded;
    early.forEach(text => onMessage(room, ws, text));
    early.length = 0;
    welcome(room, ws);
    if (!ws.stadium) { seatUp(room, app); rosterUp(room, true); }
  }
  ws.on("close", () => {
    if (!room) return;
    room.sockets.delete(ws);
    if (room.st) { if (ws.stadium) room.st.phones--; room.st.hosts.delete(ws); }
    if (!ws.stadium) { seatUp(room, ws.app); rosterUp(room); }
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
  /* Stadium seats only watch and listen, so they join without a login. The host is locked by its code. */
  if (!AUTH_ON || ws.stadium) enter();
});

/* Every few seconds: run the heartbeat gate, and close phones whose login is missing or ran out. */
const countTimer = setInterval(() => { rooms.forEach(stadiumCount) }, 2000);
const gateTimer = setInterval(() => {
  const now = Date.now();
  rooms.forEach(room => {
    gate(room, now);
    rosterUp(room);
    if (redis && !room.sockets.size && !room.loop.playing) rooms.delete(room.id);
  });
  if (!AUTH_ON) return;
  wss.clients.forEach(ws => {
    if (ws.stadium) return;
    if (!ws.user) { if (now - ws.opened > AUTH_WAIT_MS) ws.close(1008, "sign in") }
    else if (now > ws.user.exp + 30000) ws.close(1008, "login expired");
  });
}, 5000);

/* Drop phones that stopped answering. */
const sweep = setInterval(() => {
  wss.clients.forEach(ws => { if (!ws.alive) { ws.terminate(); return } ws.alive = false; try { ws.ping() } catch (e) {} });
}, 25000);

server.listen(PORT, () => console.log("stompbox-lab server on port " + PORT + ", redis " + (redis ? "on" : "off") + ", login " + (AUTH_ON ? "on" : "off")));
process.on("SIGTERM", () => { clearInterval(sweep); clearInterval(gateTimer); clearInterval(countTimer); wss.clients.forEach(ws => ws.close(1001)); server.close(() => process.exit(0)); setTimeout(() => process.exit(0), 2000) });
