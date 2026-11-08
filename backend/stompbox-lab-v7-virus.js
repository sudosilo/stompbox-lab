/* stompbox-lab-v7-virus.js
   BLUE PHONE: virus mode on the band server. Phones on a floor or in the stands play a short motif in their own
   slot of a repeating cycle and report when they heard their own tick and their neighbors' ticks. From those
   reports this file works out the true distance between every pair that heard each other, spreads the virus one
   hop per cycle from the seed phones to phones within reach, and solves where every phone stands.

   Positions come in two layers. DV-Hop gives a first guess from hop counts: the seeds measure how many meters a
   hop is worth from the hops between themselves, and distances to several seeds pin a spot by trilateration.
   Then a spring pass pulls every phone toward the measured distances to its neighbors, with seeds and seat
   phones held still at their known spots. The phones only listen and report; all the math lives here. */
"use strict";
const CYCLE_MS = 4000, SLOTS = 32, IDS = 8, SOUND = 343, SELF_M = 0.08;
const INFECT_M = 3, MAX_M = 40, KEEP = 9;

/* A cycle has a slot and tick for each of SLOTS times IDS phones. A bigger crowd takes turns: phones are
   split into groups, and a group plays only in its own cycles, so no two phones playing in the same cycle
   ever sound alike. Listening never stops, and a pair can be measured across different cycles, because each
   phone's own times all sit on its own audio clock. */
const PER = SLOTS * IDS;
function fresh() { return { phones: new Map(), owner: new Map(), edges: new Map(), next: 0, done: -1, hopLen: 1.5, G: 1 }; }
function groups(vx) { return Math.max(1, Math.ceil(vx.next / PER)); }

/* a phone joins the game and gets its slot and tick identity */
function join(vx, device, seed) {
  let p = vx.phones.get(device);
  if (!p) {
    const i = vx.next++;
    p = { device: device, slot: i % SLOTS, id: Math.floor(i / SLOTS) % IDS, group: Math.floor(i / PER), hops: {}, pos: null, fixed: null, own: new Map(), heard: new Map() };
    vx.phones.set(device, p); vx.owner.set(p.group + ":" + p.slot + ":" + p.id, p); vx.G = groups(vx);
  }
  if (seed) { p.seed = seed.id; p.fixed = { x: seed.x, y: seed.y }; p.pos = { x: seed.x, y: seed.y }; p.hops[seed.id] = 0; }
  return p;
}
function report(vx, device, m) {
  const p = vx.phones.get(device); if (!p) return;
  const c = Math.round(+m.c); if (!Number.isFinite(c)) return;
  const own = Number.isFinite(+m.own) && m.own !== null ? +m.own : null;
  /* the phone sends its clearest hearings first; the nearest neighbors are all the map needs */
  const h = Array.isArray(m.h) ? m.h.slice(0, 24).filter(x => Array.isArray(x) && x.length >= 3 && [x[0], x[1], x[2]].every(Number.isFinite)) : [];
  if (own !== null) p.own.set(c, own);
  p.heard.set(c, h);
  const keep = vx.G + 3;
  for (const k of p.own.keys()) if (k < c - keep) p.own.delete(k);
  for (const k of p.heard.keys()) if (k < c - keep) p.heard.delete(k);
}
/* the phone that played a slot and tick in a cycle */
function playerOf(vx, slot, id, c) { return vx.owner.get((c % vx.G) + ":" + slot + ":" + id) || null; }
/* the latest cycle a phone played in, at or before c */
function lastPlay(vx, p, c) { const back = ((c - p.group) % vx.G + vx.G) % vx.G; return c - back; }
function key(a, b) { return a < b ? a + "|" + b : b + "|" + a; }
function median(a) { const s = a.slice().sort((x, y) => x - y); return s[Math.floor(s.length / 2)]; }

/* pairs every phone that heard a neighbor in cycle c with that neighbor's hearing of it */
function pairUp(vx, c) {
  let added = 0;
  vx.phones.forEach(a => {
    const ha = a.heard.get(c); if (!ha) return;
    ha.forEach(x => {
      const b = playerOf(vx, x[0], x[1], c); if (!b || b === a) return;
      const bOwn = b.own.get(c); if (bOwn === undefined) return;
      /* A's own tick from its latest turn, and B hearing that same tick */
      const ca = lastPlay(vx, a, c), aOwn = a.own.get(ca), hb = b.heard.get(ca);
      if (aOwn === undefined || !hb) return;
      const back = hb.find(y => y[0] === a.slot && y[1] === a.id && playerOf(vx, y[0], y[1], ca) === a); if (!back) return;
      const k = key(a.device, b.device);
      let e = vx.edges.get(k); if (!e) { e = { a: a.device, b: b.device, d: [], seen: "" }; vx.edges.set(k, e); }
      /* one measurement per pair of turns, whichever side finds it first */
      const turn = Math.min(ca, c) + ":" + Math.max(ca, c); if (e.seen === turn) return; e.seen = turn;
      const d = SOUND / 2 * ((x[2] - aOwn) - (bOwn - back[2])) + SELF_M;
      if (!(d > 0 && d < MAX_M)) return;
      e.d.push(d); if (e.d.length > KEEP) e.d.shift();
      e.m = median(e.d); e.c = c; added++;
    });
  });
  return added;
}

/* the infection moves out one hop per cycle, only across phones close enough to touch */
function spread(vx) {
  const old = new Map(); vx.phones.forEach(p => old.set(p.device, Object.assign({}, p.hops)));
  vx.edges.forEach(e => {
    if (e.m > INFECT_M) return;
    const pa = vx.phones.get(e.a), pb = vx.phones.get(e.b); if (!pa || !pb) return;
    [[pa, pb], [pb, pa]].forEach(([from, to]) => {
      const h = old.get(from.device);
      Object.keys(h).forEach(s => { const n = h[s] + 1; if (to.hops[s] === undefined || n < to.hops[s]) to.hops[s] = n; });
    });
  });
}

/* DV-Hop: meters per hop from seed to seed, then distances to seeds, then a least squares spot */
/* how many neighbors a phone has measured well; a well measured phone trusts its springs over the hop guess */
function anchorCount(vx) {
  vx.phones.forEach(p => { p.good = 0; });
  vx.edges.forEach(e => { if (e.d.length >= 3) { const a = vx.phones.get(e.a), b = vx.phones.get(e.b); if (a) a.good++; if (b) b.good++; } });
}
function dvhop(vx) {
  anchorCount(vx);
  const seeds = []; vx.phones.forEach(p => { if (p.seed && p.fixed) seeds.push(p); });
  let sum = 0, n = 0;
  for (let i = 0; i < seeds.length; i++) for (let j = 0; j < seeds.length; j++) {
    if (i === j) continue; const h = seeds[j].hops[seeds[i].seed];
    if (h > 0) { sum += Math.hypot(seeds[i].fixed.x - seeds[j].fixed.x, seeds[i].fixed.y - seeds[j].fixed.y); n += h; }
  }
  if (n) vx.hopLen = Math.max(0.4, Math.min(5, sum / n));
  vx.phones.forEach(p => {
    p.dv = null;
    if (p.fixed) return;
    const refs = seeds.filter(s => p.hops[s.seed] !== undefined).map(s => ({ x: s.fixed.x, y: s.fixed.y, r: p.hops[s.seed] * vx.hopLen }));
    if (refs.length < 3) return;
    let x = refs.reduce((a, r) => a + r.x, 0) / refs.length, y = refs.reduce((a, r) => a + r.y, 0) / refs.length;
    for (let it = 0; it < 60; it++) {
      let gx = 0, gy = 0;
      refs.forEach(r => { const dx = x - r.x, dy = y - r.y, d = Math.hypot(dx, dy) || 1e-6, e = d - r.r; gx += e * dx / d; gy += e * dy / d; });
      x -= 0.3 * gx / refs.length; y -= 0.3 * gy / refs.length;
    }
    p.dv = { x: x, y: y };
    /* a first spot, or a fresh start for a phone the springs folded onto the wrong side */
    if (!p.pos || (p.good < 3 && Math.hypot(p.pos.x - x, p.pos.y - y) > 4 * vx.hopLen)) p.pos = { x: x, y: y };
  });
}

/* springs: every measured pair pulls toward its measured distance; known spots never move */
function relax(vx) {
  vx.phones.forEach(p => { if (p.fixed) p.pos = { x: p.fixed.x, y: p.fixed.y }; });
  /* a phone with no guess yet starts beside a placed neighbor it measured */
  for (let pass = 0; pass < 3; pass++) vx.edges.forEach(e => {
    const a = vx.phones.get(e.a), b = vx.phones.get(e.b); if (!a || !b) return;
    if (!e.m) return;
    if (a.pos && !b.pos) b.pos = { x: a.pos.x + e.m * Math.cos(b.slot), y: a.pos.y + e.m * Math.sin(b.slot) };
    else if (b.pos && !a.pos) a.pos = { x: b.pos.x + e.m * Math.cos(a.slot), y: b.pos.y + e.m * Math.sin(a.slot) };
  });
  /* the springs run on flat number arrays, so a full crowd settles in a few milliseconds a pass */
  const ps = [], idx = new Map();
  vx.phones.forEach(p => { if (p.pos) { idx.set(p.device, ps.length); ps.push(p); } });
  const n = ps.length, X = new Float64Array(n), Y = new Float64Array(n), FX = new Float64Array(n), FY = new Float64Array(n), C = new Float64Array(n);
  const fixed = new Uint8Array(n), DX = new Float64Array(n), DY = new Float64Array(n), PW = new Float64Array(n);
  ps.forEach((p, i) => { X[i] = p.pos.x; Y[i] = p.pos.y; fixed[i] = p.fixed ? 1 : 0; if (p.dv) { DX[i] = p.dv.x; DY[i] = p.dv.y; PW[i] = 0.03 / (1 + p.good * p.good); } });
  const EA = [], EB = [], EM = [], EW = [];
  vx.edges.forEach(e => { if (!e.d.length) return; const i = idx.get(e.a), j = idx.get(e.b); if (i === undefined || j === undefined) return; EA.push(i); EB.push(j); EM.push(e.m); EW.push(Math.min(1, e.d.length / 3) * 0.5); });
  const m = EA.length, passes = m > 40000 ? 20 : 30;
  for (let it = 0; it < passes; it++) {
    FX.fill(0); FY.fill(0); C.fill(0);
    for (let k = 0; k < m; k++) {
      const i = EA[k], j = EB[k], dx = X[j] - X[i], dy = Y[j] - Y[i], d = Math.sqrt(dx * dx + dy * dy) || 1e-6, err = (d - EM[k]) / d * EW[k];
      /* a pair heard many times pulls harder than a single hearing, and a known spot never moves */
      const fi = fixed[i] ? 0 : fixed[j] ? 1 : 0.5, fj = fixed[j] ? 0 : fixed[i] ? 1 : 0.5;
      FX[i] += fi * err * dx; FY[i] += fi * err * dy; C[i]++;
      FX[j] -= fj * err * dx; FY[j] -= fj * err * dy; C[j]++;
    }
    for (let i = 0; i < n; i++) {
      if (fixed[i]) continue;
      if (C[i]) { X[i] += FX[i] / Math.sqrt(C[i]); Y[i] += FY[i] / Math.sqrt(C[i]); }
      /* a light pull toward the hop count guess keeps the whole map from folding over */
      if (PW[i]) { X[i] += PW[i] * (DX[i] - X[i]); Y[i] += PW[i] * (DY[i] - Y[i]); }
    }
  }
  ps.forEach((p, i) => { if (!p.fixed) p.pos = { x: X[i], y: Y[i] }; });
}

/* runs once a cycle has had time for every phone to report; returns true when it worked a new cycle */
function step(vx, cueAt, now) {
  const c = Math.floor((now - cueAt - 1500) / CYCLE_MS) - 1;
  if (c < 0 || c <= vx.done) return false;
  vx.done = c;
  pairUp(vx, c);
  /* edges nobody has heard for a while fade out, so moving people do not drag old distances along */
  vx.edges.forEach((e, k) => { if (c - e.c > 8 + 2 * vx.G) vx.edges.delete(k); });
  spread(vx); dvhop(vx); relax(vx);
  return true;
}
function strainOf(p) { let best = null, bh = 1e9; Object.keys(p.hops).forEach(s => { if (p.hops[s] < bh) { bh = p.hops[s]; best = s; } }); return best === null ? null : { seed: best, hops: bh }; }

function plays(vx, p, c) { return ((c % vx.G) + vx.G) % vx.G === p.group; }
module.exports = { plays: plays, PER: PER, CYCLE_MS: CYCLE_MS, SLOTS: SLOTS, IDS: IDS, INFECT_M: INFECT_M, fresh: fresh, join: join, report: report, step: step, strainOf: strainOf, pairUp: pairUp, spread: spread, dvhop: dvhop, relax: relax };
