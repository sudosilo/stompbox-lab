/* stompbox-lab-v2-loop.js
   Loop engine for phones. Parses patterns, counts bars from the server clock, and
   schedules notes with Web Audio. The server only sends data, never code.

   ORANGE PHONE: stage two, pattern parser, bar timeline, scheduler, slot voices.

   Pattern notation, one bar per cycle:
     bd sd hh        steps share the bar evenly
     ~               a rest
     [bd bd]         a group squeezed into one step
     hh*4            a step repeated inside its own space
     bd@3            a step that takes the space of three
     <c2 e2>         one choice per bar, taking turns
     a, b            layers that play at the same time
   Drum names: bd sd hh oh cp ck cl. Notes: c2, eb3, f#4, or a MIDI number.
   Every parsed step holds note, vel and pos, the same fields MIDI uses. */
(function(){
"use strict";

const DRUM={bd:36,sd:38,hh:42,oh:46,cp:39,ck:76,cl:77};
const SEMI={c:0,d:2,e:4,f:5,g:7,a:9,b:11};
const TOKENS=/[\[\]<>,~]|[*@]\d+(?:\.\d+)?|[^\s\[\]<>,~*@]+/g;

function tokenToNote(tok){
  if(Object.prototype.hasOwnProperty.call(DRUM,tok))return {note:DRUM[tok],drum:tok};
  const m=/^([a-g])(#|b)?(-?\d)$/.exec(tok);
  if(m)return {note:SEMI[m[1]]+(m[2]==="#"?1:m[2]==="b"?-1:0)+(+m[3]+1)*12,drum:null};
  if(/^\d{1,3}$/.test(tok)&&+tok<128)return {note:+tok,drum:null};
  return null;
}

function parse(src){
  const toks=String(src||"").toLowerCase().match(TOKENS)||[];
  let i=0;
  function stack(close){
    const layers=[seq(close)];
    while(toks[i]===","){i++;layers.push(seq(close))}
    return layers.length===1?layers[0]:{t:"stack",c:layers,w:1};
  }
  function seq(close){
    const c=[];
    while(i<toks.length&&toks[i]!==","&&toks[i]!==close){
      if(toks[i]==="]"||toks[i]===">"){i++;continue}
      c.push(step());
    }
    return {t:"seq",c:c,w:1};
  }
  function step(){
    const tk=toks[i++];let n;
    if(tk==="["){n=stack("]");if(toks[i]==="]")i++}
    else if(tk==="<"){const s=seq(">");if(toks[i]===">")i++;n={t:"alt",c:s.c,w:1}}
    else{const v=tokenToNote(tk);n=v?{t:"note",v:v,w:1}:{t:"rest",w:1}}
    while(i<toks.length&&(toks[i][0]==="*"||toks[i][0]==="@")){
      const num=+toks[i].slice(1);
      if(toks[i][0]==="*"){if(num>=1&&num<=32)n={t:"fast",n:Math.round(num),c:n,w:n.w}}
      else if(num>0&&num<=16)n.w=num;
      i++;
    }
    return n;
  }
  return stack(null);
}

function walk(n,cyc,start,dur,out){
  if(n.t==="note")out.push({pos:start,dur:dur,note:n.v.note,drum:n.v.drum,vel:100});
  else if(n.t==="seq"){
    let total=0;for(let k=0;k<n.c.length;k++)total+=n.c[k].w;
    let p=start;
    for(let k=0;k<n.c.length;k++){const d=dur*n.c[k].w/total;walk(n.c[k],cyc,p,d,out);p+=d}
  }
  else if(n.t==="stack"){for(let k=0;k<n.c.length;k++)walk(n.c[k],cyc,start,dur,out)}
  else if(n.t==="alt"){if(n.c.length)walk(n.c[((cyc%n.c.length)+n.c.length)%n.c.length],cyc,start,dur,out)}
  else if(n.t==="fast"){const d=dur/n.n;for(let k=0;k<n.n;k++)walk(n.c,cyc,start+k*d,d,out)}
}

const trees=new Map();
/* The steps of one bar. pos and dur are fractions of the bar. */
function events(src,bar){
  let tree=trees.get(src);
  if(!tree){if(trees.size>200)trees.clear();tree=parse(src);trees.set(src,tree)}
  const out=[];walk(tree,bar,0,1,out);
  return out;
}

/* ---------- bar timeline, the same sums the server does ---------- */
const barMs=tempo=>240000/tempo;
function pendingAt(s){return s.epoch+(s.pending.atBar-s.barOffset)*barMs(s.tempo)}
function barTime(s,bar){
  if(s.pending&&bar>=s.pending.atBar)return pendingAt(s)+(bar-s.pending.atBar)*barMs(s.pending.tempo);
  return s.epoch+(bar-s.barOffset)*barMs(s.tempo);
}
function barAt(s,t){
  if(s.pending){const at=pendingAt(s);if(t>=at)return s.pending.atBar+Math.floor((t-at)/barMs(s.pending.tempo))}
  return s.barOffset+Math.floor((t-s.epoch)/barMs(s.tempo));
}
const partOf=(s,bar)=>s.pending&&bar>=s.pending.atBar?s.pending:s;

window.StompboxPattern={parse:parse,events:events,barTime:barTime,barAt:barAt};

/* ---------- engine ----------
   o.clock     server clock from stompbox-lab-v2-clock.js
   o.ctx()     the AudioContext, or nothing before the first tap
   o.chain()   input of the pedal chain, lead and pad go through the pedals
   o.bus()     input after the pedals, drums and bass stay clean
   o.patches   the sound table of the app, o.patch() is the name in use
   o.onPos(bar,beat,tempo)  called while the loop plays */
window.createLoop=function(o){
  const SLOTS=["drums","bass","lead","pad"];
  let state=null,armed=false,slot="all",nudge=0,cursor=0,timer=null,noise=null;

  const hz=m=>440*Math.pow(2,(m-69)/12);
  function noiseBuf(ctx){
    if(noise&&noise.sampleRate===ctx.sampleRate)return noise;
    noise=ctx.createBuffer(1,ctx.sampleRate,ctx.sampleRate);
    const d=noise.getChannelData(0);for(let i=0;i<d.length;i++)d[i]=Math.random()*2-1;
    return noise;
  }
  function hit(ctx,dest,at,freqFrom,freqTo,sweep,level,decay,type){
    const osc=ctx.createOscillator(),g=ctx.createGain();
    osc.type=type||"sine";
    osc.frequency.setValueAtTime(freqFrom,at);
    if(freqTo!==freqFrom)osc.frequency.exponentialRampToValueAtTime(freqTo,at+sweep);
    g.gain.setValueAtTime(level,at);g.gain.exponentialRampToValueAtTime(.001,at+decay);
    osc.connect(g);g.connect(dest);osc.start(at);osc.stop(at+decay+.02);
  }
  function hiss(ctx,dest,at,type,freq,q,level,decay){
    const src=ctx.createBufferSource(),f=ctx.createBiquadFilter(),g=ctx.createGain();
    src.buffer=noiseBuf(ctx);f.type=type;f.frequency.value=freq;f.Q.value=q;
    g.gain.setValueAtTime(level,at);g.gain.exponentialRampToValueAtTime(.001,at+decay);
    src.connect(f);f.connect(g);g.connect(dest);
    src.start(at,Math.random()*.5);src.stop(at+decay+.02);
  }
  function drum(ctx,dest,name,at,vel){
    const v=vel/100;
    if(name==="bd")hit(ctx,dest,at,150,45,.11,.95*v,.3);
    else if(name==="sd"){hiss(ctx,dest,at,"highpass",1400,.7,.5*v,.17);hit(ctx,dest,at,200,160,.05,.35*v,.09,"triangle")}
    else if(name==="hh")hiss(ctx,dest,at,"highpass",7500,.7,.22*v,.045);
    else if(name==="oh")hiss(ctx,dest,at,"highpass",6500,.7,.22*v,.24);
    else if(name==="cp")hiss(ctx,dest,at,"bandpass",1500,1.2,.6*v,.13);
    else if(name==="ck")hit(ctx,dest,at,1600,1600,0,.4*v,.04);
    else if(name==="cl")hit(ctx,dest,at,1000,1000,0,.28*v,.04);
  }
  function bass(ctx,dest,m,at,len,vel){
    const osc=ctx.createOscillator(),f=ctx.createBiquadFilter(),g=ctx.createGain(),end=at+len;
    osc.type="sawtooth";osc.frequency.value=hz(m);
    f.type="lowpass";f.Q.value=3;f.frequency.setValueAtTime(900,at);f.frequency.setTargetAtTime(260,at,.09);
    g.gain.setValueAtTime(0,at);g.gain.linearRampToValueAtTime(.42*vel/100,at+.006);
    g.gain.setTargetAtTime(0,end,.03);
    osc.connect(f);f.connect(g);g.connect(dest);osc.start(at);osc.stop(end+.25);
  }
  function lead(ctx,dest,m,at,len,vel){
    const P=o.patches[o.patch()];if(!P)return;
    const amp=ctx.createGain(),f=ctx.createBiquadFilter(),end=at+len,peak=.17*vel/100;
    f.type="lowpass";f.Q.value=1.5;
    f.frequency.setValueAtTime(P.f0,at);f.frequency.setTargetAtTime(P.f1,at,P.fd);
    amp.gain.setValueAtTime(0,at);amp.gain.linearRampToValueAtTime(peak,at+P.a);
    amp.gain.setTargetAtTime(peak*P.sus,at+P.a,P.dec/(P.sus?3:4));
    amp.gain.setTargetAtTime(0,end,P.rel/4);
    P.osc.forEach(function(spec){
      const osc=ctx.createOscillator(),g=ctx.createGain();
      osc.type=spec[0];osc.frequency.value=hz(m);osc.detune.value=spec[1];g.gain.value=spec[2]/P.osc.length*1.3;
      osc.connect(g);g.connect(f);osc.start(at);osc.stop(end+P.rel*2+.1);
    });
    f.connect(amp);amp.connect(dest);
  }
  function pad(ctx,dest,m,at,len,vel){
    const g=ctx.createGain(),end=at+len,rise=Math.min(.4,len/3);
    g.gain.setValueAtTime(0,at);g.gain.linearRampToValueAtTime(.07*vel/100,at+rise);
    g.gain.setTargetAtTime(0,end,.25);
    [-7,7].forEach(function(det){
      const osc=ctx.createOscillator();osc.type="triangle";osc.frequency.value=hz(m);osc.detune.value=det;
      osc.connect(g);osc.start(at);osc.stop(end+1.5);
    });
    g.connect(dest);
  }

  function play(ctx,id,ev,at,len){
    if(id==="drums"){if(ev.drum)drum(ctx,o.bus(),ev.drum,at,ev.vel)}
    else if(ev.drum)return;
    else if(id==="bass")bass(ctx,o.bus(),ev.note,at,len*.9,ev.vel);
    else if(id==="lead")lead(ctx,o.chain(),ev.note,at,len*.85,ev.vel);
    else if(id==="pad")pad(ctx,o.chain(),ev.note,at,len,ev.vel);
  }

  /* Runs many times a second. Schedules every step that begins inside the next short window. */
  function tick(){
    if(!state||!state.playing)return;
    const now=o.clock.now(),ctx=o.ctx();
    const bar=barAt(state,now),start=barTime(state,bar),len=barMs(partOf(state,bar).tempo);
    if(o.onPos)o.onPos(bar,now<start?0:Math.floor((now-start)/len*4),partOf(state,bar).tempo);
    if(!armed||!ctx||ctx.state!=="running"){cursor=0;return}
    const horizon=now+160;
    if(cursor<now-50||cursor>horizon)cursor=now;
    const late=(nudge-1000*(ctx.outputLatency||0))/1000;
    for(let b=Math.max(barAt(state,cursor),state.barOffset);barTime(state,b)<horizon;b++){
      const t0=barTime(state,b),part=partOf(state,b),ms=barMs(part.tempo);
      for(let k=0;k<SLOTS.length;k++){
        const id=SLOTS[k];
        if(slot!=="all"&&slot!==id)continue;
        const src=part.slots&&part.slots[id];
        if(typeof src!=="string"||src.length>400)continue;
        const evs=events(src,b);
        for(let e=0;e<evs.length;e++){
          const t=t0+evs[e].pos*ms;
          if(t<cursor||t>=horizon)continue;
          const at=Math.max(ctx.currentTime,ctx.currentTime+(t-now)/1000+late);
          play(ctx,id,evs[e],at,evs[e].dur*ms/1000);
        }
      }
    }
    cursor=horizon;
  }

  return {
    setState:function(s){
      state=s&&typeof s.tempo==="number"&&s.tempo>0&&typeof s.epoch==="number"?s:null;
      if(!timer)timer=setInterval(tick,25);
    },
    /* A phone only makes sound after a tap on that phone. */
    arm:function(on){armed=!!on},
    setSlot:function(id){slot=id},
    setNudge:function(ms){nudge=+ms||0},
    get state(){return state},
    get armed(){return armed},
    get slot(){return slot},
    get nudge(){return nudge}
  };
};
})();
