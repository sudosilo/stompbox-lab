/* stompbox-lab-v2-clock.js
   Server clock for phones. Shared by every app that plays in time with other phones.

   ORANGE PHONE: stage one, clock sync.

   The phone pings the server in a short burst and notes the round trip of each ping.
   The offset from the fastest round trip is the one that gets used, because it carries
   the least network noise. A shorter burst repeats every 30 seconds to correct drift.

   The app supplies send(message), which sends {type:"ping", t0}.
   The app calls pong(message) with every {type:"pong", t0, t1} from the server.
   now() is the server clock in milliseconds. */
(function(){
"use strict";

/* A local clock that never jumps when the phone corrects its wall clock. */
function local(){
  if(window.performance&&performance.timeOrigin)return performance.timeOrigin+performance.now();
  return Date.now();
}

window.createServerClock=function(o){
  let offset=0,rtt=0,synced=false;
  let left=0,bestRtt=Infinity,bestOffset=0,stepTimer=null,repeatTimer=null;

  function adopt(){
    if(bestRtt===Infinity)return;
    offset=bestOffset;rtt=bestRtt;synced=true;
    if(o.onChange)o.onChange(offset,rtt);
  }
  function step(){
    stepTimer=null;
    if(left<=0){adopt();return}
    left--;
    o.send({type:"ping",t0:local()});
    stepTimer=setTimeout(step,150);
  }
  function burst(count){
    clearTimeout(stepTimer);
    left=count;bestRtt=Infinity;
    step();
  }

  return {
    start:function(){
      clearInterval(repeatTimer);
      synced=false;
      burst(8);
      repeatTimer=setInterval(function(){burst(4)},30000);
    },
    stop:function(){clearInterval(repeatTimer);clearTimeout(stepTimer);left=0;synced=false;if(o.onChange)o.onChange(offset,rtt)},
    pong:function(m){
      const t3=local(),trip=t3-m.t0;
      if(!(trip>=0)||typeof m.t1!=="number")return;
      if(trip<bestRtt){
        bestRtt=trip;bestOffset=m.t1+trip/2-t3;
        /* The first burst is used as it improves, so a phone that just joined locks on fast. */
        if(!synced)adopt();
      }
    },
    now:function(){return local()+offset},
    get offset(){return offset},
    get rtt(){return rtt},
    get synced(){return synced}
  };
};
})();
