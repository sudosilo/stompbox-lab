/* stompbox-lab-v2-sync.js
   Board sync client. Shared by every app that syncs a board between phones.
   The app supplies exportState, loadState and applyChange. This file owns the
   device id, the room code, the socket and the message format.

   Sync stays off while the url is blank. Nothing is sent and nothing is opened.

   Message format, every message is one JSON object:
     type    "hello" | "change" | "snapshot" | "request"
     app     app name, phones only listen to their own app
     schema  snapshot schema version of the sender
     room    room code shared by the phones on one board
     device  id of the sending phone
     seq     counter per device, rises with every message
     ts      sender clock in milliseconds
     path    change only, for example "wah.sweep" or "patch"
     value   change only, the new value at that path
     state   snapshot only, the whole board from exportState
   A "request" asks this phone to answer with a snapshot of its board.

   ORANGE PHONE: v2 adds on() and send() so other modules share this one socket.
   The clock uses "ping" and "pong". The loop uses "transport" and "loop". */
(function(){
"use strict";

function store(key,value){
  try{
    if(value===undefined)return localStorage.getItem(key);
    localStorage.setItem(key,value);
  }catch(e){}
  return null;
}

function makeId(){
  if(window.crypto&&crypto.randomUUID)return crypto.randomUUID();
  let s="";for(let i=0;i<32;i++)s+=Math.floor(Math.random()*16).toString(16);
  return s;
}

function cleanRoom(r){return String(r||"").trim().toLowerCase().replace(/[^a-z0-9]/g,"").slice(0,24)}

window.createBoardSync=function(o){
  const app=o.app,schema=o.schema,url=o.url||"";
  let device=store(app+"-device-id");
  if(!device){device=makeId();store(app+"-device-id",device)}
  let room=cleanRoom(store(app+"-room"));
  let ws=null,retry=0,retryTimer=null,flushTimer=null,seq=0,status="off";
  const pending=new Map();
  const handlers={};

  function setStatus(s){status=s;if(o.onStatus)o.onStatus(s,room)}
  function envelope(type,extra){return Object.assign({type:type,app:app,schema:schema,room:room,device:device,seq:++seq,ts:Date.now()},extra)}
  function send(msg){if(ws&&ws.readyState===1){ws.send(JSON.stringify(msg));return true}return false}

  function receive(text){
    let m;try{m=JSON.parse(text)}catch(e){return}
    if(!m||m.app!==app)return;
    if(handlers[m.type]){handlers[m.type](m);return}
    if(m.schema!==schema){setStatus("update needed");return}
    if(m.type==="snapshot"&&m.state)o.loadState(m.state);
    else if(m.type==="change"&&m.device!==device)o.applyChange(m.path,m.value);
    else if(m.type==="request")send(envelope("snapshot",{state:o.exportState()}));
  }

  function connect(){
    clearTimeout(retryTimer);
    if(ws){const old=ws;ws=null;old.close()}
    if(!url){setStatus("off");return}
    if(!room){setStatus("no room");return}
    setStatus("connecting");
    const q="app="+encodeURIComponent(app)+"&room="+encodeURIComponent(room)+"&device="+encodeURIComponent(device);
    const sock=new WebSocket(url+(url.indexOf("?")<0?"?":"&")+q);
    ws=sock;
    sock.onopen=function(){if(ws!==sock)return;retry=0;setStatus("live");send(envelope("hello"))};
    sock.onmessage=function(e){if(ws===sock)receive(e.data)};
    sock.onerror=function(){sock.close()};
    sock.onclose=function(){
      if(ws!==sock)return;
      ws=null;setStatus("reconnecting");
      retryTimer=setTimeout(connect,Math.min(15000,500*Math.pow(2,retry++)));
    };
  }

  function flush(){
    flushTimer=null;
    pending.forEach(function(value,path){send(envelope("change",{path:path,value:value}))});
    pending.clear();
  }

  return {
    /* The app calls this after every local change. Rapid changes to one path are merged. */
    change:function(path,value){
      if(!url)return;
      pending.set(path,value);
      if(!flushTimer)flushTimer=setTimeout(flush,40);
    },
    /* The app calls this after a change that touches the whole board, such as a preset. */
    snapshot:function(){if(url)send(envelope("snapshot",{state:o.exportState()}))},
    /* Another module listens for one message type from the server. */
    on:function(type,fn){handlers[type]=fn},
    /* Another module sends its own message type over the same socket. */
    send:function(type,extra){return send(envelope(type,extra))},
    setRoom:function(r){room=cleanRoom(r);store(app+"-room",room);connect();return room},
    connect:connect,
    get room(){return room},
    get device(){return device},
    get status(){return status}
  };
};
})();
