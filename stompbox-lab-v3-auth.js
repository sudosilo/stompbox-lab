/* stompbox-lab-v3-auth.js
   Supabase login for phones. Shared by every app that signs players in.

   ORANGE PHONE: stage three, email and password login, token refresh.

   Talks to the Supabase auth address directly with fetch. The key used here is the
   public one that is safe inside a web page. The session is stored on the phone.
   While url or key is blank, login is off and on is false. */
(function(){
"use strict";
window.createAuth=function(o){
  const on=!!(o.url&&o.key),KEY=o.app+"-session";
  const base=String(o.url||"").replace(/\/+$/,"")+"/auth/v1/";
  let s=null,timer=null;
  try{s=JSON.parse(localStorage.getItem(KEY))}catch(e){}
  if(!s||!s.access_token||!s.refresh_token)s=null;

  async function call(path,body,bearer){
    const headers={apikey:o.key,"content-type":"application/json"};
    if(bearer)headers.authorization="Bearer "+bearer;
    const r=await fetch(base+path,{method:"POST",headers:headers,body:JSON.stringify(body||{})});
    const j=await r.json().catch(function(){return {}});
    if(!r.ok){const e=new Error(j.msg||j.error_description||j.message||"request failed");e.refused=true;throw e}
    return j;
  }
  function schedule(){
    clearTimeout(timer);
    if(s)timer=setTimeout(refresh,Math.max(5000,s.expires_at-Date.now()-120000));
  }
  function setSession(j){
    s=j&&j.access_token?{access_token:j.access_token,refresh_token:j.refresh_token,
      expires_at:Date.now()+(j.expires_in||3600)*1000,email:(j.user&&j.user.email)||(s&&s.email)||""}:null;
    try{if(s)localStorage.setItem(KEY,JSON.stringify(s));else localStorage.removeItem(KEY)}catch(e){}
    schedule();
    if(o.onChange)o.onChange();
  }
  /* Trades the long lived refresh token for a fresh access token before the old one runs out. */
  async function refresh(){
    if(!s)return;
    try{setSession(await call("token?grant_type=refresh_token",{refresh_token:s.refresh_token}))}
    catch(e){if(e.refused)setSession(null);else{clearTimeout(timer);timer=setTimeout(refresh,30000)}}
  }

  if(on&&s){if(s.expires_at-Date.now()<120000)refresh();else schedule()}

  return {
    on:on,
    signIn:async function(email,password){setSession(await call("token?grant_type=password",{email:email,password:password}))},
    /* Returns "in" when the account is ready, or "confirm" when Supabase sent a confirmation email first. */
    signUp:async function(email,password){
      const j=await call("signup",{email:email,password:password});
      if(j.access_token){setSession(j);return "in"}
      return "confirm";
    },
    signOut:function(){if(s)call("logout",{},s.access_token).catch(function(){});setSession(null)},
    refresh:refresh,
    get token(){return on&&s&&s.expires_at>Date.now()?s.access_token:null},
    get email(){return s?s.email:""}
  };
};
})();
