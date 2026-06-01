import json,threading,time,datetime,websocket
from collections import defaultdict

class FlattradeWebSocket:
    def __init__(self,token,client_id,on_tick=None,on_candle_close=None,on_vwap_update=None):
        self.token=token;self.client_id=client_id
        self.on_tick=on_tick;self.on_candle_close=on_candle_close;self.on_vwap_update=on_vwap_update
        self.ws_url="wss://piconnect.flattrade.in/PiConnectWSAPI/"
        self.ws=None;self.subscribed=set()
        self.was_ever_connected=False;self.token_expired=False
        self.lock=threading.Lock()
        # Token map: {symbol: token_number} and reverse {token_number: symbol}
        self.sym_to_token={}   # e.g. {"RELIANCE": "2885"}
        self.token_to_sym={}   # e.g. {"2885": "RELIANCE"}
        # State machine: DISCONNECTED/CONNECTING/AUTHENTICATING/CONNECTED/TOKEN_EXPIRED/STOPPING
        self._state="DISCONNECTED"
        self.candles=defaultdict(dict);self.last_candle=defaultdict(int)
        # Volume tracking — two separate baselines
        self.base_volume_offset={}      # Session baseline — set ONCE, never changed
        self.candle_start_volume={}     # Per-candle baseline — resets at each 5-min boundary
        self.last_cum_volume={}
        self.volume_initialized={}
        self._thread=None;self._stop=False
        self.last_tick_time=time.time()
        self.last_tf_tick_time=0  # Only updated on real tf (stock price) ticks
        # Exponential backoff
        self.reconnect_delay=1.0
        self.reconnect_attempts=0

    def _set_state(self,s):
        with self.lock:
            if self._state!=s:
                print(f"[WS] State: {self._state} → {s}")
                self._state=s

    def _get_state(self):
        with self.lock:return self._state

    @property
    def connected(self):
        return self._get_state()=="CONNECTED"

    def _market_hours(self):
        ist=datetime.datetime.now(datetime.timezone.utc).astimezone(datetime.timezone(datetime.timedelta(hours=5,minutes=30)))
        m=ist.hour*60+ist.minute
        return 9*60+10<=m<=15*60+35

    def start(self):
        if self._thread and self._thread.is_alive():
            print("[WS] Already running");return
        self._stop=False
        self._set_state("DISCONNECTED")
        self._thread=threading.Thread(target=self._run,daemon=True)
        self._thread.start()
        self._watchdog=threading.Thread(target=self._watchdog_run,daemon=True)
        self._watchdog.start()
        self._heartbeat=threading.Thread(target=self._heartbeat_run,daemon=True)
        self._heartbeat.start()
        print("[WS] Started")

    def _heartbeat_run(self):
        """Send heartbeat every 30 seconds as required by Flattrade API"""
        while not self._stop:
            time.sleep(30)
            if self._get_state()=="CONNECTED":
                try:
                    self.ws.send(json.dumps({"t":"h"}))
                except Exception as e:
                    print(f"[WS] Heartbeat err: {e}")

    def _watchdog_run(self):
        """Reconnect if no ticks for 45 seconds during market hours (catches OR-window drops fast)"""
        while not self._stop:
            time.sleep(15)
            if self._get_state()=="CONNECTED" and self._market_hours():
                age=time.time()-self.last_tick_time
                if age>45:
                    print(f"[WS] Watchdog: no ticks for {age:.0f}s — reconnecting")
                    self._set_state("DISCONNECTED")
                    if self.ws:
                        try:self.ws.close()
                        except:pass

    def stop(self):
        self._stop=True;self._set_state("STOPPING")
        if self.ws:
            try:self.ws.close()
            except:pass
        print("[WS] Stopped")

    def _run(self):
        while not self._stop:
            try:
                state=self._get_state()
                if state=="TOKEN_EXPIRED":
                    print("[WS] Token expired — stopping");self._stop=True;break
                if state=="STOPPING":break
                if not self._market_hours():
                    print("[WS] Market closed but connecting anyway...")
                self._set_state("CONNECTING")
                print(f"[WS] Connecting attempt {self.reconnect_attempts+1}...")
                self.ws=websocket.WebSocketApp(self.ws_url,
                    on_open=self._on_open,on_message=self._on_message,
                    on_close=self._on_close,on_error=self._on_error)
                self.ws.run_forever(ping_interval=30,ping_timeout=10)
            except Exception as e:print(f"[WS] Error:{e}")
            if not self._stop and self._get_state()!="STOPPING":
                # Exponential backoff: 1→2→4→8→...→60s
                delay=min(self.reconnect_delay*(2**self.reconnect_attempts),60)
                self.reconnect_attempts+=1
                print(f"[WS] Reconnecting in {delay:.1f}s...")
                time.sleep(delay)

    def _on_open(self,ws):
        self._set_state("AUTHENTICATING")
        print("[WS] Connected! Auth...")
        try:
            ws.send(json.dumps({
                "t":"a","uid":self.client_id,"actid":self.client_id,
                "source":"API","accesstoken":self.token
            }))
        except Exception as e:
            print(f"[WS] Auth send err:{e}")
            self._set_state("DISCONNECTED")

    def _on_message(self,ws,message):
        try:
            data=json.loads(message)
            t=data.get("t")
            if t in ("ck","ak"):
                if data.get("s")=="OK":
                    self._set_state("CONNECTED")
                    # Reset backoff on successful auth
                    self.reconnect_delay=1.0;self.reconnect_attempts=0
                    self.was_ever_connected=True;self.token_expired=False
                    print(f"[WS] Auth OK type={t}")
                    if self.subscribed:self._resub()
                else:
                    print(f"[WS] Auth FAILED:{data}")
                    self._set_state("TOKEN_EXPIRED")
                    self.token_expired=True;self._stop=True
            elif t=="tf":self._tick(data)
            elif t=="tk":
                # Subscription acknowledgment — populate token map
                tok=data.get("tk","")
                ts_raw=data.get("ts","")
                sym=ts_raw.replace("-EQ","").strip()
                if tok and sym:
                    with self.lock:
                        self.sym_to_token[sym]=tok
                        self.token_to_sym[tok]=sym
                ltp=data.get("lp",0)
                print(f"[WS] Sub ACK: {sym} token={tok} ltp={ltp}")
                # Process as first tick too
                if ltp:self._tick(data)
            elif t=="touchline":
                # Bulk subscription acknowledgment
                print(f"[WS] Touchline ACK: {len(data)} fields")
            elif t in ("p","h","hk"):
                self.last_tick_time=time.time()  # Reset watchdog on heartbeat/ack
            elif t=="ms":
                pass  # Market status message — silently ignore, don't reset tick timer
            elif t=="om":pass  # Order update — ignore for now
            else:print(f"[WS] Msg:{data}")
        except Exception as e:print(f"[WS] Msg err:{e}")

    def _on_close(self,ws,code,msg):
        if code==1008:
            print("[WS] Token expired code=1008")
            self._set_state("TOKEN_EXPIRED")
            self.token_expired=True;self._stop=True
        else:
            print(f"[WS] Disconnected code={code} — will reconnect")
            self._set_state("DISCONNECTED")

    def _on_error(self,ws,error):
        print(f"[WS] Err:{error}")
        self._set_state("DISCONNECTED")

    def subscribe(self,symbols):
        new=[s for s in symbols if s not in self.subscribed]
        if not new:return
        with self.lock:self.subscribed.update(new)
        sent=0
        for i in range(0,len(new),10):
            batch=new[i:i+10]
            # Use token format if available, else fall back to symbol-EQ format
            keys_list=[]
            for s in batch:
                tok=self.sym_to_token.get(s)
                if tok:
                    keys_list.append(f"NSE|{tok}")
                else:
                    keys_list.append(f"NSE|{s}-EQ")  # fallback
            keys="#".join(keys_list)
            if self._get_state()=="CONNECTED":
                try:
                    self.ws.send(json.dumps({"t":"t","k":keys}))
                    sent+=len(batch)
                    time.sleep(0.2)  # 200ms delay between batches
                except Exception as e:
                    print(f"[WS] Subscribe err:{e}")
                    self._set_state("DISCONNECTED")
                    break
        has_tokens=sum(1 for s in new if s in self.sym_to_token)
        print(f"[WS] Subscribed {sent}/{len(new)} stocks ({has_tokens} with tokens, {len(new)-has_tokens} fallback)")

    def _resub(self):
        syms=list(self.subscribed);self.subscribed.clear();self.subscribe(syms)

    def _tick(self,data):
        try:
            # Try symbol from ts field first, then map from token
            sym=data.get("ts","").replace("-EQ","").strip()
            if not sym:
                tok=data.get("tk","")
                sym=self.token_to_sym.get(tok,"")
            if not sym:return
            ltp=float(data.get("lp",0) or 0)
            cum_vol=int(data.get("v",0) or 0)
            ts=int(data.get("ft",0) or 0)
            if not ltp:return
            self.last_tick_time=time.time()
            self.last_tf_tick_time=time.time()  # Real stock price tick
            with self.lock:
                if cum_vol>0:self.last_cum_volume[sym]=cum_vol
                # Set ONCE — never update base_volume_offset again
                if sym not in self.volume_initialized and cum_vol>0:
                    self.base_volume_offset[sym]=cum_vol    # Session baseline — permanent
                    self.candle_start_volume[sym]=cum_vol   # Candle baseline — resets per bar
                    self.volume_initialized[sym]=True
                # Interval vol = within current 5-min candle only
                interval_vol=max(0,cum_vol-self.candle_start_volume.get(sym,cum_vol))
            tick_out={"symbol":sym,"ltp":ltp,"volume":interval_vol,"time":ts}
            # Capture prev close and OHLD from subscription ACK (tk) and tick (tf) messages
            pc=float(data.get("c",0) or 0)
            if pc>0:tick_out["prev_close"]=pc
            op=float(data.get("o",0) or 0)
            if op>0:tick_out["open"]=op
            hi=float(data.get("h",0) or 0)
            if hi>0:tick_out["high"]=hi
            lo=float(data.get("l",0) or 0)
            if lo>0:tick_out["low"]=lo
            if self.on_tick:
                self.on_tick(tick_out)
            if self.on_vwap_update and interval_vol>0:
                self.on_vwap_update(sym,ltp,interval_vol)
            self._candle(sym,ltp,cum_vol,ts)
        except Exception as e:print(f"[WS] Tick err:{e}")

    def _candle(self,sym,ltp,cum_vol,ts):
        try:
            if not ts:ts=int(time.time())
            bucket=(ts//300)*300
            with self.lock:
                if bucket!=self.last_candle[sym] and self.last_candle[sym]>0:
                    lb=self.last_candle[sym]
                    if lb in self.candles[sym]:
                        c=self.candles[sym][lb];c["closed"]=True
                        # Final candle volume = ticks within that 5-min window
                        c["volume"]=max(0,cum_vol-self.candle_start_volume.get(sym,cum_vol))
                        # Update CANDLE baseline only (NOT session baseline)
                        self.candle_start_volume[sym]=cum_vol
                        if self.on_candle_close:self.on_candle_close(sym,c)
                    if len(self.candles[sym])>20:
                        del self.candles[sym][min(self.candles[sym].keys())]
                if bucket not in self.candles[sym]:
                    if sym not in self.candle_start_volume and cum_vol>0:
                        self.candle_start_volume[sym]=cum_vol
                    self.candles[sym][bucket]={
                        "symbol":sym,"time":bucket,
                        "open":ltp,"high":ltp,"low":ltp,"close":ltp,
                        "volume":0,"closed":False
                    }
                else:
                    c=self.candles[sym][bucket]
                    c["high"]=max(c["high"],ltp);c["low"]=min(c["low"],ltp)
                    c["close"]=ltp
                    c["volume"]=max(0,cum_vol-self.candle_start_volume.get(sym,cum_vol))
                self.last_candle[sym]=bucket
        except Exception as e:print(f"[WS] Candle err:{e}")

    def reset_session(self):
        with self.lock:
            self.volume_initialized.clear()
            self.base_volume_offset.clear()
            self.candle_start_volume.clear()
            self.last_cum_volume.clear()
        print("[WS] Session reset — volume baselines cleared")

    @property
    def status(self):
        with self.lock:
            return{
                "state":self._state,
                "connected":self._state=="CONNECTED",
                "subscribed":len(self.subscribed)
            }
