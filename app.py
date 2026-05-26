import datetime
import hashlib
import json
import os
import random
import threading
import time
import requests
from flask import Flask,request,jsonify,redirect
import sys
sys.path.insert(0,os.path.dirname(__file__))
from websocket_engine import FlattradeWebSocket
from orb_strategy import ORBStrategy
from order_manager import OrderManager

app=Flask(__name__)
API_KEY="2b0413e59c324d94a6178efce8d8790b"
API_SECRET="2026.11d611243f04456aadeca7d1cabe02a3aceed69c4d476e43"
CLIENT_ID="FZ07236"
FLATTRADE_AUTH_URL="https://authapi.flattrade.in/trade/apitoken"
FLATTRADE_ORDER_URL="https://piconnect.flattrade.in/PiConnectAPI/PlaceOrder"
FLATTRADE_SEARCH_URL="https://piconnect.flattrade.in/PiConnectAPI/SearchScrip"
FLATTRADE_POS_URL="https://piconnect.flattrade.in/PiConnectAPI/PositionBook"
STATE_FILE="/home/ubuntu/market-watch/.state.json"
NSE_HEADERS={"User-Agent":"Mozilla/5.0","Accept":"application/json","Referer":"https://www.nseindia.com"}
session_token=None;ws_engine=None;market_data={};lock=threading.Lock()  # vwap_data removed — orb owns VWAP
fetch_count=0;strategy_active=False;_last_snapshot_save=0
asm_gsm_symbols=set();asm_gsm_last_fetch=None
ui_settings={"ap":"1","am":"5","atype":"sv","aOn":False}

def load_state():
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE,'r') as f:
                d=json.load(f)
            print(f"[State] Loaded: strategy={d.get('strategy_active',False)}, capital={d.get('capital',20000)}")
            return d
    except Exception as e:
        print(f"[State] Load error: {e}")
    return {}

def save_state(active):
    try:
        with open(STATE_FILE,'w') as f:
            json.dump({"strategy_active":active,"capital":trading_config.get("CAPITAL",20000),
                "ui_settings":ui_settings,
                "settings_saved":datetime.datetime.now().isoformat(),
                "session_token":session_token,
                "token_saved":datetime.datetime.now().isoformat()},f)
    except Exception as e:
        print(f"[State] Save error: {e}")

_state=load_state()
strategy_active=_state.get("strategy_active",False) if isinstance(_state,dict) else False

trading_config={"CAPITAL":20000,"RISK_PCT":0.005,"MAX_TRADES_DAY":3,"MAX_DAILY_LOSS_PCT":0.02,"MIS_MARGIN":4,
    "OR_START_H":9,"OR_START_M":15,"OR_END_H":9,"OR_END_M":20,
    "TRADE_START_H":9,"TRADE_START_M":30,"TRADE_END_H":10,"TRADE_END_M":45,
    "FORCE_EXIT_H":14,"FORCE_EXIT_M":55,"OR_MIN_SIZE_PCT":0.3,"OR_MAX_SIZE_PCT":2.5,
    "VOLUME_MULTIPLIER":2.0,"NIFTY_GAP_LIMIT":1.5,"RVOL_MIN":1.5,"RVOL_STRONG":2.0,"MAX_POSITION_PCT":0.25,
    "T1_EXIT_PCT":0.40,"T2_EXIT_PCT":0.40,"T3_EXIT_PCT":0.20,
    "MIN_PRICE":100,"MAX_PRICE":10000,"MIN_VOLUME":1000000,"MAX_CONSECUTIVE_LOSS":2}

if isinstance(_state,dict) and "capital" in _state:
    trading_config["CAPITAL"]=_state["capital"]
    print(f"[State] Capital restored: Rs {_state['capital']:,}")

if isinstance(_state,dict) and "ui_settings" in _state:
    saved=_state.get("ui_settings",{})
    saved_time_str=_state.get("settings_saved","")
    now=datetime.datetime.now()
    reset_needed=False
    if saved_time_str:
        try:
            saved_dt=datetime.datetime.fromisoformat(saved_time_str)
            # Reset ap/am/aOn if saved on a previous day AND it is now past 8:00 AM
            if saved_dt.date()<now.date() and now.hour>=8:
                reset_needed=True
        except:
            reset_needed=True
    if reset_needed:
        # Keep atype (user preference for sound/visual) but reset alert thresholds and bell
        saved_kept={"atype":saved.get("atype","sv")}
        ui_settings.update(saved_kept)
        print(f"[State] Alert settings reset for new day (saved {saved_time_str[:10]}), atype kept")
    else:
        ui_settings.update(saved)
        print(f"[State] UI settings restored: ap={saved.get('ap','?')}% am={saved.get('am','?')}min")

# Restore session token if saved today (Flattrade tokens are valid within the same trading day)
def _restore_token():
    global session_token,ws_engine
    if not isinstance(_state,dict): return
    saved_token=_state.get("session_token")
    saved_time=_state.get("token_saved","")
    if not saved_token or not saved_time: return
    try:
        saved_dt=datetime.datetime.fromisoformat(saved_time)
        now=datetime.datetime.now()
        # Only restore if saved today and before 4pm (tokens expire end of day)
        if saved_dt.date()==now.date() and now.hour<16:
            session_token=saved_token
            print(f"[State] Session token restored from today ({saved_dt.strftime('%H:%M')})")
        else:
            print(f"[State] Saved token is from {saved_dt.date()} — skipping (stale)")
    except Exception as e:
        print(f"[State] Token restore error: {e}")

_restore_token()
orb=ORBStrategy(trading_config)
om=OrderManager(CLIENT_ID,lambda:session_token,paper_mode=True)

orb.on_signal=lambda s:print(f"[APP] SIGNAL: {s['symbol']} {s['direction']}")
om.on_fill=lambda p:print(f"[APP] FILLED: {p['symbol']} {p['direction']} ({('CNC' if p.get('product')=='C' else 'MIS')})")
om.on_exit=lambda p:(orb.record_trade_result(p.get("net_pnl",0)),print(f"[APP] CLOSED: {p['symbol']} PnL={p.get('net_pnl',0):+.2f}"))
om.on_sl_hit=lambda s,l,p:print(f"[APP] SL HIT: {s}@{l}")
om.on_target=lambda s,t,l,p:print(f"[APP] {t} HIT: {s}@{l}")

def _ist():
    return datetime.datetime.now(datetime.timezone.utc).astimezone(datetime.timezone(datetime.timedelta(hours=5,minutes=30)))

def is_index(sym):
    return " " in sym or sym.startswith("NIFTY") or sym.startswith("SENSEX")

def ws_has_live_ticks():
    # Use last_tf_tick_time — only real stock price ticks, not heartbeats/ms messages
    return ws_engine and ws_engine.connected and (time.time()-getattr(ws_engine,"last_tf_tick_time",0))<30

def reset_vwap():
    orb.vwap_data={}  # orb owns VWAP — reset here for daily clock
    print("[VWAP] Reset")

def save_prices_snapshot():
    try:
        with lock: snap={s:{k:d[k] for k in ("ltp","change","change_pct","open","high","low","prev_close","vwap","volume_raw","value","range_pct") if k in d} for s,d in market_data.items() if d.get("ltp",0)>0}
        with open(PRICES_SNAPSHOT,"w") as f: json.dump(snap,f)
    except Exception as e:
        print(f"[Prices] Snapshot save error: {e}")

def restore_prices_snapshot():
    if not os.path.exists(PRICES_SNAPSHOT): return
    try:
        with open(PRICES_SNAPSHOT) as f: snap=json.load(f)
        count=0
        with lock:
            for sym,prices in snap.items():
                if sym in market_data:
                    market_data[sym].update(prices);count+=1
        print(f"[Prices] Restored last-known prices for {count} stocks")
    except Exception as e:
        print(f"[Prices] Snapshot restore error: {e}")

def fetch_nifty_from_yahoo():
    """Fetch Nifty 50 change% from Yahoo Finance — reliable fallback when NSE Nifty feed fails."""
    try:
        r=requests.get("https://query1.finance.yahoo.com/v8/finance/chart/%5ENSEI?interval=1d&range=1d",
            headers={"User-Agent":"Mozilla/5.0"},timeout=8)
        if r.status_code==200:
            meta=r.json().get("chart",{}).get("result",[{}])[0].get("meta",{})
            ltp=float(meta.get("regularMarketPrice") or 0)
            prev=float(meta.get("chartPreviousClose") or meta.get("previousClose") or 0)
            if ltp>0 and prev>0:
                chg_pct=round((ltp-prev)/prev*100,3)
                chg=round(ltp-prev,2)
                with lock:
                    market_data["NIFTY 100"]={"symbol":"NIFTY 100","ltp":ltp,
                        "change_pct":chg_pct,"change":chg,
                        "updated":datetime.datetime.now().isoformat()}
                orb.update_nifty(chg_pct,_ist())
                print(f"[Yahoo] Nifty {ltp:.2f} chg={chg_pct:+.3f}%")
                return True
    except Exception as e:
        print(f"[Yahoo] Nifty fetch error: {e}")
    return False

def fetch_prices_from_yahoo():
    """Fetch last-traded prices from Yahoo Finance v8 chart — used when NSE is unavailable."""
    from concurrent.futures import ThreadPoolExecutor,as_completed
    with lock: syms=[s for s in market_data if not is_index(s)]
    if not syms: return
    print(f"[Yahoo] Fetching last prices for {len(syms)} stocks...")
    updated=0
    def fetch_one(sym):
        try:
            r=requests.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}.NS?interval=1d&range=5d",
                headers={"User-Agent":"Mozilla/5.0"},timeout=8)
            if r.status_code!=200: return None
            meta=r.json().get("chart",{}).get("result",[{}])[0].get("meta",{})
            ltp=float(meta.get("regularMarketPrice") or 0)
            prev=float(meta.get("chartPreviousClose") or meta.get("previousClose") or 0)
            if ltp<=0: return None
            chg=round(ltp-prev,2) if prev>0 else 0
            chg_pct=round((ltp-prev)/prev*100,2) if prev>0 else 0
            vol=int(meta.get("regularMarketVolume") or 0)
            hi=float(meta.get("regularMarketDayHigh") or ltp)
            lo=float(meta.get("regularMarketDayLow") or ltp)
            avg_price=(hi+lo+ltp)/3
            val=round(avg_price*vol/1e7,2) if vol>0 else 0  # crores (volume × avg price)
            wk_hi=float(meta.get("fiftyTwoWeekHigh") or 0)
            wk_lo=float(meta.get("fiftyTwoWeekLow") or 0)
            rng=round((ltp-wk_lo)/(wk_hi-wk_lo)*100,1) if wk_hi>wk_lo else 0
            return sym,{"ltp":ltp,"prev_close":prev,"change":chg,"change_pct":chg_pct,"volume_raw":vol,"value":val,"range_pct":rng}
        except: return None
    with ThreadPoolExecutor(max_workers=20) as ex:
        futures={ex.submit(fetch_one,s):s for s in syms}
        for fut in as_completed(futures):
            res=fut.result()
            if res:
                sym,prices=res
                with lock:
                    if sym in market_data:
                        market_data[sym].update(prices);updated+=1
    print(f"[Yahoo] Updated {updated} stocks with last prices")
    if updated>0: save_prices_snapshot()

def fetch_asm_gsm_list():
    global asm_gsm_symbols,asm_gsm_last_fetch
    symbols=set()
    try:
        s=requests.Session()
        s.get("https://www.nseindia.com",headers=NSE_HEADERS,timeout=5)
        r=s.get("https://www.nseindia.com/api/reportASM",headers=NSE_HEADERS,timeout=10)
        if r.status_code==200:
            data=r.json()
            for key in ["longterm","shortterm"]:
                for item in data.get(key,{}).get("data",[]):
                    sym=item.get("symbol","").replace("-EQ","").strip()
                    if sym:symbols.add(sym)
            print(f"[MARGIN] ASM list: {len(symbols)} stocks")
    except Exception as e:
        print(f"[MARGIN] ASM fetch error: {e}")
    try:
        s2=requests.Session()
        s2.get("https://www.nseindia.com",headers=NSE_HEADERS,timeout=5)
        r2=s2.get("https://www.nseindia.com/api/reportGSM",headers=NSE_HEADERS,timeout=10)
        if r2.status_code==200:
            data2=r2.json()
            items=data2 if isinstance(data2,list) else data2.get("data",[])
            for item in items:
                sym=item.get("symbol","").replace("-EQ","").strip()
                if sym:symbols.add(sym)
            print(f"[MARGIN] ASM+GSM total: {len(symbols)} stocks")
    except Exception as e:
        print(f"[MARGIN] GSM fetch error: {e}")
    asm_gsm_symbols=symbols
    asm_gsm_last_fetch=datetime.datetime.now()
    update_stock_margins()

def get_stock_margin(sym):
    if sym in asm_gsm_symbols:return 0
    return 4

def update_stock_margins():
    with lock:
        syms=list(market_data.keys())
    margins={sym:get_stock_margin(sym) for sym in syms}
    orb.stock_margins=margins
    skipped=sum(1 for m in margins.values() if m<3)
    print(f"[MARGIN] Updated {len(margins)} stocks, {skipped} skipped (<3x margin)")

NSE_MASTER_URL="https://api.shoonya.com/NSE_symbols.txt.zip"
NSE_MASTER_FILE="/home/ubuntu/market-watch/NSE_symbols.txt"
NSE_MASTER_ZIP="/home/ubuntu/market-watch/NSE_symbols.txt.zip"
TOP500_CACHE="/home/ubuntu/market-watch/.top500_cache.json"
PRICES_SNAPSHOT="/home/ubuntu/market-watch/.prices_snapshot.json"
_master_token_map={}  # global cache: {symbol: token_str}

def refresh_master_file():
    global _master_token_map
    try:
        import csv,zipfile,io
        print("[TOKEN] Downloading NSE master contract file...")
        r=requests.get(NSE_MASTER_URL,timeout=30)
        if r.status_code!=200:
            print(f"[TOKEN] Master file download failed: {r.status_code}")
            return False
        with open(NSE_MASTER_ZIP,"wb") as f:
            f.write(r.content)
        with zipfile.ZipFile(NSE_MASTER_ZIP,"r") as z:
            z.extractall(os.path.dirname(NSE_MASTER_ZIP))
        token_map={}
        with open(NSE_MASTER_FILE,"r") as f:
            reader=csv.reader(f)
            next(reader)
            for row in reader:
                if len(row)<6:continue
                exch,token,_,sym,tsym,inst=row[0],row[1],row[2],row[3],row[4],row[5]
                if exch=="NSE" and inst.strip()=="EQ":
                    clean=tsym.replace("-EQ","").strip()
                    token_map[clean]=token
        _master_token_map=token_map
        print(f"[TOKEN] Master file loaded: {len(token_map)} NSE EQ tokens")
        return True
    except Exception as e:
        print(f"[TOKEN] Master file error: {e}")
        return False

def load_master_from_disk():
    global _master_token_map
    try:
        import csv
        if not os.path.exists(NSE_MASTER_FILE):
            return False
        token_map={}
        with open(NSE_MASTER_FILE,"r") as f:
            reader=csv.reader(f)
            next(reader)
            for row in reader:
                if len(row)<6:continue
                exch,token,_,sym,tsym,inst=row[0],row[1],row[2],row[3],row[4],row[5]
                if exch=="NSE" and inst.strip()=="EQ":
                    clean=tsym.replace("-EQ","").strip()
                    token_map[clean]=token
        _master_token_map=token_map
        print(f"[TOKEN] Loaded from disk: {len(token_map)} tokens")
        return True
    except Exception as e:
        print(f"[TOKEN] Disk load error: {e}")
        return False

def fetch_tokens_for_symbols(symbols,token):
    global _master_token_map
    if not _master_token_map:
        if not load_master_from_disk():
            refresh_master_file()
    sym_to_token={}
    missing=[]
    for sym in symbols:
        tok=_master_token_map.get(sym)
        if tok:
            sym_to_token[sym]=tok
        else:
            missing.append(sym)
    if missing:
        print(f"[TOKEN] {len(missing)} symbols not in master: {missing[:10]}")
    print(f"[WS] Token fetch: {len(sym_to_token)}/{len(symbols)} resolved from master file")
    return sym_to_token

def seed_market_data_from_master():
    """Seed top 500 stocks by traded value from master file"""
    import csv
    if not os.path.exists(NSE_MASTER_FILE): return
    # If we have live market_data with values, pick top 500 by value
    with lock:
        valued = [(sym, d.get("value", 0)) for sym, d in market_data.items()
                  if not is_index(sym) and d.get("value", 0) > 0]
    if len(valued) >= 100:
        # Sort by value descending, keep top 500
        valued.sort(key=lambda x: x[1], reverse=True)
        top500 = set(sym for sym, _ in valued[:500])
        # Remove symbols not in top 500
        with lock:
            to_remove = [s for s in list(market_data.keys())
                         if not is_index(s) and s not in top500]
            for s in to_remove:
                del market_data[s]
        print(f"[WS] Filtered to top 500 by value, removed {len(to_remove)} stocks")
        return
    # Fallback: NSE data not available, seed up to 500 EQ from master file
    count = 0
    with open(NSE_MASTER_FILE, "r") as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            if count >= 500: break
            if len(row) < 6: continue
            if row[0] == "NSE" and row[5].strip() == "EQ":
                sym = row[4].replace("-EQ","").strip()
                with lock:
                    if sym not in market_data:
                        market_data[sym] = {"symbol":sym,"ltp":0,"vwap":0,"volume_raw":0,
                            "change":0,"change_pct":0,"dir":"","seeded":True}
                        count += 1
    print(f"[WS] Seeded {count} symbols from master file (NSE unavailable)")

def start_ws_subscription():
    # Step 1: Wait up to 30s for NSE API to load live data
    waited = 0
    while len(market_data) < 10 and waited < 30:
        print(f"[WS] Waiting for NSE data... {len(market_data)} stocks so far")
        time.sleep(5)
        waited += 5
    # Step 2: If NSE loaded OK — filter to top 500 by value
    with lock:
        valued = [(s,d.get("value",0)) for s,d in market_data.items()
                  if not is_index(s) and d.get("value",0)>0]
    if len(valued) >= 100:
        valued.sort(key=lambda x:x[1],reverse=True)
        top500 = set(s for s,_ in valued[:500])
        with lock:
            to_remove=[s for s in list(market_data.keys()) if not is_index(s) and s not in top500]
            for s in to_remove: del market_data[s]
        print(f"[WS] Top 500 by value selected, removed {len(to_remove)} stocks")
    # Step 3: If NSE unavailable — seed from master file as fallback
    elif len(market_data) < 10:
        print(f"[WS] NSE unavailable — seeding 500 symbols from master file")
        import csv
        if os.path.exists(NSE_MASTER_FILE):
            count=0
            with open(NSE_MASTER_FILE,"r") as f:
                reader=csv.reader(f)
                next(reader)
                for row in reader:
                    if count>=500:break
                    if len(row)<6:continue
                    if row[0]=="NSE" and row[5].strip()=="EQ":
                        sym=row[4].replace("-EQ","").strip()
                        with lock:
                            if sym not in market_data:
                                market_data[sym]={"symbol":sym,"ltp":0,"vwap":0,"volume_raw":0,
                                    "change":0,"change_pct":0,"dir":"","seeded":True}
                                count+=1
            print(f"[WS] Seeded {count} symbols from master file")
    syms=[s for s in market_data.keys() if not is_index(s)]
    print(f"[WS] Fetching tokens for {len(syms)} symbols...")
    tok_map=fetch_tokens_for_symbols(syms,session_token)
    if ws_engine:
        ws_engine.sym_to_token=tok_map
        ws_engine.token_to_sym={v:k for k,v in tok_map.items()}
        print(f"[WS] Token map loaded: {len(tok_map)} tokens")
    time.sleep(1)
    if ws_engine:ws_engine.subscribe(syms)

def on_tick(tick):
    sym,ltp=tick["symbol"],tick["ltp"]
    if not sym or ltp<=0:return
    with lock:
        if sym not in market_data:
            market_data[sym]={"symbol":sym,"ltp":ltp,"vwap":0,"volume_raw":0,"change":0,"change_pct":0,"dir":""}
        old=market_data[sym].get("ltp",ltp)
        market_data[sym]["ltp"]=ltp
        market_data[sym]["volume_raw"]=tick.get("volume",0)
        market_data[sym]["dir"]="up" if ltp>old else("dn" if ltp<old else "")
        vol=tick.get("volume",0)
        if vol>0:
            # VWAP: orb.update_vwap is single source — mirror to market_data for UI
            vwap_val=orb.get_vwap(sym)
            if vwap_val>0:market_data[sym]["vwap"]=round(vwap_val,2)
        prev=market_data[sym].get("prev_close",ltp)
        if prev>0:
            market_data[sym]["change"]=round(ltp-prev,2)
            market_data[sym]["change_pct"]=round((ltp-prev)/prev*100,2)
    om.update_ltp(sym,ltp)
    global _last_snapshot_save
    if time.time()-_last_snapshot_save>300:
        _last_snapshot_save=time.time()
        threading.Thread(target=save_prices_snapshot,daemon=True).start()

def on_candle_close(sym,candle):
    if not strategy_active:return
    now=_ist();h,m=now.hour,now.minute
    if h==9 and 15<=m<=20:
        with orb.lock:
            orb.or_data[sym]={"high":candle["high"],"low":candle["low"],"volume":candle["volume"],"built":False}
        orb.finalize_or(sym,or_candle=candle)
        return
    orb.record_candle_volume(sym,candle.get("volume",0))
    if orb._is_trade_time():
        nifty_chg=market_data.get("NIFTY 100",{}).get("change_pct",0)
        orb.update_nifty(nifty_chg,_ist())
        can,_=orb.can_trade()
        if not can:return
        signal=orb.check_breakout(sym,candle,nifty_chg,market_data)
        if signal:
            result=om.place_order(signal)
            if not result["success"]:
                print(f"[APP] Order failed {sym}: {result.get('error')}")
                orb.active_signals.pop(sym,None)

def master_clock_loop():
    or_finalized_today=False;daily_reset_done=False;last_date=None
    print("[CLOCK] Master clock started")
    while True:
        try:
            now=_ist();today=now.strftime("%Y-%m-%d")
            h,m,s=now.hour,now.minute,now.second;mins=h*60+m
            if h==8 and m==0 and s<=30 and not daily_reset_done:
                threading.Thread(target=fetch_asm_gsm_list,daemon=True).start()
                threading.Thread(target=refresh_master_file,daemon=True).start()
                orb.reset_daily();reset_vwap()
                alert_store.clear()
                # Reset alert thresholds (ap, am, aOn) — keep atype preference
                ui_settings.update({"ap":"1","am":"5","aOn":False})
                save_state(strategy_active)
                print("[CLOCK] Alert settings reset to defaults at 8:00 AM")
                daily_reset_done=True;or_finalized_today=False
                print("[CLOCK] Daily reset at 8:00 AM")
            if h==9 and m==20 and s<=10 and not or_finalized_today and strategy_active:
                if len(orb.or_data)==0:
                    with lock:snap=dict(market_data)
                    for sym,d in snap.items():
                        if is_index(sym):continue
                        ltp=d.get("ltp",0)
                        if ltp>0:orb.process_or_tick(sym,ltp,d.get("volume_raw",0))
                    print(f"[CLOCK] Forced OR from NSE: {len(orb.or_data)} stocks")
                orb.finalize_all_or_ranges()
                or_finalized_today=True
                print("[CLOCK] OR finalized at 9:20 AM!")
            if today!=last_date:
                last_date=today;daily_reset_done=False;or_finalized_today=False
        except Exception as e:
            print(f"[CLOCK] Error: {e}")
        time.sleep(1)  # Always runs — even after exception

def _make_nse_session():
    """Create a fresh NSE session with homepage cookie handshake."""
    s=requests.Session()
    try:
        s.get("https://www.nseindia.com",headers=NSE_HEADERS,timeout=5)
    except Exception as e:
        print(f"[NSE] Session init warning: {e}")
    return s

def fetch_nse_loop():
    global fetch_count
    fail_count=0
    # Persistent session — reused across poll cycles; recreated only on 403 or every 10 min
    nse_session=_make_nse_session()
    session_born=time.time()
    for attempt in range(5):
        try:
            r=nse_session.get("https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20500",headers=NSE_HEADERS,timeout=15)
            if r.status_code==403:
                print(f"[NSE] Init 403 attempt {attempt+1}/5 — refreshing session")
                nse_session=_make_nse_session();session_born=time.time()
                time.sleep(2**attempt);continue
            if r.status_code!=200:
                print(f"[NSE] Init HTTP {r.status_code} — market closed?")
                break
            for item in r.json().get("data",[]):
                sym=item.get("symbol","")
                if sym:
                    with lock:
                        market_data[sym]={"symbol":sym,
                            "ltp":float(item.get("lastPrice",0) or 0),
                            "open":float(item.get("open",0) or 0),
                            "high":float(item.get("dayHigh",0) or 0),
                            "low":float(item.get("dayLow",0) or 0),
                            "prev_close":float(item.get("previousClose",0) or 0),
                            "change":float(item.get("change",0) or 0),
                            "change_pct":float(item.get("pChange",0) or 0),
                            "volume_raw":int(item.get("totalTradedVolume",0) or 0),
                            "value":float(item.get("totalTradedValue",0) or 0),
                            "dir":"","updated":datetime.datetime.now().isoformat(),
                            "range_pct":round((float(item.get("lastPrice",0) or 0)-float(item.get("yearLow",0) or 0))/(float(item.get("yearHigh",0) or 1)-float(item.get("yearLow",0) or 0))*100,1) if float(item.get("yearHigh",0) or 0)>float(item.get("yearLow",0) or 0) else 0}
            print(f"[NSE] Loaded {len(market_data)} stocks")
            break
        except Exception as e:
            print(f"[NSE] Init error attempt {attempt+1}/5: {e}")
            time.sleep(2**attempt)
    while True:
        try:
            # Skip all NSE polling outside market hours — avoids rate-limit on weekends
            now_ist=_ist()
            ist_mins=now_ist.hour*60+now_ist.minute
            market_open=now_ist.weekday()<5 and 9*60<=ist_mins<=15*60+35
            if not market_open:
                time.sleep(60)
                continue
            # Refresh session every 10 minutes to avoid cookie expiry
            if time.time()-session_born>600:
                nse_session=_make_nse_session();session_born=time.time()
                print("[NSE] Session refreshed (10 min)")
            live=ws_has_live_ticks()
            # --- Nifty update: try NSE first, fall back to Yahoo (needed for ORB Nifty gate) ---
            nifty_updated=False
            try:
                r100=nse_session.get("https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20100",headers=NSE_HEADERS,timeout=5)
                if r100.status_code==403:
                    print("[NSE] Nifty100 403 — refreshing session")
                    nse_session=_make_nse_session();session_born=time.time()
                elif r100.status_code==200:
                    for item in r100.json().get("data",[]):
                        if item.get("symbol")=="NIFTY 100":
                            chg_pct=float(item.get("pChange",0) or 0)
                            with lock:
                                market_data["NIFTY 100"]={"symbol":"NIFTY 100",
                                    "ltp":float(item.get("lastPrice",0) or 0),
                                    "change_pct":chg_pct,
                                    "change":float(item.get("change",0) or 0),
                                    "updated":datetime.datetime.now().isoformat()}
                            orb.update_nifty(chg_pct,_ist())
                            nifty_updated=True
                            break
            except Exception as e:
                print(f"[NSE] Nifty100 error: {e}")
            # Yahoo fallback — runs every 30s so ORB gate is never stale
            if not nifty_updated:
                fetch_nifty_from_yahoo()
                nifty_updated=True
            # --- Stock price update (only when WS not live) ---
            if not live:
                r500=nse_session.get("https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20500",headers=NSE_HEADERS,timeout=10)
                # FIX: check status BEFORE calling .json() — 403 body is not JSON
                if r500.status_code==403:
                    fail_count=min(fail_count+1,8)
                    delay=min(2**fail_count,60)*(0.75+random.random()*0.5)  # jitter
                    print(f"[NSE] Rate limited 403 fail#{fail_count} — sleeping {delay:.1f}s, refreshing session")
                    nse_session=_make_nse_session();session_born=time.time()
                    time.sleep(delay)
                    continue  # skip ORB processing this cycle; retry next iteration
                fail_count=0
                if r500.status_code!=200: continue
                for item in r500.json().get("data",[]):
                    sym=item.get("symbol","")
                    if sym and sym in market_data:
                        ltp=float(item.get("lastPrice",0) or 0)
                        if ltp<=0:continue
                        with lock:
                            market_data[sym]["ltp"]=ltp
                            market_data[sym]["change_pct"]=float(item.get("pChange",0) or 0)
                            market_data[sym]["change"]=float(item.get("change",0) or 0)
                            market_data[sym]["volume_raw"]=int(item.get("totalTradedVolume",0) or 0)
                        om.update_ltp(sym,ltp)
                        # Approximate VWAP from NSE data for polling mode
                        hi=float(item.get("dayHigh",0) or 0)
                        lo=float(item.get("dayLow",0) or 0)
                        if hi>0 and lo>0 and ltp>0:
                            market_data[sym]["vwap"]=round((hi+lo+ltp)/3,2)
                        yr_hi=float(item.get("yearHigh",0) or 0)
                        yr_lo=float(item.get("yearLow",0) or 0)
                        if yr_hi>yr_lo:
                            market_data[sym]["range_pct"]=round((ltp-yr_lo)/(yr_hi-yr_lo)*100,1)
            fetch_count+=1
            src="WS" if live else "NSE"
            print(f"[{src}] Update #{fetch_count} — {len(market_data)} stocks")
            if fetch_count%60==0: save_prices_snapshot()  # save every ~5 min
            if strategy_active and not live:
                now=_ist();h_ist=now.hour;m_ist=now.minute;mins_ist=h_ist*60+m_ist
                if h_ist==9 and 15<=m_ist<20:
                    with lock:snap=dict(market_data)
                    for sym,d in snap.items():
                        if is_index(sym):continue
                        ltp=d.get("ltp",0)
                        if ltp>0:orb.process_or_tick(sym,ltp,d.get("volume_raw",0))
                    print(f"[ORB] Building OR from NSE... {m_ist}m")
                elif h_ist==9 and m_ist==20:
                    pass  # Master clock owns OR finalization at 9:20 AM
                elif 9*60+30<=mins_ist<=10*60+45:
                    if len(orb.or_data)>0:
                        nifty_chg=market_data.get("NIFTY 100",{}).get("change_pct",0)
                        orb.update_nifty(nifty_chg,_ist())  # always update — freshness gate relies on timestamp
                        can,reason=orb.can_trade()
                        if can:
                            with lock:snap=dict(market_data)
                            signals_placed=0
                            for sym,d in snap.items():
                                if is_index(sym):continue
                                if orb.trades_today>=orb.config["MAX_TRADES_DAY"]:break
                                if signals_placed>=1:break
                                ltp=d.get("ltp",0)
                                if ltp<=0:continue
                                if sym in orb.active_signals:continue
                                if sym in om.positions:continue
                                if sym not in orb.or_data:continue
                                or_=orb.or_data[sym]
                                if not or_.get("built"):continue
                                if not(0.3<=or_.get("size_pct",0)<=2.5):continue
                                if not(100<=ltp<=10000):continue
                                candle={"open":d.get("open",ltp),"high":d.get("high",ltp),
                                    "low":d.get("low",ltp),"close":ltp,
                                    "volume":d.get("volume_raw",0),"closed":True}
                                orb.update_nifty(nifty_chg,_ist())
                                signal=orb.check_breakout(sym,candle,nifty_chg,snap)
                                if signal:
                                    result=om.place_order(signal)
                                    if result.get("success"):
                                        print(f"[ORB] AUTO ORDER #{orb.trades_today}: {sym} {signal['direction']}")
                                        signals_placed+=1
                                    else:
                                        orb.active_signals.pop(sym,None)
                                        if not result.get("mis_not_allowed"):
                                            print(f"[ORB] Order failed {sym}: {result.get('error')}")
        except Exception as e:
            print(f"[NSE] Error: {e}")
        time.sleep(5)

def exchange_code(code):
    global session_token,ws_engine
    try:
        h=hashlib.sha256(f"{API_KEY}{code}{API_SECRET}".encode()).hexdigest()
        r=requests.post(FLATTRADE_AUTH_URL,json={"api_key":API_KEY,"request_code":code,"api_secret":h},timeout=10)
        print(f"[Auth] Response: {r.text[:200]}",flush=True)
        data=r.json()
        if data.get("token"):
            session_token=data["token"]
            print(f"[Auth] SUCCESS! Token: {session_token}",flush=True)
            save_state(strategy_active)  # persist token to disk immediately
            ws_engine=FlattradeWebSocket(session_token,CLIENT_ID,on_tick,on_candle_close,on_vwap_update=orb.update_vwap)
            ws_engine.start()
            threading.Thread(target=start_ws_subscription,daemon=True).start()
            return True
        print(f"[Auth] Failed: {data}",flush=True);return False
    except Exception as e:
        print(f"[Auth] Error: {e}",flush=True);return False

def sync_flattrade_positions():
    if not session_token or om.paper_mode:return
    try:
        payload={"uid":CLIENT_ID,"actid":CLIENT_ID}
        jdata=f"jData={json.dumps(payload)}&jKey={session_token}"
        r=requests.post(FLATTRADE_POS_URL,data=jdata,
            headers={"Content-Type":"application/x-www-form-urlencoded"},timeout=10)
        raw=r.text.strip()
        if not raw:return
        data=json.loads(raw)
        # PositionBook returns a list when positions exist, dict with stat=Not_Ok when empty
        if not isinstance(data,list):return
        for pos in data:
            sym=pos.get("tsym","").replace("-EQ","")
            netqty=int(pos.get("netqty","0") or 0)
            prd=pos.get("prd","")
            if not sym or prd not in("I","C"):continue  # sync both MIS and CNC positions
            if netqty==0:
                if sym in om.positions:del om.positions[sym]
                continue
            fp=float(pos.get("netupldprc","0") or 0)
            ltp=float(pos.get("lp","0") or 0)
            if sym in om.positions:
                om.positions[sym].update({"entry_price":round(fp,2),"last_ltp":round(ltp,2),
                    "qty_remaining":abs(netqty),"synced":True})
    except Exception as e:
        print(f"[POS] Sync error: {e}")

def position_sync_loop():
    global session_token
    while True:
        try:
            now=_ist();mins=now.hour*60+now.minute
            if ws_engine and getattr(ws_engine,"token_expired",False):
                if getattr(ws_engine,"was_ever_connected",False) and session_token:
                    print("[APP] WS token expired — clearing session")
                    session_token=None
                save_state(False)
            if 9*60+15<=mins<=15*60+30:sync_flattrade_positions()
        except Exception as e:
            print(f"[POS] Loop error: {e}")
        time.sleep(30)

@app.route("/",methods=["GET"])
def index():
    code=request.args.get("code")
    if code and request.args.get("client"):
        print(f"[Auth] Code: {code[:20]}...",flush=True)
        exchange_code(code)
        # Redirect to clean URL — prevents code re-use if user refreshes the page
        return redirect("/")
    return open(os.path.join(os.path.dirname(__file__),"index.html")).read(),200,{"Content-Type":"text/html"}

@app.route("/api/status")
def api_status():
    return jsonify({"success":True,"server":"NSE Market Watch v3.0","client_id":CLIENT_ID,
        "logged_in":session_token is not None,"paper_mode":om.paper_mode,
        "strategy_active":strategy_active,
        "ws_connected":ws_engine.connected if ws_engine else False,
        "ws_state":ws_engine._get_state() if ws_engine else "DISCONNECTED",
        "ws_has_ticks":ws_has_live_ticks(),
        "tracked":len(market_data),"fetch_count":fetch_count,
        "time":datetime.datetime.now().isoformat(),"capital":trading_config["CAPITAL"],
        "ui_settings":ui_settings,"asm_gsm_count":len(asm_gsm_symbols),
        "margin_last_fetch":asm_gsm_last_fetch.isoformat() if asm_gsm_last_fetch else None,
        "orb":orb.status,"orders":om.status})

@app.route("/api/market-data")
def api_market_data():
    with lock:data=list(market_data.values())
    data.sort(key=lambda x:abs(x.get("change_pct",0)),reverse=True)
    return jsonify({"success":True,"count":len(data),"data":data,
        "fetch_count":fetch_count,"last_updated":datetime.datetime.now().isoformat()})

# Server-side alert store — persists across page refreshes, clears at 8AM
alert_store={}  # {symbol: {pct, dir, time, ts}}

@app.route("/api/alerts/save",methods=["POST"])
def save_alerts():
    global alert_store
    data=request.json or {}
    alerts=data.get("alerts",{})
    alert_store.update(alerts)
    return jsonify({"success":True,"count":len(alert_store)})

@app.route("/api/alerts/load")
def load_alerts():
    return jsonify({"success":True,"alerts":alert_store,"count":len(alert_store)})

@app.route("/api/alerts/clear",methods=["POST"])
def clear_alerts():
    global alert_store
    alert_store={}
    return jsonify({"success":True})

@app.route("/api/alerts")
def api_alerts():
    pct=float(request.args.get("pct",1))
    alerts=[]
    with lock:
        for sym,d in market_data.items():
            chg=abs(d.get("change_pct",0))
            if chg>=pct:
                alerts.append({"symbol":sym,"change_pct":d.get("change_pct",0),"ltp":d.get("ltp",0),
                    "alert_pct":round(chg,2),"alert_dir":"UP" if d.get("change_pct",0)>0 else "DN","alerted":True})
    alerts.sort(key=lambda x:abs(x["change_pct"]),reverse=True)
    return jsonify({"success":True,"alerts":alerts,"count":len(alerts)})

@app.route("/api/margin-status")
def api_margin_status():
    with lock:syms=list(market_data.keys())
    margins={sym:get_stock_margin(sym) for sym in syms}
    skipped=[s for s,m in margins.items() if m<3]
    return jsonify({"success":True,"asm_gsm_count":len(asm_gsm_symbols),
        "asm_gsm_symbols":sorted(list(asm_gsm_symbols)),"skipped_count":len(skipped),
        "skipped_symbols":sorted(skipped),
        "last_fetch":asm_gsm_last_fetch.isoformat() if asm_gsm_last_fetch else None})

@app.route("/api/logout",methods=["POST"])
def api_logout():
    global session_token
    session_token=None
    if ws_engine:ws_engine.stop()
    return jsonify({"success":True})

@app.route("/api/strategy/start",methods=["POST"])
def start_strategy():
    global strategy_active
    strategy_active=True
    if orb.trades_today==0 and orb.daily_pnl==0:orb.reset_daily()
    save_state(True)
    now=_ist();mins=now.hour*60+now.minute
    if mins>9*60+20:
        built=0
        with lock:
            for sym,d in market_data.items():
                if is_index(sym):continue
                op=d.get("open",0);ltp=d.get("ltp",0)
                if op>0 and ltp>0:
                    hi=d.get("high",op);lo=d.get("low",op)
                    or_hi=round(max(hi,op*1.002),2);or_lo=round(min(lo,op*0.998),2)
                    or_sz=round(or_hi-or_lo,2);or_pct=round((or_sz/or_lo)*100,2)
                    orb.or_data[sym]={"high":or_hi,"low":or_lo,"size":or_sz,
                        "size_pct":or_pct,"volume":d.get("volume_raw",0),"built":True}
                    built+=1
        print(f"[ORB] Late OR built: {built} stocks")
    return jsonify({"success":True,"or_built":len(orb.or_data)})

@app.route("/api/strategy/stop",methods=["POST"])
def stop_strategy():
    global strategy_active
    strategy_active=False;save_state(False)
    return jsonify({"success":True})

@app.route("/api/emergency-stop",methods=["POST"])
def emergency_stop():
    global strategy_active
    strategy_active=False
    save_state(False)
    n=len(om.positions)
    om.exit_all("EMERGENCY_STOP")
    print(f"[APP] EMERGENCY STOP — strategy off, {n} positions exited")
    return jsonify({"success":True,"message":f"Strategy stopped, {n} positions exited","positions_exited":n})

@app.route("/api/strategy/signals")
def api_signals():
    return jsonify({"success":True,"signals":list(orb.active_signals.values()),"count":len(orb.active_signals)})

@app.route("/api/config",methods=["GET"])
def get_config():
    return jsonify({"success":True,"capital":trading_config["CAPITAL"],"risk_pct":trading_config["RISK_PCT"],
        "max_trades":trading_config["MAX_TRADES_DAY"],"paper_mode":om.paper_mode,"ui_settings":ui_settings,
        "risk_amount":round(trading_config["CAPITAL"]*trading_config["RISK_PCT"],2),
        "buying_power":trading_config["CAPITAL"]*trading_config["MIS_MARGIN"],
        "max_daily_loss":round(trading_config["CAPITAL"]*trading_config["MAX_DAILY_LOSS_PCT"],2)})

@app.route("/api/config",methods=["POST"])
def set_config():
    data=request.json
    if "capital" in data:
        cap=int(data["capital"])
        if not(1000<=cap<=10000000):return jsonify({"success":False,"error":"Capital must be Rs 1,000 to Rs 1 Crore"})
        if len(om.positions)>0:return jsonify({"success":False,"error":f"Cannot change capital with {len(om.positions)} open positions"})
        trading_config["CAPITAL"]=cap;orb.config["CAPITAL"]=cap;save_state(strategy_active)
    if "paper_mode" in data:om.set_paper_mode(bool(data["paper_mode"]))
    if "ui_settings" in data:ui_settings.update(data["ui_settings"]);save_state(strategy_active)
    return jsonify({"success":True,"capital":trading_config["CAPITAL"],"paper_mode":om.paper_mode,
        "ui_settings":ui_settings,
        "risk_amount":round(trading_config["CAPITAL"]*trading_config["RISK_PCT"],2),
        "buying_power":trading_config["CAPITAL"]*trading_config["MIS_MARGIN"],
        "max_daily_loss":round(trading_config["CAPITAL"]*trading_config["MAX_DAILY_LOSS_PCT"],2)})

@app.route("/api/orders/positions")
def get_positions():return jsonify({"success":True,"positions":om.get_positions()})

@app.route("/api/orders/place",methods=["POST"])
def place_order():
    try:
        data=request.json or {}
        sym=(data.get("symbol") or "").strip().upper()
        direction=(data.get("direction") or "").upper()
        quantity=int(data.get("quantity",1))
        price=float(data.get("price",0))
        if not sym or sym in("NULL","UNDEFINED"):
            return jsonify({"success":False,"error":"Invalid symbol"})
        if direction not in("BUY","SELL"):
            return jsonify({"success":False,"error":"Direction must be BUY or SELL"})
        if quantity<=0 or quantity>10000:
            return jsonify({"success":False,"error":"Quantity must be 1-10000"})
        if price<=0:
            return jsonify({"success":False,"error":"Price must be positive"})
        signal={"symbol":sym,"direction":direction,"quantity":quantity,
            "ltp":price,"sl_price":data.get("sl_price"),"target1":data.get("target1"),
            "target2":data.get("target2"),"target3":data.get("target3",0)}
        return jsonify(om.place_order(signal))
    except(ValueError,TypeError) as e:
        return jsonify({"success":False,"error":str(e)})

@app.route("/api/orders/exit",methods=["POST"])
def exit_pos():
    d=request.json or {}
    sym=d.get("symbol")
    pos=om.positions.get(sym)
    if not pos:return jsonify({"success":False,"error":"No position"})
    qty=d.get("quantity") or pos["qty_remaining"]
    price=d.get("price") or pos.get("last_ltp",pos["entry_price"])
    return jsonify(om.exit_position(sym,qty,price,d.get("reason","MANUAL")))

@app.route("/api/orders/exit-all",methods=["POST"])
def exit_all():om.exit_all("MANUAL");return jsonify({"success":True})

@app.route("/api/orders/tradelog")
def get_tradelog():return jsonify({"success":True,"trades":om.get_trade_log()})

@app.route("/api/pnl/daily")
def daily_pnl():return jsonify({"success":True,"daily":om.get_daily_report(),"total":om.get_total_pnl()})

@app.route("/api/pnl/today")
def today_pnl():
    today=om._today();d=om.daily_pnl.get(today,{})
    return jsonify({"success":True,"net_pnl":round(d.get("net_pnl",0),2),
        "gross_pnl":round(d.get("gross_pnl",0),2),"charges":round(d.get("charges",0),2),
        "trades":d.get("trades",0),"wins":d.get("wins",0),"losses":d.get("losses",0),
        "positions":len(om.positions)})

@app.route("/api/pnl/export")
def export_pnl():
    report=om.get_daily_report()
    csv="Date,Trades,Wins,Losses,Win Rate,Gross PnL,Charges,Net PnL\n"
    for d in report:csv+=f"{d['date']},{d['trades']},{d['wins']},{d['losses']},{d['win_rate']}%,{d['gross_pnl']},{d['charges']},{d['net_pnl']}\n"
    total=om.get_total_pnl()
    csv+=f"TOTAL,{total['total_trades']},{total['total_wins']},{total['total_losses']},{total['win_rate']}%,{total['total_gross']},{total['total_charges']},{total['total_net']}\n"
    return csv,200,{"Content-Type":"text/csv","Content-Disposition":"attachment; filename=pnl_report.csv"}

def _flattrade_get(path,extra={}):
    """Generic Flattrade REST call. Returns parsed JSON or raises."""
    if not session_token:raise RuntimeError("Not logged in")
    payload={"uid":CLIENT_ID,"actid":CLIENT_ID,**extra}
    jdata=f"jData={json.dumps(payload)}&jKey={session_token}"
    r=requests.post(f"https://piconnect.flattrade.in/PiConnectAPI/{path}",
        data=jdata,headers={"Content-Type":"application/x-www-form-urlencoded"},timeout=10)
    return r.json()

@app.route("/api/broker/orders")
def broker_orders():
    try:
        data=_flattrade_get("OrderBook")
        if isinstance(data,dict) and data.get("stat")=="Not_Ok":
            return jsonify({"success":True,"orders":[],"message":data.get("emsg","")})
        orders=[]
        for o in (data if isinstance(data,list) else []):
            orders.append({"order_id":o.get("norenordno"),"symbol":o.get("symname"),
                "side":"BUY" if o.get("trantype")=="B" else "SELL",
                "qty":int(o.get("qty",0)),"price":float(o.get("prc",0)),
                "status":o.get("status"),"product":o.get("s_prdt_ali"),
                "reject_reason":o.get("rejreason","").replace("RED:RULE:","").strip(),
                "fill_qty":int(o.get("fillshares","0") or 0),
                "avg_price":float(o.get("avgprc","0") or 0),
                "time":o.get("norentm")})
        return jsonify({"success":True,"orders":orders})
    except RuntimeError as e:return jsonify({"success":False,"error":str(e)})
    except Exception as e:return jsonify({"success":False,"error":str(e)})

@app.route("/api/broker/tradebook")
def broker_tradebook():
    try:
        data=_flattrade_get("TradeBook")
        if isinstance(data,dict) and data.get("stat")=="Not_Ok":
            return jsonify({"success":True,"trades":[],"message":data.get("emsg","")})
        trades=[]
        for t in (data if isinstance(data,list) else []):
            trades.append({"order_id":t.get("norenordno"),"symbol":t.get("symname"),
                "side":"BUY" if t.get("trantype")=="B" else "SELL",
                "qty":int(t.get("qty",0)),"fill_price":float(t.get("flprc","0") or 0),
                "fill_qty":int(t.get("fillshares","0") or 0),
                "product":t.get("s_prdt_ali"),"time":t.get("fltm")})
        return jsonify({"success":True,"trades":trades})
    except RuntimeError as e:return jsonify({"success":False,"error":str(e)})
    except Exception as e:return jsonify({"success":False,"error":str(e)})

@app.route("/api/broker/funds")
def broker_funds():
    try:
        data=_flattrade_get("Limits")
        if data.get("stat")!="Ok":
            return jsonify({"success":False,"error":data.get("emsg","Limits failed")})
        cash=float(data.get("cash","0"))
        payin=float(data.get("payin","0"))
        used=float(data.get("marginused","0") or 0)
        return jsonify({"success":True,"cash":round(cash,2),"payin":round(payin,2),
            "used":round(used,2),"available":round(cash+payin-used,2),
            "name":data.get("prfname","")})
    except RuntimeError as e:return jsonify({"success":False,"error":str(e)})
    except Exception as e:return jsonify({"success":False,"error":str(e)})

@app.route("/api/broker/positions")
def broker_positions():
    try:
        data=_flattrade_get("PositionBook")
        if isinstance(data,dict) and data.get("stat")=="Not_Ok":
            return jsonify({"success":True,"positions":[],"message":data.get("emsg","")})
        positions=[]
        for p in (data if isinstance(data,list) else []):
            netqty=int(p.get("netqty","0") or 0)
            if netqty==0:continue
            entry=float(p.get("netupldprc","0") or 0)
            ltp=float(p.get("lp","0") or 0)
            pnl=float(p.get("rpnl","0") or 0)+(ltp-entry)*netqty if ltp and entry else 0
            positions.append({"symbol":p.get("tsym","").replace("-EQ",""),
                "qty":netqty,"entry_price":round(entry,2),"ltp":round(ltp,2),
                "product":p.get("s_prdt_ali"),"unrealised_pnl":round(pnl,2),
                "realised_pnl":round(float(p.get("rpnl","0") or 0),2)})
        return jsonify({"success":True,"positions":positions})
    except RuntimeError as e:return jsonify({"success":False,"error":str(e)})
    except Exception as e:return jsonify({"success":False,"error":str(e)})

@app.route("/api/ws/status")
def ws_status():
    if ws_engine:return jsonify({"success":True,**ws_engine.status})
    return jsonify({"success":True,"connected":False,"subscribed":0})

@app.route("/api/ws/restart",methods=["POST"])
def restart_ws():
    global ws_engine
    if not session_token:return jsonify({"success":False,"error":"Not logged in"})
    if ws_engine:ws_engine.stop();time.sleep(1)
    ws_engine=FlattradeWebSocket(session_token,CLIENT_ID,on_tick,on_candle_close,on_vwap_update=orb.update_vwap)
    ws_engine.start()
    threading.Thread(target=start_ws_subscription,daemon=True).start()
    return jsonify({"success":True,"message":"WS restarted"})

if __name__=="__main__":
    print("[Server] NSE Market Watch v3.0 starting...")
    def _startup():
        global ws_engine
        time.sleep(5)
        fetch_asm_gsm_list()
        # Auto-load NSE top 500 at startup — no login needed
        print("[Startup] Loading NSE top 500 stocks...")
        for attempt in range(5):
            try:
                s=requests.Session()
                s.get("https://www.nseindia.com",headers=NSE_HEADERS,timeout=5)
                r=s.get("https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20500",
                    headers=NSE_HEADERS,timeout=15)
                if r.status_code!=200:
                    print(f"[Startup] NSE HTTP {r.status_code} — market closed?")
                    break
                items=r.json().get("data",[])
                # Sort by value descending, take top 500
                items_sorted=sorted(
                    [i for i in items if i.get("symbol") and float(i.get("totalTradedValue",0) or 0)>0],
                    key=lambda x:float(x.get("totalTradedValue",0) or 0),reverse=True)[:500]
                for item in items_sorted:
                    sym=item.get("symbol","")
                    if sym:
                        with lock:
                            market_data[sym]={
                                "symbol":sym,
                                "ltp":float(item.get("lastPrice",0) or 0),
                                "open":float(item.get("open",0) or 0),
                                "high":float(item.get("dayHigh",0) or 0),
                                "low":float(item.get("dayLow",0) or 0),
                                "prev_close":float(item.get("previousClose",0) or 0),
                                "change":float(item.get("change",0) or 0),
                                "change_pct":float(item.get("pChange",0) or 0),
                                "volume_raw":int(item.get("totalTradedVolume",0) or 0),
                                "value":float(item.get("totalTradedValue",0) or 0),
                                "dir":"","updated":datetime.datetime.now().isoformat()}
                print(f"[Startup] Loaded {len(market_data)} stocks (top 500 by value)")
                try:
                    syms=[s for s in market_data if not is_index(s)]
                    with open(TOP500_CACHE,"w") as f: json.dump(syms,f)
                except Exception: pass
                break
            except Exception as e:
                print(f"[Startup] NSE load attempt {attempt+1}/5: {e}")
                time.sleep(2**attempt)
        # If NSE failed, seed from cached top-500 list or full master file
        if len(market_data) < 10:
            seeded=0
            if os.path.exists(TOP500_CACHE):
                try:
                    with open(TOP500_CACHE) as f: cached=json.load(f)
                    for sym in cached:
                        with lock:
                            market_data[sym]={"symbol":sym,"ltp":0,"vwap":0,
                                "volume_raw":0,"value":0,"change":0,"change_pct":0,
                                "dir":"","seeded":True}
                            seeded+=1
                    print(f"[Startup] Seeded {seeded} symbols from top-500 cache")
                except Exception as e:
                    print(f"[Startup] Cache load error: {e}")
            if len(market_data) < 10:
                print("[Startup] NSE unavailable — seeding from master file sorted by last-known value")
                import csv
                # Load last-known values from snapshot for ranking
                snap_values={}
                if os.path.exists(PRICES_SNAPSHOT):
                    try:
                        with open(PRICES_SNAPSHOT) as f: snap=json.load(f)
                        snap_values={s:float(d.get("value",0) or 0) for s,d in snap.items()}
                    except Exception: pass
                if os.path.exists(NSE_MASTER_FILE):
                    all_syms=[]
                    with open(NSE_MASTER_FILE,"r") as f:
                        reader=csv.reader(f)
                        next(reader)
                        for row in reader:
                            if len(row)<6:continue
                            if row[0]=="NSE" and row[5].strip()=="EQ":
                                sym=row[4].replace("-EQ","").strip()
                                all_syms.append(sym)
                    # Sort by last-known value descending; unknowns go to bottom
                    all_syms.sort(key=lambda s:snap_values.get(s,0),reverse=True)
                    count=0
                    for sym in all_syms:
                        if count>=500:break
                        with lock:
                            if sym not in market_data:
                                market_data[sym]={"symbol":sym,"ltp":0,"vwap":0,
                                    "volume_raw":0,"value":0,"change":0,"change_pct":0,
                                    "dir":"","seeded":True}
                                count+=1
                    ranked=sum(1 for s in list(market_data)[:500] if snap_values.get(s,0)>0)
                    print(f"[Startup] Seeded {count} symbols from master file ({ranked} ranked by last-known value)")
        restore_prices_snapshot()
        # Run Yahoo fetch if volume or range data is missing — covers fresh starts and snapshot gaps
        if not any(d.get("volume_raw",0)>0 for d in market_data.values()) or not any(d.get("range_pct",0)>0 for d in market_data.values()):
            threading.Thread(target=fetch_prices_from_yahoo,daemon=True).start()
        # Auto-reconnect WS if we restored a token from today
        if session_token and not ws_engine:
            print("[Startup] Restoring WS with saved token...")
            ws_engine=FlattradeWebSocket(session_token,CLIENT_ID,on_tick,on_candle_close,on_vwap_update=orb.update_vwap)
            ws_engine.start()
            threading.Thread(target=start_ws_subscription,daemon=True).start()
    threading.Thread(target=_startup,daemon=True).start()
    threading.Thread(target=master_clock_loop,daemon=True).start()
    print("[Server] Master clock started")
    threading.Thread(target=fetch_nse_loop,daemon=True).start()
    threading.Thread(target=position_sync_loop,daemon=True).start()
    print("[Server] Position sync started")
    print("[Server] Running at: http://43.205.180.54:5000")
    app.run(host="0.0.0.0",port=5000,debug=False,threaded=True)
