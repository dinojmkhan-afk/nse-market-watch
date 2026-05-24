import datetime,threading,json,time,requests
from collections import defaultdict
FLATTRADE_URL="https://piconnect.flattrade.in/PiConnectTP/PlaceOrder"
FLATTRADE_LIMITS_URL="https://piconnect.flattrade.in/PiConnectTP/Limits"
HISTORY_FILE="/home/ubuntu/market-watch/.trade_history.json"

class OrderManager:
    def __init__(self,client_id,get_token_fn,paper_mode=True):
        self.client_id=client_id;self.get_token=get_token_fn
        self.paper_mode=paper_mode;self.lock=threading.Lock()
        self.positions={};self.trade_log=[];self.daily_pnl={}
        self.today_str=self._today()
        self._load_history()  # Load saved history
        self._ensure_today()
        self.on_fill=None;self.on_exit=None;self.on_sl_hit=None;self.on_target=None
        self.recent_orders={}   # {symbol: timestamp} duplicate protection
        self.order_cooldown=5   # seconds between orders for same symbol
        threading.Thread(target=self._monitor,daemon=True).start()
        print("[OM] Started")
    def _today(self):
        return (datetime.datetime.now(datetime.timezone.utc).astimezone(datetime.timezone(datetime.timedelta(hours=5,minutes=30)))).strftime("%Y-%m-%d")
    def _ist(self):return datetime.datetime.now(datetime.timezone.utc).astimezone(datetime.timezone(datetime.timedelta(hours=5,minutes=30)))
    def _ensure_today(self):
        if self.today_str not in self.daily_pnl:
            self.daily_pnl[self.today_str]={"date":self.today_str,"trades":0,"wins":0,"losses":0,"gross_pnl":0.0,"charges":0.0,"net_pnl":0.0,"trade_log":[]}
    def _charges(self,qty,price,direction):
        v=qty*price
        return round(v*0.00025*(1 if direction=="SELL" else 0)+v*0.0000297+v*0.000001+v*0.00003*(1 if direction=="BUY" else 0),2)
    def get_available_cash(self):
        """Fetch available cash from Flattrade Limits API"""
        try:
            token=self.get_token()
            if not token:return None
            payload={"uid":self.client_id,"actid":self.client_id}
            jdata=f"jData={json.dumps(payload)}&jKey={token}"
            r=requests.post(FLATTRADE_LIMITS_URL,data=jdata,
                headers={"Content-Type":"application/x-www-form-urlencoded"},timeout=5)
            resp=r.json()
            if resp.get("stat")=="Ok":
                cash=float(resp.get("cash","0"))
                marginused=float(resp.get("marginused","0"))
                available=cash-marginused
                print(f"[OM] Limits: cash={cash:.0f} used={marginused:.0f} available={available:.0f}")
                return available
        except Exception as e:
            print(f"[OM] Limits API error: {e}")
        return None

    def place_order(self,signal):
        sym=signal["symbol"];direction=signal["direction"]
        qty=signal["quantity"];price=signal["ltp"]
        sl=signal["sl_price"];t1=signal["target1"];t2=signal["target2"];t3=signal["target3"]
        # Duplicate order protection
        now=time.time()
        if sym in self.recent_orders:
            elapsed=now-self.recent_orders[sym]
            if elapsed<self.order_cooldown:
                print(f"[OM] DUPLICATE BLOCKED: {sym} ordered {elapsed:.1f}s ago")
                return{"success":False,"error":f"Duplicate order — wait {self.order_cooldown}s"}
        if sym in self.positions:return{"success":False,"error":f"{sym} already open"}
        # Check available cash before placing real order
        if not self.paper_mode:
            margin=signal.get("margin",4)
            required_cash=round((qty*price)/margin,2)
            available=self.get_available_cash()
            if available is not None and available<required_cash:
                msg=f"{sym} SKIPPED: need Rs{required_cash:.0f} cash but only Rs{available:.0f} available"
                print(f"[OM] {msg}")
                return{"success":False,"error":msg}
            elif available is not None:
                print(f"[OM] Cash OK: need Rs{required_cash:.0f} available Rs{available:.0f}")
        result=self._paper(sym,direction,qty,price) if self.paper_mode else self._real(sym,direction,qty,price)
        if result["success"]:
            fp=result.get("fill_price",price)
            qt1=int(qty*0.40);qt2=int(qty*0.40);qt3=qty-qt1-qt2
            pos={"symbol":sym,"direction":direction,"qty":qty,"qty_t1":qt1,"qty_t2":qt2,"qty_t3":qt3,
                "qty_remaining":qty,"entry_price":fp,"sl_price":sl,"target1":t1,"target2":t2,"target3":t3,
                "t1_done":False,"t2_done":False,"t3_done":False,"status":"OPEN","paper_mode":self.paper_mode,
                "entry_time":self._ist().isoformat(),"order_id":result.get("order_id","PAPER"),"last_ltp":fp,
                "charges":self._charges(qty,fp,direction)}
            with self.lock:self.positions[sym]=pos
            self._ensure_today()
            self.recent_orders[sym]=time.time()  # Record for cooldown
            if self.on_fill:self.on_fill(pos)
            print(f"[OM] {'PAPER' if self.paper_mode else 'REAL'} FILLED:{direction} {qty} {sym}@{fp}")
        return result
    def _paper(self,sym,direction,qty,price):
        print(f"[OM] PAPER FILLED: {direction} {qty} {sym} @ {price}")
        return{"success":True,"fill_price":price,"order_id":f"PAPER-{sym}-{int(time.time())}","paper":True}
    def _real(self,sym,direction,qty,price):
        try:
            token=self.get_token()
            if not token:return{"success":False,"error":"Not logged in"}
            import urllib.parse
            tsym=urllib.parse.quote(f"{sym}-EQ",safe="-")
            payload={"uid":self.client_id,"actid":self.client_id,"exch":"NSE","tsym":tsym,
                "qty":str(qty),"prc":str(round(price*1.001,2)),"prd":"I",
                "trantype":"B" if direction=="BUY" else "S","prctyp":"LMT","ret":"DAY","remarks":"ORB-Auto","ordersource":"API"}
            jdata=f"jData={json.dumps(payload)}&jKey={token}"
            r=requests.post(FLATTRADE_URL,data=jdata,headers={"Content-Type":"application/x-www-form-urlencoded"},timeout=10)
            raw=r.text.strip()
            print(f"[OM] Real raw: {raw[:200]}")
            if not raw:
                return{"success":False,"error":"No response from Flattrade — market may be closed","rejected":False}
            try:resp=json.loads(raw)
            except:return{"success":False,"error":f"Bad response: {raw[:100]}","rejected":False}
            print(f"[OM] Real:{resp}")
            if resp.get("stat")=="Ok":return{"success":True,"fill_price":price,"order_id":resp.get("norenordno",""),"paper":False}
            emsg=resp.get("emsg","Order rejected")
            # Check if MIS not allowed
            mis_errors=["MIS not allowed","not allowed for intraday","product type not allowed",
                       "scrip not allowed","asm","gsm","t2t","circuit","not eligible"]
            is_mis_error=any(e.lower() in emsg.lower() for e in mis_errors)
            print(f"[OM] REJECTED ({('MIS_NOT_ALLOWED' if is_mis_error else 'OTHER')}): {emsg}")
            return{"success":False,"error":emsg,"rejected":True,"mis_not_allowed":is_mis_error}
        except requests.exceptions.Timeout:
            return{"success":False,"error":"Order timeout — check Flattrade app manually!","rejected":False}
        except requests.exceptions.ConnectionError:
            return{"success":False,"error":"Network error — check connection!","rejected":False}
        except Exception as e:return{"success":False,"error":str(e)}
    def exit_position(self,sym,qty,price,reason="MANUAL"):
        if sym not in self.positions:return{"success":False,"error":"No position"}
        pos=self.positions[sym];exit_dir="SELL" if pos["direction"]=="BUY" else "BUY"
        if self.paper_mode:
            result={"success":True,"fill_price":price}
        else:
            result=self._real(sym,exit_dir,qty,price)
            # Smart retry — network errors only
            if not result["success"]:
                err=result.get("error","").lower()
                if "timeout" in err or "network" in err or "connection" in err:
                    print(f"[OM] Network error exiting {sym}, retrying in 2s...")
                    time.sleep(2)
                    result=self._real(sym,exit_dir,qty,price)
                    if not result["success"]:
                        print(f"[OM] EXIT RETRY FAILED {sym}: {result.get('error')} — MANUAL ACTION REQUIRED!")
                        return result
                else:
                    print(f"[OM] Exit rejected {sym}: {result.get('error')} — not retrying")
                    return result

        # PnL calculation runs for BOTH paper and real mode
        fp=result.get("fill_price",price)
        pnl=(fp-pos["entry_price"])*qty if pos["direction"]=="BUY" else (pos["entry_price"]-fp)*qty
        charges=self._charges(qty,fp,exit_dir)
        net=pnl-charges
        pos["qty_remaining"]-=qty
        if pos["qty_remaining"]<=0:
            pos.update({"status":"CLOSED","exit_price":fp,"exit_time":self._ist().isoformat(),
                "gross_pnl":round(pnl,2),"charges":round(charges,2),"net_pnl":round(net,2),"reason":reason})
            self._record(pos)
            with self.lock:del self.positions[sym]
            if self.on_exit:self.on_exit(pos)
        print(f"[OM] EXIT: {sym} {qty}@{fp} PnL:{pnl:+.2f} Net:{net:+.2f} [{reason}]")
        return result
    def exit_all(self,reason="FORCE_EXIT"):
        for sym in list(self.positions.keys()):
            pos=self.positions.get(sym)
            if pos:self.exit_position(sym,pos["qty_remaining"],pos.get("last_ltp",pos["entry_price"]),reason)
    def update_ltp(self,sym,ltp):
        if sym not in self.positions:return
        pos=self.positions[sym];pos["last_ltp"]=ltp
        if pos["status"]!="OPEN":return
        if pos["direction"]=="BUY":self._chk_buy(sym,pos,ltp)
        else:self._chk_sell(sym,pos,ltp)
    def _chk_buy(self,sym,pos,ltp):
        if ltp<=pos["sl_price"]:self.exit_position(sym,pos["qty_remaining"],ltp,"SL_HIT");(self.on_sl_hit and self.on_sl_hit(sym,ltp,pos));return
        if not pos["t1_done"] and ltp>=pos["target1"]:
            self.exit_position(sym,pos["qty_t1"],ltp,"T1_HIT");pos["t1_done"]=True;pos["sl_price"]=pos["entry_price"]
            self.on_target and self.on_target(sym,"T1",ltp,pos)
        if pos["t1_done"] and not pos["t2_done"] and ltp>=pos["target2"]:
            self.exit_position(sym,pos["qty_t2"],ltp,"T2_HIT");pos["t2_done"]=True;pos["sl_price"]=pos["target1"]
            self.on_target and self.on_target(sym,"T2",ltp,pos)
        if pos["t2_done"] and not pos["t3_done"] and ltp>=pos["target3"]:
            self.exit_position(sym,pos["qty_remaining"],ltp,"T3_HIT");pos["t3_done"]=True
            self.on_target and self.on_target(sym,"T3",ltp,pos)
    def _chk_sell(self,sym,pos,ltp):
        if ltp>=pos["sl_price"]:self.exit_position(sym,pos["qty_remaining"],ltp,"SL_HIT");(self.on_sl_hit and self.on_sl_hit(sym,ltp,pos));return
        if not pos["t1_done"] and ltp<=pos["target1"]:
            self.exit_position(sym,pos["qty_t1"],ltp,"T1_HIT");pos["t1_done"]=True;pos["sl_price"]=pos["entry_price"]
            self.on_target and self.on_target(sym,"T1",ltp,pos)
        if pos["t1_done"] and not pos["t2_done"] and ltp<=pos["target2"]:
            self.exit_position(sym,pos["qty_t2"],ltp,"T2_HIT");pos["t2_done"]=True;pos["sl_price"]=pos["target1"]
            self.on_target and self.on_target(sym,"T2",ltp,pos)
        if pos["t2_done"] and not pos["t3_done"] and ltp<=pos["target3"]:
            self.exit_position(sym,pos["qty_remaining"],ltp,"T3_HIT");pos["t3_done"]=True
            self.on_target and self.on_target(sym,"T3",ltp,pos)
    def _load_history(self):
        """Load trade history from file"""
        try:
            import json as _j
            if __import__("os").path.exists(HISTORY_FILE):
                data=_j.load(open(HISTORY_FILE))
                self.daily_pnl=data.get("daily_pnl",{})
                self.trade_log=data.get("trade_log",[])
                print(f"[OM] Loaded history: {len(self.daily_pnl)} days, {len(self.trade_log)} trades")
        except Exception as e:
            print(f"[OM] History load error: {e}")

    def _save_history(self):
        """Save trade history to file with backup"""
        try:
            import json as _j,shutil,os
            data={"daily_pnl":self.daily_pnl,"trade_log":self.trade_log}
            tmp=HISTORY_FILE+".tmp"
            _j.dump(data,open(tmp,"w"))
            # Gap 6: Atomic write — backup then replace
            if os.path.exists(HISTORY_FILE):
                shutil.copy2(HISTORY_FILE,HISTORY_FILE+".bak")
            os.replace(tmp,HISTORY_FILE)
        except Exception as e:
            print(f"[OM] History save error: {e}")

    def _monitor(self):
        while True:
            try:
                now=self._ist();mins=now.hour*60+now.minute
                if mins>=14*60+55 and self.positions:
                    print("[OM] FORCE EXIT 2:55PM!");self.exit_all("FORCE_EXIT_2:55PM")
                today=self._today()
                if today!=self.today_str:
                    self.today_str=today
                    self._ensure_today()
                    self._save_history()  # Save on day change
            except Exception as e:print(f"[OM] Monitor err:{e}")
            time.sleep(30)
    def _record(self,pos):
        self._ensure_today();today=self.daily_pnl[self.today_str]
        net=pos.get("net_pnl",0);today["trades"]+=1
        today["gross_pnl"]+=pos.get("gross_pnl",0);today["charges"]+=pos.get("charges",0)
        today["net_pnl"]+=net
        if net>=0:today["wins"]+=1
        else:today["losses"]+=1
        rec={"symbol":pos["symbol"],"direction":pos["direction"],"qty":pos["qty"],
            "entry_price":pos["entry_price"],"exit_price":pos.get("exit_price",0),
            "entry_time":pos["entry_time"],"exit_time":pos.get("exit_time",""),
            "gross_pnl":pos.get("gross_pnl",0),"charges":pos.get("charges",0),
            "net_pnl":net,"reason":pos.get("reason",""),"paper_mode":pos["paper_mode"],"date":self.today_str}
        today["trade_log"].append(rec);self.trade_log.append(rec)
        self._save_history()  # Save to file immediately
    def get_daily_report(self):
        self._ensure_today()
        return[{"date":d,"trades":v["trades"],"wins":v["wins"],"losses":v["losses"],
            "gross_pnl":round(v["gross_pnl"],2),"charges":round(v["charges"],2),
            "net_pnl":round(v["net_pnl"],2),"win_rate":round(v["wins"]/v["trades"]*100,1) if v["trades"]>0 else 0}
            for d,v in sorted(self.daily_pnl.items(),reverse=True)]
    def get_total_pnl(self):
        tg=sum(d["gross_pnl"] for d in self.daily_pnl.values())
        tc=sum(d["charges"] for d in self.daily_pnl.values())
        tn=sum(d["net_pnl"] for d in self.daily_pnl.values())
        tt=sum(d["trades"] for d in self.daily_pnl.values())
        tw=sum(d["wins"] for d in self.daily_pnl.values())
        return{"total_gross":round(tg,2),"total_charges":round(tc,2),"total_net":round(tn,2),
            "total_trades":tt,"total_wins":tw,"total_losses":tt-tw,
            "win_rate":round(tw/tt*100,1) if tt>0 else 0}
    def get_positions(self):return list(self.positions.values())
    def get_trade_log(self,date=None):
        if date:return self.daily_pnl.get(date,{}).get("trade_log",[])
        return self.trade_log
    def set_paper_mode(self,paper):
        self.paper_mode=paper;print(f"[OM] Mode:{'PAPER' if paper else 'REAL'}")
    @property
    def status(self):
        self._ensure_today();today=self.daily_pnl[self.today_str]
        return{"paper_mode":self.paper_mode,"positions":len(self.positions),
            "trades_today":today["trades"],"pnl_today":round(today["net_pnl"],2),
            "wins_today":today["wins"],"losses_today":today["losses"]}
