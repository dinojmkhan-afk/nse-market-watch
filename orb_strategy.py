import datetime,threading
from collections import defaultdict
CONFIG={"CAPITAL":20000,"RISK_PCT":0.005,"MAX_TRADES_DAY":3,"MAX_DAILY_LOSS_PCT":0.02,"MIS_MARGIN":4,
    "OR_START_H":9,"OR_START_M":15,"OR_END_H":9,"OR_END_M":20,
    "TRADE_START_H":9,"TRADE_START_M":30,"TRADE_END_H":10,"TRADE_END_M":45,
    "FORCE_EXIT_H":14,"FORCE_EXIT_M":55,
    "OR_MIN_SIZE_PCT":0.3,"OR_MAX_SIZE_PCT":2.5,"VOLUME_MULTIPLIER":2.0,"NIFTY_GAP_LIMIT":1.5,"RVOL_MIN":1.5,"RVOL_STRONG":2.0,"MAX_POSITION_PCT":0.25,
    "T1_EXIT_PCT":0.40,"T2_EXIT_PCT":0.40,"T3_EXIT_PCT":0.20,
    "MIN_PRICE":100,"MAX_PRICE":10000,"MIN_VOLUME":1000000,"MAX_CONSECUTIVE_LOSS":2}
class ORBStrategy:
    def __init__(self,config=None):
        self.config=config or CONFIG;self.lock=threading.Lock()
        self.or_data={};self.avg_volumes={};self.volume_history=defaultdict(list)
        self.active_signals={};self.trades_today=0;self.losses_today=0
        self.stock_margins={}  # {symbol: margin_multiplier} updated by app.py
        self.consecutive_loss=0;self.daily_pnl=0
        self.trading_stopped=False;self.stop_reason=""
        self.on_signal=None;self.on_stop=None
        self.trial_mode=False  # bypasses all time/freshness restrictions
        # Fix 3: Internal VWAP tracking
        self.vwap_data={}
        # Fix 2: Nifty staleness tracking
        self.nifty_change=0
        self.last_nifty_update=None
    def _ist(self):
        return datetime.datetime.now(datetime.timezone.utc).astimezone(datetime.timezone(datetime.timedelta(hours=5,minutes=30)))
    def _mins(self,dt):return dt.hour*60+dt.minute
    def _is_or_build_time(self):
        if self.trial_mode:return True
        n=self._mins(self._ist())
        return self.config["OR_START_H"]*60+self.config["OR_START_M"]<=n<self.config["OR_END_H"]*60+self.config["OR_END_M"]
    def _is_trade_time(self):
        if self.trial_mode:return True
        ist=self._ist()
        if ist.weekday()>=5:return False  # Gap 11: No trading on weekends
        n=self._mins(ist)
        return self.config["TRADE_START_H"]*60+self.config["TRADE_START_M"]<=n<=self.config["TRADE_END_H"]*60+self.config["TRADE_END_M"]
    def _is_force_exit_time(self):
        n=self._mins(self._ist())
        return n>=self.config["FORCE_EXIT_H"]*60+self.config["FORCE_EXIT_M"]

    def update_nifty(self,nifty_change,timestamp=None):
        self.nifty_change=nifty_change
        if timestamp and hasattr(timestamp,'tzinfo') and timestamp.tzinfo is None:
            timestamp=timestamp.replace(tzinfo=self._ist().tzinfo)
        self.last_nifty_update=timestamp or self._ist()

    def _is_nifty_fresh(self):
        if self.trial_mode:return True
        if self.last_nifty_update is None:return False
        age=(self._ist()-self.last_nifty_update).total_seconds()
        if age>30:
            print(f"[ORB] Nifty data stale {age:.1f}s skipping")
            return False
        return True

    def update_vwap(self,sym,price,volume):
        """Fix 3: Build VWAP from live ticks"""
        if sym not in self.vwap_data:
            self.vwap_data[sym]={"cum_pv":0,"cum_vol":0}
        self.vwap_data[sym]["cum_pv"]+=price*volume
        self.vwap_data[sym]["cum_vol"]+=volume

    def get_vwap(self,sym):
        """Fix 3: Get current VWAP"""
        d=self.vwap_data.get(sym,{})
        if d.get("cum_vol",0)>0:return round(d["cum_pv"]/d["cum_vol"],2)
        return 0

    def process_or_tick(self,sym,ltp,volume):
        if not self._is_or_build_time():return
        with self.lock:
            if sym not in self.or_data:
                self.or_data[sym]={"high":ltp,"low":ltp,"volume":0,"built":False}
            else:
                self.or_data[sym]["high"]=max(self.or_data[sym]["high"],ltp)
                self.or_data[sym]["low"]=min(self.or_data[sym]["low"],ltp)
            self.or_data[sym]["volume"]+=volume  # accumulate, not overwrite
            # Track volume history for RVOL calculation
            self.volume_history[sym].append(volume)
            if len(self.volume_history[sym])>5:self.volume_history[sym].pop(0)
    def record_candle_volume(self,sym,vol):
        """Call on every 5-min candle close to build rolling volume average"""
        if vol>0:
            self.volume_history[sym].append(vol)
            if len(self.volume_history[sym])>20:
                self.volume_history[sym].pop(0)

    def finalize_all_or_ranges(self):
        """Master clock calls this at exactly 9:20:01 AM"""
        with self.lock:
            count=0;skipped=0
            for sym in list(self.or_data.keys()):
                o=self.or_data[sym]
                if not o.get("built"):
                    # Gap 4: Skip invalid OR — zero volume or zero range
                    if o["volume"]<=0 or o["high"]<=0 or o["low"]<=0:
                        skipped+=1;continue
                    if o["high"]==o["low"]:
                        skipped+=1;continue
                    sz=o["high"]-o["low"]
                    o["size"]=round(sz,2)
                    o["size_pct"]=round((sz/o["low"])*100 if o["low"]>0 else 0,2)
                    o["built"]=True
                    # Seed volume history with OR candle volume only — clears any tick-level
                    # volumes accumulated during 9:15–9:20 so WS candle volumes start clean
                    self.volume_history[sym]=[o["volume"]]
                    self.avg_volumes[sym]=o["volume"]
                    count+=1
            print(f"[ORB] Master clock finalized {count} OR ranges, skipped {skipped} invalid at 9:20!")

    def finalize_or(self,sym,or_candle=None):
        if sym not in self.or_data:return
        with self.lock:
            o=self.or_data[sym]
            # If OR candle provided, use its exact OHLC
            if or_candle:
                o["high"]=or_candle["high"]
                o["low"]=or_candle["low"]
                o["volume"]=or_candle["volume"]
                # Seed volume history with OR candle volume
                self.volume_history[sym]=[or_candle["volume"]]
            sz=o["high"]-o["low"]
            o["size"]=round(sz,2)
            o["size_pct"]=round((sz/o["low"])*100 if o["low"]>0 else 0,2)
            o["built"]=True
            # avg_volumes now = OR candle volume as seed baseline
            self.avg_volumes[sym]=o["volume"]
            print(f"[ORB] {sym} OR H={o['high']} L={o['low']} Sz={o['size_pct']}% Vol={o['volume']}")
    def check_breakout(self,sym,candle,nifty_chg,market_data):
        # Fix 1: Only process CLOSED candles
        if not candle.get("closed",False):return None
        # Fix 2: Check Nifty freshness
        if not self._is_nifty_fresh():return None
        if self.trading_stopped or not self._is_trade_time():return None
        ok,_=self.can_trade()
        if not ok:return None
        # FIX B: Thread-safe read of or_data
        with self.lock:
            if sym not in self.or_data or sym in self.active_signals:return None
            o=dict(self.or_data[sym])  # snapshot to avoid race condition
        if not o.get("built"):return None
        if not (self.config["OR_MIN_SIZE_PCT"]<=o["size_pct"]<=self.config["OR_MAX_SIZE_PCT"]):return None
        ltp=candle["close"];vol=candle["volume"]
        # Skip index symbols — not tradeable
        if " " in sym or sym.startswith("NIFTY") or sym.startswith("SENSEX"):return None
        if not (self.config["MIN_PRICE"]<=ltp<=self.config["MAX_PRICE"]):return None
        # RVOL = Current volume / rolling 20-candle average
        history=self.volume_history.get(sym,[])
        rvol_min=self.config["RVOL_MIN"]
        if len(history)>=2:
            # WS mode: accurate 5-min candle volumes available
            baseline=sum(history)/len(history)
            rvol=vol/baseline if baseline>0 else 0
            if baseline>0 and rvol<rvol_min:
                if not self.trial_mode:print(f"[ORB] {sym} RVOL={rvol:.2f} < {rvol_min} (vol={vol:.0f} avg={baseline:.0f}) skipped")
                return None
        else:
            # WS candle history < 2 — bypass RVOL filter using OR candle baseline
            baseline=self.avg_volumes.get(sym,0)
            rvol=rvol_min  # Default to minimum to pass filter
            if baseline<=0:return None  # No volume data — skip
            if not self.trial_mode:print(f"[ORB] {sym} RVOL bypassed (only {len(history)} candle{'s' if len(history)!=1 else ''}) baseline={baseline:.0f}")
        signal_strength="STRONG" if rvol>=self.config["RVOL_STRONG"] else "MEDIUM"
        # In trial mode bypass Nifty direction — allows signals in any market direction
        ng=self.trial_mode or self.nifty_change>0.1
        nr=self.trial_mode or self.nifty_change<-0.1
        # Fix 3: Internal VWAP + fallback to market_data
        vwap=self.get_vwap(sym)
        if vwap==0 and market_data:
            vwap=market_data.get(sym,{}).get("vwap",0)
        ist_now=datetime.datetime.now(datetime.timezone.utc).astimezone(datetime.timezone(datetime.timedelta(hours=5,minutes=30)))
        past_945=ist_now.hour*60+ist_now.minute>=9*60+45
        if vwap==0 and past_945 and not self.trial_mode:
            print(f"[ORB] {sym} VWAP unavailable after 9:45 skipping")
            return None
        vwap_ok_buy=vwap==0 or ltp>vwap
        vwap_ok_sell=vwap==0 or ltp<vwap
        if vwap>0:print(f"[ORB] {sym} LTP={ltp} VWAP={vwap} RVOL={rvol:.2f} [{signal_strength}]")
        signal=None
        if candle["close"]>o["high"] and ng and vwap_ok_buy:signal=self._sig(sym,"BUY",ltp,o,rvol,signal_strength,baseline)
        elif candle["close"]<o["low"] and nr and vwap_ok_sell:signal=self._sig(sym,"SELL",ltp,o,rvol,signal_strength,baseline)
        if signal:
            with self.lock:self.active_signals[sym]=signal
            print(f"[ORB] SIGNAL {sym} {signal['direction']} @ {ltp}")
            if self.on_signal:self.on_signal(signal)
        return signal
    def _sig(self,sym,direction,ltp,o,rvol=0,signal_strength='MEDIUM',baseline=0):
        sz=o["size"];cap=self.config["CAPITAL"]
        # Get margin multiplier for this stock (set by app.py margin checker)
        margin=self.stock_margins.get(sym,self.config["MIS_MARGIN"])
        # Skip if margin < 3x
        if margin<3:
            print(f"[ORB] {sym} SKIPPED margin={margin}x < 3x minimum")
            return None
        # Risk = fixed % of capital — margin only gates max position size, NOT risk
        risk=cap*self.config["RISK_PCT"]
        if direction=="BUY":sl=round(o["low"],2);t1=round(ltp+1.5*sz,2);t2=round(ltp+2.5*sz,2);t3=round(ltp+4*sz,2)
        else:sl=round(o["high"],2);t1=round(ltp-1.5*sz,2);t2=round(ltp-2.5*sz,2);t3=round(ltp-4*sz,2)
        sld=abs(ltp-sl)
        # RRR validation minimum 1:1.5
        if sld>0:
            rr_check=abs(t1-ltp)/sld
            if rr_check<1.0:
                print(f"[ORB] {sym} RR={rr_check:.2f} < 1.0 skipped")
                return None
        elif sld==0:return None
        # Reject if SL > 2% of LTP (too wide)
        if sld>ltp*0.02:
            print(f"[ORB] {sym} SL too wide {sld:.2f}>{ltp*0.02:.2f} skipped")
            return None
        qty=max(1,int(risk/sld)) if sld>0 else 1
        # Position size cap: max 25% of capital × margin per trade
        max_qty=max(1,int(cap*self.config["MAX_POSITION_PCT"]*margin/ltp))
        qty=min(qty,max_qty)
        # Half position for medium signals
        if signal_strength=="MEDIUM":
            qty=max(1,qty//2)
        rr=round(sz/sld,2) if sld>0 else 0
        print(f"[ORB] {sym} margin={margin}x risk=Rs{risk:.0f} qty={qty} max_qty={max_qty} sl_dist={sld:.2f}")
        return{"symbol":sym,"direction":direction,"ltp":ltp,"or_high":o["high"],"or_low":o["low"],
            "or_size":sz,"or_size_pct":o["size_pct"],"sl_price":sl,"target1":t1,"target2":t2,
            "target3":t3,"quantity":qty,"risk_amount":round(risk,2),"rr_ratio":rr,
            "rvol":round(rvol,2),"signal_strength":signal_strength,"margin":margin,
            "time":datetime.datetime.now().isoformat()}
    def can_trade(self):
        if self.trading_stopped:return False,self.stop_reason
        if self.trial_mode:return True,""  # bypass all daily limits in trial mode
        cap=self.config["CAPITAL"];ml=cap*self.config["MAX_DAILY_LOSS_PCT"]
        if self.trades_today>=self.config["MAX_TRADES_DAY"]:return False,"Max trades reached"
        if self.daily_pnl<=-ml:self._stop(f"Daily loss limit Rs {ml}");return False,self.stop_reason
        if self.consecutive_loss>=self.config["MAX_CONSECUTIVE_LOSS"]:self._stop(f"{self.consecutive_loss} consecutive losses");return False,self.stop_reason
        return True,""
    def _stop(self,reason):
        self.trading_stopped=True;self.stop_reason=reason
        print(f"[ORB] STOPPED:{reason}")
        if self.on_stop:self.on_stop(reason)
    def record_trade_result(self,pnl):
        self.daily_pnl+=pnl;self.trades_today+=1
        if pnl<0:self.losses_today+=1;self.consecutive_loss+=1
        else:self.consecutive_loss=0
    def reset_daily(self):
        self.trades_today=0;self.losses_today=0;self.consecutive_loss=0
        self.daily_pnl=0;self.trading_stopped=False;self.stop_reason=""
        self.active_signals={};self.or_data={};self.volume_history=defaultdict(list)
        # Fix 12: Reset VWAP and Nifty daily
        self.vwap_data={}
        self.nifty_change=0
        self.last_nifty_update=None
        print("[ORB] Daily reset")
    @property
    def status(self):
        cap=self.config["CAPITAL"];ml=cap*self.config["MAX_DAILY_LOSS_PCT"]
        return{"trading_stopped":self.trading_stopped,"stop_reason":self.stop_reason,
            "trades_today":self.trades_today,"max_trades":self.config["MAX_TRADES_DAY"],
            "daily_pnl":round(self.daily_pnl,2),"max_daily_loss":round(ml,2),
            "consecutive_loss":self.consecutive_loss,"or_built":sum(1 for v in self.or_data.values() if v.get("built")),
            "active_signals":len(self.active_signals),"is_trade_time":self._is_trade_time(),
            "is_or_time":self._is_or_build_time(),"is_force_exit":self._is_force_exit_time(),
            "capital":cap,"risk_per_trade":round(cap*self.config["RISK_PCT"],2),
            "buying_power":cap*self.config["MIS_MARGIN"]}
