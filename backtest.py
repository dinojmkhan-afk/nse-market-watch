#!/usr/bin/env python3
"""
NSE ORB Backtest Engine
Replays ORBStrategy on historical 5-min OHLCV data.
Usage: python3 backtest.py [--months 3] [--stocks 100]
"""
import argparse, os, sys, json, csv, datetime, time
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from orb_strategy import ORBStrategy, CONFIG

try:
    import yfinance as yf
    import pandas as pd
except ImportError:
    print("ERROR: pip install yfinance"); sys.exit(1)

RESULTS_FILE  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "backtest_results.csv")
STATUS_FILE   = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".backtest_status.json")
IST           = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
UTC           = datetime.timezone.utc
SLIPPAGE      = 0.0005   # 0.05%
OR_START_MINS = 9 * 60 + 15
OR_END_MINS   = 9 * 60 + 20
TRADE_START   = 9 * 60 + 30
TRADE_END     = 10 * 60 + 45
FORCE_EXIT    = 14 * 60 + 55

NIFTY100 = [
    "RELIANCE","TCS","HDFCBANK","BHARTIARTL","ICICIBANK","INFY","SBIN","HINDUNILVR","ITC",
    "KOTAKBANK","LT","HCLTECH","AXISBANK","WIPRO","ASIANPAINT","MARUTI","ULTRACEMCO","TITAN","NTPC",
    "BAJFINANCE","SUNPHARMA","TECHM","POWERGRID","TATAMOTORS","NESTLEIND","ONGC","HDFCLIFE",
    "TATASTEEL","GRASIM","COALINDIA","DIVISLAB","SBILIFE","DRREDDY","BAJAJFINSV","ADANIPORTS","BPCL",
    "TATACONSUM","CIPLA","EICHERMOT","APOLLOHOSP","JSWSTEEL","SIEMENS","BRITANNIA","HEROMOTOCO",
    "VEDL","HINDALCO","BAJAJ-AUTO","INDUSINDBK","LTIM","ZOMATO","ABB","BOSCHLTD","HAL","GODREJCP",
    "CHOLAFIN","PIDILITIND","HAVELLS","DLF","DABUR","BERGERPAINTS","TORNTPHARM","COLPAL","MARICO",
    "MUTHOOTFIN","LUPIN","BEL","BANKBARODA","PNB","RECLTD","PFC","TATAPOWER","TRENT","ZYDUSLIFE",
    "VBL","BHEL","CONCOR","LICI","NAUKRI","IRFC","CANBK","AMBUJACEM","SHREECEM","INDIGO",
    "DMART","STAR","ALKEM","BIOCON","ATGL","MAXHEALTH","INDUSTOWER","TATACOMM","MFSL",
    "DIXON","PERSISTENT","MOTHERSON","OBEROIRLTY","LODHA","PAYTM","NYKAA","JUBLFOOD","CROMPTON","POLYCAB",
]


# ── Subclass to bypass live-clock checks for historical replay ────────────────
class BacktestORBStrategy(ORBStrategy):
    def _is_trade_time(self):  return True
    def _is_nifty_fresh(self): return True


# ── Utility ───────────────────────────────────────────────────────────────────
def set_status(status, progress, phase="", percent=0, error=None):
    try:
        with open(STATUS_FILE, "w") as f:
            json.dump({
                "status": status, "progress": progress,
                "phase": phase, "percent": percent,
                "error": error, "updated": datetime.datetime.now().isoformat()
            }, f)
    except Exception:
        pass

def slippage(price, side):
    return round(price * (1 + SLIPPAGE) if side == "BUY" else price * (1 - SLIPPAGE), 2)

def calc_charges(qty, price, direction):
    v = qty * price
    return round(
        v * 0.00025 * (1 if direction == "SELL" else 0) +
        v * 0.0000297 + v * 0.000001 +
        v * 0.00003  * (1 if direction == "BUY" else 0), 2
    )

def mins(ts):
    return ts.hour * 60 + ts.minute

def trading_days(start, end):
    days, d = [], start
    while d <= end:
        if d.weekday() < 5:
            days.append(d)
        d += datetime.timedelta(days=1)
    return days


# ── Data download ─────────────────────────────────────────────────────────────
def download_5min(sym_ns, start, end, verbose=False):
    """Download 5-min data in ≤55-day chunks to stay within yfinance limits."""
    CHUNK = 55
    dfs = []
    cs = start
    while cs <= end:
        ce = min(cs + datetime.timedelta(days=CHUNK), end)
        try:
            df = yf.download(
                sym_ns,
                start=cs.strftime("%Y-%m-%d"),
                end=(ce + datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
                interval="5m", progress=False, auto_adjust=True, actions=False
            )
            if not df.empty:
                # Flatten MultiIndex columns (newer yfinance)
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.droplevel(1)
                dfs.append(df)
        except Exception as e:
            if verbose:
                print(f"  [DL] {sym_ns} chunk {cs}: {e}")
        cs = ce + datetime.timedelta(days=1)
        time.sleep(0.15)

    if not dfs:
        return None
    combined = pd.concat(dfs)
    combined = combined[~combined.index.duplicated(keep="first")].sort_index()
    if combined.index.tzinfo is None:
        combined.index = combined.index.tz_localize("UTC")
    combined.index = combined.index.tz_convert("Asia/Kolkata")
    return combined


def fetch_nifty_daily(start, end):
    """Return {date: daily_change_pct} for Nifty 100 / Nifty 50 fallback."""
    changes = {}
    for sym in ["^CNX100", "^NSEI"]:
        try:
            df = yf.download(sym, start=start.strftime("%Y-%m-%d"),
                             end=(end + datetime.timedelta(days=2)).strftime("%Y-%m-%d"),
                             interval="1d", progress=False, auto_adjust=True, actions=False)
            if df.empty: continue
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.droplevel(1)
            df["pct"] = df["Close"].pct_change() * 100
            for idx, row in df.iterrows():
                d = idx.date() if hasattr(idx, "date") else datetime.date.fromisoformat(str(idx)[:10])
                changes[d] = round(float(row["pct"]) if not pd.isna(row["pct"]) else 0.0, 3)
            print(f"[Nifty] {len(changes)} days fetched from {sym}")
            break
        except Exception as e:
            print(f"[Nifty] {sym} error: {e}")
    return changes


# ── Position simulation ───────────────────────────────────────────────────────
def make_position(sig, date_str):
    ep = slippage(sig["ltp"], sig["direction"])
    qty = sig["quantity"]
    qt1, qt2 = int(qty * 0.4), int(qty * 0.4)
    qt3 = qty - qt1 - qt2
    entry_charges = calc_charges(qty, ep, sig["direction"])
    return {
        "date":           date_str,
        "symbol":         sig["symbol"],
        "direction":      sig["direction"],
        "entry_price":    ep,
        "sl_price":       sig["sl_price"],
        "target1":        sig["target1"],
        "target2":        sig["target2"],
        "target3":        sig["target3"],
        "quantity":       qty,
        "qt1": qt1, "qt2": qt2, "qt3": qt3,
        "qty_remaining":  qty,
        "t1_done": False, "t2_done": False, "t3_done": False,
        "realized_gross": 0.0,
        "charges":        entry_charges,
        "or_high":        sig["or_high"],
        "or_low":         sig["or_low"],
        "or_size_pct":    sig["or_size_pct"],
        "rvol":           sig["rvol"],
        "signal_strength": sig["signal_strength"],
    }


def finalize(pos, exit_price, exit_reason):
    """Close remaining qty and build the CSV trade record."""
    d = pos["direction"]
    ep = pos["entry_price"]
    rem = pos["qty_remaining"]

    # PnL for remaining qty
    rem_pnl = (exit_price - ep) * rem if d == "BUY" else (ep - exit_price) * rem
    pos["charges"] += calc_charges(rem, exit_price, "SELL" if d == "BUY" else "BUY")
    total_gross = pos["realized_gross"] + rem_pnl
    net = total_gross - pos["charges"]

    # Weighted average exit: compute across partial exits + this final exit
    # For simplicity, report the final exit price as exit_price
    return {
        "date":           pos["date"],
        "symbol":         pos["symbol"],
        "direction":      d,
        "entry_price":    round(pos["entry_price"], 2),
        "exit_price":     round(exit_price, 2),
        "exit_reason":    exit_reason,
        "quantity":       pos["quantity"],
        "gross_pnl":      round(total_gross, 2),
        "charges":        round(pos["charges"], 2),
        "net_pnl":        round(net, 2),
        "or_high":        pos["or_high"],
        "or_low":         pos["or_low"],
        "or_size_pct":    pos["or_size_pct"],
        "rvol":           pos["rvol"],
        "signal_strength": pos["signal_strength"],
        "t1_hit":         pos["t1_done"],
        "t2_hit":         pos["t2_done"],
        "t3_hit":         pos["t3_done"],
    }


def tick_position(pos, candle, force=False):
    """
    Advance one candle on an open position.
    Returns completed trade dict if closed, else None.
    """
    d   = pos["direction"]
    hi  = candle["high"]
    lo  = candle["low"]
    cl  = candle["close"]

    if force:
        ep = slippage(cl, "SELL" if d == "BUY" else "BUY")
        return finalize(pos, ep, "FORCE_EXIT")

    if d == "BUY":
        # SL check first
        if lo <= pos["sl_price"]:
            ep = slippage(pos["sl_price"], "SELL")
            return finalize(pos, ep, "SL_HIT")

        # T1
        if not pos["t1_done"] and hi >= pos["target1"]:
            ep = slippage(pos["target1"], "SELL")
            pnl = (ep - pos["entry_price"]) * pos["qt1"]
            pos["realized_gross"] += pnl
            pos["charges"] += calc_charges(pos["qt1"], ep, "SELL")
            pos["qty_remaining"] -= pos["qt1"]
            pos["t1_done"] = True
            pos["sl_price"] = pos["entry_price"]   # trail to breakeven

        # T2
        if pos["t1_done"] and not pos["t2_done"] and hi >= pos["target2"]:
            ep = slippage(pos["target2"], "SELL")
            pnl = (ep - pos["entry_price"]) * pos["qt2"]
            pos["realized_gross"] += pnl
            pos["charges"] += calc_charges(pos["qt2"], ep, "SELL")
            pos["qty_remaining"] -= pos["qt2"]
            pos["t2_done"] = True
            pos["sl_price"] = pos["target1"]       # trail to T1

        # T3 (remaining qty)
        if pos["t2_done"] and not pos["t3_done"] and hi >= pos["target3"]:
            ep = slippage(pos["target3"], "SELL")
            pos["t3_done"] = True
            return finalize(pos, ep, "T3_HIT")

    else:  # SELL
        if hi >= pos["sl_price"]:
            ep = slippage(pos["sl_price"], "BUY")
            return finalize(pos, ep, "SL_HIT")

        if not pos["t1_done"] and lo <= pos["target1"]:
            ep = slippage(pos["target1"], "BUY")
            pnl = (pos["entry_price"] - ep) * pos["qt1"]
            pos["realized_gross"] += pnl
            pos["charges"] += calc_charges(pos["qt1"], ep, "BUY")
            pos["qty_remaining"] -= pos["qt1"]
            pos["t1_done"] = True
            pos["sl_price"] = pos["entry_price"]

        if pos["t1_done"] and not pos["t2_done"] and lo <= pos["target2"]:
            ep = slippage(pos["target2"], "BUY")
            pnl = (pos["entry_price"] - ep) * pos["qt2"]
            pos["realized_gross"] += pnl
            pos["charges"] += calc_charges(pos["qt2"], ep, "BUY")
            pos["qty_remaining"] -= pos["qt2"]
            pos["t2_done"] = True
            pos["sl_price"] = pos["target1"]

        if pos["t2_done"] and not pos["t3_done"] and lo <= pos["target3"]:
            ep = slippage(pos["target3"], "BUY")
            pos["t3_done"] = True
            return finalize(pos, ep, "T3_HIT")

    return None


# ── Per-day simulation ────────────────────────────────────────────────────────
def simulate_day(date, sym_candles, nifty_chg, config):
    """
    sym_candles: {symbol: {mins_of_day: candle_row_dict}}
    Returns list of trade records.
    """
    orb = BacktestORBStrategy(config)
    orb.nifty_change = nifty_chg
    date_str = date.strftime("%Y-%m-%d")

    # ── Step 1: build OR from 9:15 candle for each symbol ────────────────
    for sym, cmap in sym_candles.items():
        c915 = cmap.get(OR_START_MINS)
        if c915 is None: continue
        if c915["high"] <= 0 or c915["low"] <= 0: continue
        orb.or_data[sym] = {"high": c915["high"], "low": c915["low"],
                            "volume": c915["volume"], "built": False}
        orb.finalize_or(sym, {"high": c915["high"], "low": c915["low"],
                               "volume": c915["volume"]})
        # Seed volume history
        orb.volume_history[sym] = [c915["volume"] or 1]
        orb.avg_volumes[sym]    =  c915["volume"] or 1

    # ── Step 2: collect all time slots in order ───────────────────────────
    all_slots = sorted(set(
        m for cmap in sym_candles.values() for m in cmap.keys()
        if m >= OR_END_MINS
    ))

    active = {}   # sym -> position
    trades = []

    # VWAP tracking per symbol (incremental)
    vwap_pv  = defaultdict(float)
    vwap_vol = defaultdict(float)

    for slot in all_slots:
        is_trade_window = TRADE_START <= slot <= TRADE_END
        is_force = slot >= FORCE_EXIT

        for sym, cmap in sym_candles.items():
            c = cmap.get(slot)
            if c is None: continue

            # Update VWAP
            if c["volume"] > 0:
                vwap_pv[sym]  += c["close"] * c["volume"]
                vwap_vol[sym] += c["volume"]
                orb.vwap_data[sym] = {
                    "cum_pv": vwap_pv[sym], "cum_vol": vwap_vol[sym]
                }

            # Record candle volume for RVOL (post-OR candles)
            if slot >= OR_END_MINS:
                orb.record_candle_volume(sym, c["volume"])

            # Monitor active position
            if sym in active:
                result = tick_position(active[sym], c, force=is_force)
                if result:
                    trades.append(result)
                    orb.record_trade_result(result["net_pnl"])
                    if result["exit_reason"] == "SL_HIT":
                        orb.record_sl_hit(active[sym]["direction"])
                    del active[sym]
                continue

            # Signal detection in trade window
            if not is_trade_window: continue
            ok, _ = orb.can_trade()
            if not ok: continue
            if sym in orb.active_signals: continue

            candle_dict = {
                "open": c["open"], "high": c["high"],
                "low": c["low"], "close": c["close"],
                "volume": c["volume"], "closed": True,
                "time": int(datetime.datetime.combine(date,
                    datetime.time(slot // 60, slot % 60),
                    tzinfo=IST).timestamp() // 300) * 300,
            }
            orb.update_nifty(nifty_chg, datetime.datetime.now(UTC))
            sig = orb.check_breakout(sym, candle_dict, nifty_chg, {})
            if sig:
                active[sym] = make_position(sig, date_str)

    # Force-exit anything still open at end of day
    for sym, pos in active.items():
        last_slot = max((s for s in sym_candles[sym] if s <= FORCE_EXIT + 60), default=None)
        if last_slot:
            c = sym_candles[sym][last_slot]
            ep = slippage(c["close"], "SELL" if pos["direction"] == "BUY" else "BUY")
            result = finalize(pos, ep, "FORCE_EXIT")
            trades.append(result)
            orb.record_trade_result(result["net_pnl"])

    return trades


# ── Summary ───────────────────────────────────────────────────────────────────
def print_summary(trades):
    if not trades:
        print("\n[Backtest] No trades recorded.")
        return

    wins  = [t for t in trades if t["net_pnl"] > 0]
    loses = [t for t in trades if t["net_pnl"] <= 0]
    total_net = sum(t["net_pnl"] for t in trades)

    # Daily PnL
    daily = defaultdict(float)
    for t in trades:
        daily[t["date"]] += t["net_pnl"]
    daily_vals = sorted(daily.items())

    # Drawdown
    equity, peak, max_dd = 0.0, 0.0, 0.0
    for _, pnl in daily_vals:
        equity += pnl
        if equity > peak: peak = equity
        dd = peak - equity
        if dd > max_dd: max_dd = dd

    avg_win  = sum(t["net_pnl"] for t in wins)  / len(wins)  if wins  else 0
    avg_loss = sum(t["net_pnl"] for t in loses) / len(loses) if loses else 0
    pf = abs(sum(t["net_pnl"] for t in wins) / sum(t["net_pnl"] for t in loses)) if loses and avg_loss != 0 else float("inf")

    best_day  = max(daily.items(), key=lambda x: x[1]) if daily else ("--", 0)
    worst_day = min(daily.items(), key=lambda x: x[1]) if daily else ("--", 0)

    # Sharpe (daily returns)
    import math
    daily_pnls = [v for _, v in daily_vals]
    if len(daily_pnls) > 1:
        mean = sum(daily_pnls) / len(daily_pnls)
        std  = math.sqrt(sum((x - mean)**2 for x in daily_pnls) / (len(daily_pnls)-1))
        sharpe = (mean / std * math.sqrt(252)) if std > 0 else 0
    else:
        sharpe = 0

    print("\n" + "="*60)
    print(f"  BACKTEST SUMMARY  ({len(set(t['date'] for t in trades))} trading days)")
    print("="*60)
    print(f"  Total Trades   : {len(trades)}")
    print(f"  Win Rate       : {len(wins)/len(trades)*100:.1f}%  ({len(wins)}W / {len(loses)}L)")
    print(f"  Avg Win        : Rs {avg_win:+.2f}")
    print(f"  Avg Loss       : Rs {avg_loss:+.2f}")
    print(f"  Win/Loss Ratio : {abs(avg_win/avg_loss):.2f}x" if avg_loss else "  Win/Loss Ratio : ∞")
    print(f"  Profit Factor  : {pf:.2f}")
    print(f"  Total Net P&L  : Rs {total_net:+.2f}")
    print(f"  Max Drawdown   : Rs {max_dd:.2f}")
    print(f"  Sharpe Ratio   : {sharpe:.2f}")
    print(f"  Best Day       : {best_day[0]}  Rs {best_day[1]:+.2f}")
    print(f"  Worst Day      : {worst_day[0]}  Rs {worst_day[1]:+.2f}")
    print("="*60)


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", type=int, default=3)
    ap.add_argument("--stocks", type=int, default=100)
    args = ap.parse_args()

    set_status("running", "Starting backtest...", "init", 0)

    end_date   = datetime.date.today()
    start_date = end_date - datetime.timedelta(days=args.months * 31)
    symbols    = NIFTY100[:args.stocks]
    tdays      = trading_days(start_date, end_date)

    print(f"[Backtest] {start_date} → {end_date}  ({len(tdays)} trading days, {len(symbols)} symbols)")
    set_status("running", f"Fetching Nifty daily data...", "download", 1)

    # ── Fetch Nifty daily changes ─────────────────────────────────────────
    nifty_changes = fetch_nifty_daily(start_date, end_date)

    # ── Download 5-min data for all symbols ──────────────────────────────
    print(f"[Backtest] Downloading 5-min data for {len(symbols)} symbols...")
    sym_data = {}   # sym -> DataFrame
    failed   = []
    for i, sym in enumerate(symbols):
        pct = int(5 + (i / len(symbols)) * 55)
        set_status("running", f"Downloading {sym}... ({i+1}/{len(symbols)})", "download", pct)
        df = download_5min(f"{sym}.NS", start_date, end_date)
        if df is not None and not df.empty:
            sym_data[sym] = df
        else:
            failed.append(sym)
        if (i + 1) % 20 == 0:
            print(f"  {i+1}/{len(symbols)} done  ({len(failed)} failed so far)")

    print(f"[Backtest] Downloaded {len(sym_data)} symbols. Failed: {len(failed)}")
    if failed:
        print(f"  Skipped: {failed[:20]}")

    # ── Build per-day candle maps ─────────────────────────────────────────
    # pre-index: sym -> date -> {slot_mins: candle_dict}
    print("[Backtest] Indexing candles by day...")
    sym_day_candles = {}   # sym -> date -> {mins: candle}
    for sym, df in sym_data.items():
        day_map = defaultdict(dict)
        for ts, row in df.iterrows():
            d = ts.date()
            if d < start_date or d > end_date: continue
            if ts.weekday() >= 5: continue
            m = ts.hour * 60 + ts.minute
            if m < OR_START_MINS or m > FORCE_EXIT + 60: continue
            # Handle both flat and MultiIndex column access
            try:
                day_map[d][m] = {
                    "open":   float(row["Open"]),
                    "high":   float(row["High"]),
                    "low":    float(row["Low"]),
                    "close":  float(row["Close"]),
                    "volume": int(row["Volume"]),
                }
            except (KeyError, TypeError):
                pass
        sym_day_candles[sym] = dict(day_map)

    # ── Simulate each trading day ─────────────────────────────────────────
    all_trades = []
    config = dict(CONFIG)  # use live config

    for di, date in enumerate(tdays):
        pct = int(60 + (di / len(tdays)) * 38)
        set_status("running", f"Replaying {date}... ({di+1}/{len(tdays)} days)", "simulate", pct)

        # Build sym_candles for this day
        sym_candles = {
            sym: sym_day_candles[sym].get(date, {})
            for sym in sym_day_candles
        }
        # Only include symbols that have OR candle data
        sym_candles = {s: cm for s, cm in sym_candles.items() if OR_START_MINS in cm}

        if not sym_candles: continue

        nifty_chg = nifty_changes.get(date, 0.0)
        day_trades = simulate_day(date, sym_candles, nifty_chg, config)
        all_trades.extend(day_trades)

        if day_trades:
            day_pnl = sum(t["net_pnl"] for t in day_trades)
            print(f"  {date}  {len(day_trades)} trades  Net Rs {day_pnl:+.0f}  Nifty {nifty_chg:+.2f}%")

    # ── Write CSV ─────────────────────────────────────────────────────────
    set_status("running", f"Writing results ({len(all_trades)} trades)...", "output", 99)
    cols = ["date","symbol","direction","entry_price","exit_price","exit_reason",
            "quantity","gross_pnl","charges","net_pnl","or_high","or_low","or_size_pct",
            "rvol","signal_strength","t1_hit","t2_hit","t3_hit"]
    with open(RESULTS_FILE, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(all_trades)

    print(f"\n[Backtest] Results written → {RESULTS_FILE}")
    print_summary(all_trades)
    set_status("complete", f"Done — {len(all_trades)} trades", "complete", 100)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        set_status("error", "Interrupted", "error", 0, "KeyboardInterrupt")
        print("\n[Backtest] Interrupted")
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        set_status("error", str(e), "error", 0, tb)
        print(f"[Backtest] Error: {e}\n{tb}")
        sys.exit(1)
