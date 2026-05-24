# NSE Market Watch v3.0 — ORB Algo Trading

## Commands
- Restart: sudo systemctl restart marketwatch
- Logs: sudo journalctl -u marketwatch -f --no-pager
- Verify: python3 -m py_compile app.py
- Backup: tar -czf ~/backup-$(date +%Y%m%d-%H%M).tar.gz app.py orb_strategy.py order_manager.py websocket_engine.py index.html

## Files
- app.py — Flask server, NSE polling, WS management
- orb_strategy.py — ORB logic, breakout detection
- order_manager.py — order execution, position tracking
- websocket_engine.py — Flattrade WS, ticks, candles
- index.html — frontend UI

## Rules
- Always py_compile before restart
- Always backup after major changes
- Broker: Flattrade, Client FZ07236
- WS tokens from Shoonya master file (SearchScrip is dead/404)
