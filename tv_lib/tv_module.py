import json
import logging
import random
import re
import string
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, List, Optional
import websocket

# Logging configuration
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("TradingViewWS")

WS_URL  = "wss://data.tradingview.com/socket.io/websocket"
HEADERS = {"Origin": "https://www.tradingview.com"}

@dataclass
class TVConfig:
    ws_url: str = WS_URL
    auth_token: str = "unauthorized_user_token"
    headers: dict = field(default_factory=lambda: dict(HEADERS))

def _encode(msg: dict) -> str:
    body = json.dumps(msg, separators=(",", ":"))
    return f"~m~{len(body)}~m~{body}"

def _decode(raw: str) -> List[dict]:
    results = []
    for part in raw.split("~m~"):
        part = part.strip()
        if not part or part.isdigit(): continue
        try: results.append(json.loads(part))
        except: pass
    return results

def _heartbeat_reply(raw: str) -> Optional[str]:
    m = re.search(r"~h~(\d+)", raw)
    if m:
        token = m.group(1)
        return f"~m~{len('~h~' + token)}~m~~h~{token}"
    return None

def _new_session() -> str:
    return "qs_" + uuid.uuid4().hex[:12]

# --- Pandas-free OHLCV Parser ---
def _parse_ohlcv_list(raw: str) -> List[dict]:
    rows = []
    match = re.search(r'"s":\[(.*?)\]', raw, re.DOTALL)
    if match:
        try:
            bars = json.loads(f"[{match.group(1)}]")
            for bar in bars:
                v = bar.get("v", [])
                if len(v) >= 6:
                    rows.append({
                        "time": datetime.fromtimestamp(v[0]).strftime('%H:%M'),
                        "open": float(v[1]), "high": float(v[2]),
                        "low": float(v[3]), "close": float(v[4]), "vol": float(v[5])
                    })
        except: pass
    return rows

class TVHistorical:
    def __init__(self, market="BINANCE", ticker="BTCUSDT", config=TVConfig()):
        self.symbol = f"{market.upper()}:{ticker.upper()}"
        self.cfg = config

    def get(self, interval="1", n_bars=10) -> List[dict]:
        try:
            ws = websocket.create_connection(self.cfg.ws_url, header=self.cfg.headers, timeout=10)
            ws.recv() # Banner
            chart_sess = _new_session()
            ws.send(_encode({"m": "set_auth_token", "p": [self.cfg.auth_token]}))
            ws.send(_encode({"m": "chart_create_session", "p": [chart_sess, ""]}))
            ws.send(_encode({"m": "resolve_symbol", "p": [chart_sess, "s1", f'={{"symbol":"{self.symbol}"}}']}))
            ws.send(_encode({"m": "create_series", "p": [chart_sess, "rs", "s1", "s1", interval, n_bars]}))
            
            raw_buf = ""
            while "series_completed" not in raw_buf:
                msg = ws.recv()
                pong = _heartbeat_reply(msg)
                if pong: ws.send(pong)
                raw_buf += msg
            ws.close()
            return _parse_ohlcv_list(raw_buf)
        except Exception as e:
            log.error(f"Fetch error: {e}")
            return []

class TVStreamer:
    def __init__(self, symbols: List[str], on_quote: Callable, config=TVConfig()):
        self.symbols = [s.upper() for s in symbols]
        self.on_quote = on_quote
        self.cfg = config
        self._connected = False
        self._stopping = False
        self._ws = None

    def start(self):
        self._ws = websocket.WebSocketApp(
            self.cfg.ws_url, header=self.cfg.headers,
            on_open=self._on_open, on_message=self._on_message, on_close=self._on_close
        )
        threading.Thread(target=self._ws.run_forever, daemon=True).start()

    def _on_open(self, ws):
        self._connected = True
        session = _new_session()
        ws.send(_encode({"m": "set_auth_token", "p": [self.cfg.auth_token]}))
        ws.send(_encode({"m": "quote_create_session", "p": [session]}))
        for sym in self.symbols:
            ws.send(_encode({"m": "quote_add_symbols", "p": [session, sym]}))

    def _on_message(self, ws, raw):
        pong = _heartbeat_reply(raw)
        if pong: ws.send(pong); return
        for msg in _decode(raw):
            if msg.get("m") == "qsd":
                params = msg.get("p", [])
                if len(params) >= 2:
                    self.on_quote(params[1].get("n"), params[1].get("v", {}))

    def _on_close(self, ws, *args):
        self._connected = False
        if not self._stopping: time.sleep(5); self.start()

    def stop(self): self._stopping = True; self._ws.close()
    def __enter__(self): self.start(); return self
    def __exit__(self, *_): self.stop()
