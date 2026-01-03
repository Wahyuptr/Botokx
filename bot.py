#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BOT SMC HYBRID v13.2.1 - FINAL PRODUCTION
Status: AUDITED & PATCHED
Features: Real PnL, Active Circuit Breaker, Thread Safe, Auto-Liquidate
"""

import ccxt
import time
import json
import os
import sys
import logging
import threading
import signal
import sqlite3
import concurrent.futures
import pandas as pd
import numpy as np
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv
import telebot

# [FIX] Windows Compatibility
try:
    import fcntl
except ImportError:
    fcntl = None

load_dotenv()

# ==================== CONFIGURATION ====================
os.environ["OMP_NUM_THREADS"] = "1"

class Konfigurasi:
    VERSI = "13.2.1-FINAL"
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    LOG_DIR = os.path.join(BASE_DIR, "logs")
    DATA_DIR = os.path.join(BASE_DIR, "data")
    
    # Files
    FILE_STATE = os.path.join(DATA_DIR, "state.json")
    FILE_LOG = os.path.join(LOG_DIR, "bot.log")
    FILE_DB = os.path.join(DATA_DIR, "trades.db")
    
    # Risk Settings
    RISK_PCT = 2.0
    MAX_DAILY_LOSS_PCT = 5.0 # Circuit Breaker limit
    LEVERAGE = 10
    TIMEFRAME = os.getenv("TIMEFRAME", "15m")
    
    # Execution
    SLIPPAGE_TOLERANCE = 0.01 
    STALE_ORDER_TIMEOUT = 300 # 5 minutes
    
    # Strategy
    ATR_PERIOD = 14
    ATR_MULT = 2.0
    RR_RATIO = 1.5
    VOLATILITY_MAX = 10.0
    
    # Creds
    SYMBOLS = ["BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT", "XRP/USDT:USDT"]
    TOKEN_TG = os.getenv("TELEGRAM_BOT_TOKEN", "")
    CHAT_ID_TG = os.getenv("TELEGRAM_CHAT_ID", "")
    API_KEY = os.getenv("OKX_API_KEY", "")
    API_SECRET = os.getenv("OKX_SECRET_KEY", "")
    API_PASS = os.getenv("OKX_PASSPHRASE", "")
    SANDBOX = str(os.getenv("OKX_SANDBOX", "False")).lower() == "true"

for d in [Konfigurasi.LOG_DIR, Konfigurasi.DATA_DIR]:
    os.makedirs(d, exist_ok=True)

# ==================== LOGGING ====================
def setup_logging():
    logger = logging.getLogger("SMCBot")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
    
    fh = RotatingFileHandler(Konfigurasi.FILE_LOG, maxBytes=5*1024*1024, backupCount=2, encoding='utf-8')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger

logger = setup_logging()

# ==================== ATOMIC STATE MANAGER ====================
class ThreadSafeState:
    def __init__(self):
        self._lock = threading.RLock()
        self._data = {
            'pnl_daily': 0.0,
            'balance_start': 0.0,
            'paused': False,
            'reason': ''
        }
        self.load()

    def load(self):
        with self._lock:
            if os.path.exists(Konfigurasi.FILE_STATE):
                try:
                    with open(Konfigurasi.FILE_STATE, 'r') as f:
                        self._data.update(json.load(f))
                except: pass

    def save(self):
        with self._lock:
            with open(Konfigurasi.FILE_STATE, 'w') as f:
                json.dump(self._data, f)

    def update_pnl(self, amount):
        with self._lock:
            self._data['pnl_daily'] += amount
            self.save()

    def set_paused(self, status, reason=""):
        with self._lock:
            self._data['paused'] = status
            self._data['reason'] = reason
            self.save()

    def get(self, key):
        with self._lock:
            return self._data.get(key)
    
    def set(self, key, value):
        with self._lock:
            self._data[key] = value
            self.save()

# ==================== SECURE DATABASE ====================
class DatabaseManager:
    def __init__(self):
        self.local = threading.local()
        self.init_db()

    def _get_conn(self):
        if not hasattr(self.local, 'conn'):
            self.local.conn = sqlite3.connect(Konfigurasi.FILE_DB, timeout=10)
            self.local.conn.execute('PRAGMA journal_mode=WAL;')
        return self.local.conn

    def init_db(self):
        conn = sqlite3.connect(Konfigurasi.FILE_DB)
        conn.execute('''CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY, symbol TEXT, side TEXT, 
            entry_price REAL, exit_price REAL, quantity REAL, 
            sl_price REAL, tp_price REAL, pnl REAL, 
            status TEXT, opened_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
            closed_at TIMESTAMP, reason TEXT
        )''')
        conn.close()

    def save_trade(self, data):
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO trades (symbol, side, entry_price, quantity, sl_price, tp_price, status) 
            VALUES (?,?,?,?,?,?,'open')
        """, (data['symbol'], data['side'], data['entry'], data['qty'], data['sl'], data['tp']))
        conn.commit()
        return cur.lastrowid

    def close_trade(self, trade_id, exit_price, pnl, reason):
        conn = self._get_conn()
        conn.execute("""
            UPDATE trades SET exit_price=?, pnl=?, status='closed', closed_at=CURRENT_TIMESTAMP, reason=? 
            WHERE id=?
        """, (exit_price, pnl, reason, trade_id))
        conn.commit()

    def get_open_trades(self):
        conn = self._get_conn()
        cur = conn.execute("SELECT * FROM trades WHERE status=?", ('open',))
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

# ==================== EXCHANGE & ORDER MGMT ====================
class ExchangeManager:
    def __init__(self):
        self.client = None
        self.market_info = {}
        self.init_exchange()
        
    def init_exchange(self):
        try:
            self.client = ccxt.okx({
                'apiKey': Konfigurasi.API_KEY, 'secret': Konfigurasi.API_SECRET, 'password': Konfigurasi.API_PASS,
                'enableRateLimit': True, 'options': {'defaultType': 'swap'}
            })
            if Konfigurasi.SANDBOX: self.client.set_sandbox_mode(True)
            self.client.load_markets()
            
            for s in Konfigurasi.SYMBOLS:
                try:
                    m = self.client.market(s)
                    self.market_info[s] = {
                        'min_qty': m['limits']['amount']['min'],
                        'contract_val': float(m.get('contractSize', 1.0))
                    }
                    self.client.set_leverage(Konfigurasi.LEVERAGE, s)
                except: pass
        except Exception as e:
            logger.critical(f"Exchange Init Failed: {e}")
            sys.exit(1)

    def safe_req(self, func, *args, **kwargs):
        for _ in range(3):
            try: 
                time.sleep(0.2)
                return func(*args, **kwargs)
            except (ccxt.NetworkError, ccxt.RequestTimeout): time.sleep(1)
            except Exception as e: 
                logger.error(f"API Error: {e}")
                return None
        return None

    # Wrapper methods for cleaner Controller code
    def fetch_balance(self): return self.safe_req(self.client.fetch_balance)
    def fetch_ticker(self, symbol): return self.safe_req(self.client.fetch_ticker, symbol)
    def fetch_positions(self): return self.safe_req(self.client.fetch_positions)
    def create_order(self, *args, **kwargs): return self.safe_req(self.client.create_order, *args, **kwargs)
    def cancel_order(self, id, symbol): return self.safe_req(self.client.cancel_order, id, symbol)
    def fetch_ohlcv(self, *args, **kwargs): return self.safe_req(self.client.fetch_ohlcv, *args, **kwargs)

    def cancel_stale_orders(self):
        try:
            orders = self.safe_req(self.client.fetch_open_orders)
            if not orders: return
            now = time.time() * 1000
            for o in orders:
                if (now - o['timestamp']) > (Konfigurasi.STALE_ORDER_TIMEOUT * 1000):
                    logger.warning(f"Cancelling stale order {o['id']} ({o['symbol']})")
                    self.cancel_order(o['id'], o['symbol'])
        except: pass

# ==================== STRATEGY ====================
class EMACrossStrategy:
    def analyze(self, df):
        if len(df) < 25: return None
        df['ema9'] = df['close'].ewm(span=9).mean()
        df['ema21'] = df['close'].ewm(span=21).mean()
        
        # Simple RSI
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rs = gain / loss.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs)).iloc[-1]
        
        # ATR
        h, l, c = df['high'], df['low'], df['close']
        atr = pd.concat([h-l, (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1).rolling(14).mean().iloc[-1]
        
        curr, prev = df.iloc[-1], df.iloc[-2]
        
        # Volatility Filter
        if (atr / curr['close']) * 100 > Konfigurasi.VOLATILITY_MAX: return None

        # Signal Logic
        signal = None
        if 30 < rsi < 70:
            if curr['ema9'] > curr['ema21'] and prev['ema9'] <= prev['ema21']:
                signal = {'side': 'buy', 'atr': atr}
            elif curr['ema9'] < curr['ema21'] and prev['ema9'] >= prev['ema21']:
                signal = {'side': 'sell', 'atr': atr}
        return signal

# ==================== BOT CONTROLLER ====================
class BotController:
    def __init__(self):
        self.state = ThreadSafeState()
        self.db = DatabaseManager()
        self.ex = ExchangeManager()
        self.strat = EMACrossStrategy()
        
        self.trades = {} 
        self.trade_ids = {}
        self.lock = threading.RLock()
        self.cooldowns = {}
        self.pool = concurrent.futures.ThreadPoolExecutor(max_workers=3)
        
        bal = self.ex.fetch_balance()
        if bal and self.state.get('balance_start') == 0:
            self.state.set('balance_start', float(bal['USDT']['total']))

    def panic_close_all(self):
        logger.critical("!!! PANIC CLOSE INITIATED !!!")
        with self.lock: symbols = list(self.trades.keys())
        for sym in symbols:
            with self.lock:
                if sym not in self.trades: continue
                t = self.trades[sym]
            
            side = 'sell' if t['side'] == 'buy' else 'buy'
            self.ex.create_order(sym, 'market', side, t['qty'])
            
            with self.lock:
                if sym in self.trades: 
                    del self.trades[sym]
                    if sym in self.trade_ids:
                        self.db.close_trade(self.trade_ids[sym], 0, 0, "CIRCUIT_BREAKER")

    def check_circuit_breaker(self):
        start = self.state.get('balance_start')
        if start <= 0: return
        
        bal = self.ex.fetch_balance()
        if not bal: return
        
        curr = float(bal['USDT']['total'])
        loss_pct = ((start - curr) / start) * 100
        
        if loss_pct >= Konfigurasi.MAX_DAILY_LOSS_PCT:
            logger.critical(f"Daily Loss {loss_pct:.2f}% Hit. LIQUIDATING!")
            self.panic_close_all()
            self.state.set_paused(True, f"Stop Loss Hit: -{loss_pct:.2f}%")

    def sync_positions(self):
        logger.info("[SYNC] Reconciling positions...")
        db_trades = {t['symbol']: t for t in self.db.get_open_trades()}
        try:
            live = self.ex.fetch_positions()
            live = [p for p in live if float(p['contracts']) > 0] if live else []
        except: live = []
        
        with self.lock:
            for p in live:
                sym = p['symbol']
                qty = float(p['contracts'])
                side = p['side'] if p['side'] in ['buy','sell'] else ('buy' if p['side']=='long' else 'sell')
                entry = float(p['entryPrice'])
                
                if sym in db_trades:
                    self.trades[sym] = {'symbol': sym, 'side': side, 'qty': qty, 'entry': entry, 
                                      'sl': db_trades[sym]['sl_price'], 'tp': db_trades[sym]['tp_price']}
                    self.trade_ids[sym] = db_trades[sym]['id']
                else:
                    sl = entry * (0.95 if side == 'buy' else 1.05)
                    tp = entry * (1.05 if side == 'buy' else 0.95)
                    data = {'symbol': sym, 'side': side, 'qty': qty, 'entry': entry, 'sl': sl, 'tp': tp}
                    self.trades[sym] = data
                    self.trade_ids[sym] = self.db.save_trade(data)

    def execute_trade(self, symbol, signal):
        if self.state.get('paused'): return
        with self.lock:
            if symbol in self.trades: return
            if time.time() < self.cooldowns.get(symbol, 0): return

        bal = self.ex.fetch_balance()
        if not bal: return
        
        total = float(bal['USDT']['total'])
        risk_amt = total * (Konfigurasi.RISK_PCT / 100)
        
        ticker = self.ex.fetch_ticker(symbol)
        if not ticker: return
        price = ticker['last']
        
        atr_sl = signal['atr'] * Konfigurasi.ATR_MULT
        contract_val = self.ex.market_info.get(symbol, {}).get('contract_val', 1)
        qty = max(risk_amt / (atr_sl * contract_val), self.ex.market_info.get(symbol, {}).get('min_qty', 1))

        if signal['side'] == 'buy':
            sl, tp = price - atr_sl, price + (atr_sl * Konfigurasi.RR_RATIO)
            limit_price = price * (1 + Konfigurasi.SLIPPAGE_TOLERANCE)
        else:
            sl, tp = price + atr_sl, price - (atr_sl * Konfigurasi.RR_RATIO)
            limit_price = price * (1 - Konfigurasi.SLIPPAGE_TOLERANCE)

        liq_buffer = price * (1/Konfigurasi.LEVERAGE) * 0.85
        if abs(price - sl) > liq_buffer: 
            logger.warning(f"Trade skipped: SL too close to Liq for {symbol}")
            return

        logger.info(f"OPEN {signal['side']} {symbol} Limit:{limit_price}")
        order = self.ex.create_order(symbol, 'limit', signal['side'], qty, limit_price)
        
        if order:
            data = {'symbol': symbol, 'side': signal['side'], 'qty': qty, 'entry': price, 'sl': sl, 'tp': tp}
            with self.lock:
                self.trades[symbol] = data
                self.trade_ids[symbol] = self.db.save_trade(data)

    def manage_positions(self):
        with self.lock: snapshot = {k: v.copy() for k, v in self.trades.items()}
        for sym, t in snapshot.items():
            ticker = self.ex.fetch_ticker(sym)
            if not ticker: continue
            curr = ticker['last']
            
            close = False
            reason = ""
            if t['side'] == 'buy':
                if curr <= t['sl']: close, reason = True, "SL"
                elif curr >= t['tp']: close, reason = True, "TP"
            else:
                if curr >= t['sl']: close, reason = True, "SL"
                elif curr <= t['tp']: close, reason = True, "TP"
            
            if close: self.close_position(sym, reason)

    def close_position(self, symbol, reason):
        with self.lock:
            if symbol not in self.trades: return
            t = self.trades[symbol]
            
        side = 'sell' if t['side'] == 'buy' else 'buy'
        order = self.ex.create_order(symbol, 'market', side, t['qty'])
        
        # [AUDIT FIX] Calculate Real PnL
        exit_price = self.ex.fetch_ticker(symbol)['last']
        if order and 'average' in order and order['average']:
            exit_price = float(order['average'])
            
        contract_val = self.ex.market_info.get(symbol, {}).get('contract_val', 1.0)
        if t['side'] == 'buy': pnl = (exit_price - t['entry']) * t['qty'] * contract_val
        else: pnl = (t['entry'] - exit_price) * t['qty'] * contract_val
            
        with self.lock:
            if symbol in self.trades:
                del self.trades[symbol]
                tid = self.trade_ids.pop(symbol, None)
                if tid: self.db.close_trade(tid, exit_price, pnl, reason)
                self.cooldowns[symbol] = time.time() + 300
        
        self.state.update_pnl(pnl)
        logger.info(f"CLOSED {symbol}: {reason} | PnL: ${pnl:.4f}")

    def run(self):
        logger.info(f"?? Bot {Konfigurasi.VERSI} Started")
        self.sync_positions()
        TelegramBot(self)
        
        while True:
            try:
                self.check_circuit_breaker()
                self.ex.cancel_stale_orders()
                
                if not self.state.get('paused'):
                    self.manage_positions()
                    with self.lock: needed = [s for s in Konfigurasi.SYMBOLS if s not in self.trades]
                    if needed:
                        futures = {self.pool.submit(self.ex.client.fetch_ohlcv, s, Konfigurasi.TIMEFRAME): s for s in needed}
                        for f in concurrent.futures.as_completed(futures):
                            try:
                                res = f.result()
                                if res:
                                    sig = self.strat.analyze(pd.DataFrame(res, columns=['time','open','high','low','close','vol']))
                                    if sig: self.execute_trade(futures[f], sig)
                            except: pass
                time.sleep(5)
            except KeyboardInterrupt: sys.exit(0)
            except Exception as e: 
                logger.error(f"Loop Error: {e}"); time.sleep(5)

# ==================== TELEGRAM ====================
class TelegramBot:
    def __init__(self, bot_ctrl):
        if not Konfigurasi.TOKEN_TG: return
        self.bot = bot_ctrl
        self.tb = telebot.TeleBot(Konfigurasi.TOKEN_TG)
        self.register_handlers()
        threading.Thread(target=self.tb.infinity_polling, daemon=True).start()

    def register_handlers(self):
        def is_auth(m): return str(m.chat.id) == Konfigurasi.CHAT_ID_TG
        
        @self.tb.message_handler(commands=['status'])
        def status(m):
            if not is_auth(m): return
            paused = self.bot.state.get('paused')
            msg = f"?? Status: {'PAUSED' if paused else 'RUNNING'}\n"
            msg += f"Daily PnL: ${self.bot.state.get('pnl_daily'):.2f}\n"
            msg += f"Trades: {len(self.bot.trades)}"
            self.tb.reply_to(m, msg)

        @self.tb.message_handler(commands=['resume'])
        def resume(m):
            if not is_auth(m): return
            self.bot.state.set_paused(False)
            self.bot.state.set('balance_start', 0)
            self.tb.reply_to(m, "? Bot Resumed.")

if __name__ == "__main__":
    if not Konfigurasi.API_KEY: sys.exit("ERROR: .env file missing")
    BotController().run()