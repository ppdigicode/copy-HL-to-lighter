#!/usr/bin/env python3
"""
Lighter Mean-Reversion Bot V5.3-MCI

STRATÉGIE MEAN-REVERSION avec MCI (Market Curvature Index):
- Bars 1 minute: Détection excès (Z-score + RSI + ret_30s)
- Bars 10 secondes: Détection pivot (MCI = curvature normalisée)
- Entry : Excès 1min + Flip MCI 10s
- Exit : TP 0.15%, SL 0.30%, Z-reversion, Time cap

MCI Trigger:
- LONG:  mci_prev < -0.1 ET mci > +0.3 (prix rebondit)
- SHORT: mci_prev > +0.1 ET mci < -0.3 (prix plonge)
"""

import os
import sys
import signal
import time
import asyncio
import aiohttp
import json
import traceback
import threading
import numpy as np
from collections import deque
from datetime import datetime
from dotenv import load_dotenv

try:
    import lighter
except ImportError:
    print("❌ ERROR: pip install lighter-python-sdk")
    sys.exit(1)


# ══════════════════════════════════════════════════════════════════════════════
# ORDERBOOK (from collector)
# ══════════════════════════════════════════════════════════════════════════════

class OrderBook:
    def __init__(self, name: str):
        self.name = name
        self.asks = {}  # price -> size
        self.bids = {}
        self.last_nonce = None
        self.last_update_ts = 0.0
        
        # Pour OFI
        self.bid_changes = deque(maxlen=1000)
        self.ask_changes = deque(maxlen=1000)
    
    def reset(self):
        self.asks = {}
        self.bids = {}
        self.last_nonce = None
    
    def best_bid(self):
        return max(self.bids) if self.bids else None
    
    def best_ask(self):
        return min(self.asks) if self.asks else None
    
    def mid(self):
        b, a = self.best_bid(), self.best_ask()
        return (b + a) / 2 if b and a else None
    
    def spread_bps(self):
        b, a = self.best_bid(), self.best_ask()
        if not b or not a:
            return None
        mid = (b + a) / 2
        return ((a - b) / mid) * 10000 if mid else None
    
    def imbalance(self):
        """Bid-ask imbalance L1"""
        b, a = self.best_bid(), self.best_ask()
        if not b or not a:
            return None
        bid_size = self.bids.get(b, 0)
        ask_size = self.asks.get(a, 0)
        total = bid_size + ask_size
        return (bid_size - ask_size) / total if total > 0 else 0
    
    def imbalance_L10(self):
        """Imbalance sur profondeur L1-L10 (V3 STRATÉGIE VALIDÉE)"""
        best_bids = sorted(self.bids.items(), reverse=True)[:10]
        best_asks = sorted(self.asks.items())[:10]
        
        bid_vol = sum(size for _, size in best_bids)
        ask_vol = sum(size for _, size in best_asks)
        total = bid_vol + ask_vol
        
        return (bid_vol - ask_vol) / total if total > 0 else 0
    
    def calculate_ofi(self, window_sec: float):
        """Calculate Order Flow Imbalance (from collector)"""
        now = time.time()
        cutoff = now - window_sec
        
        bid_added = 0
        bid_removed = 0
        ask_added = 0
        ask_removed = 0
        
        for ts, price, delta in self.bid_changes:
            if ts < cutoff:
                continue
            if delta > 0:
                bid_added += delta
            else:
                bid_removed += abs(delta)
        
        for ts, price, delta in self.ask_changes:
            if ts < cutoff:
                continue
            if delta > 0:
                ask_added += delta
            else:
                ask_removed += abs(delta)
        
        return (bid_added - bid_removed) - (ask_added - ask_removed)
    
    def apply_update(self, ob_data: dict) -> bool:
        """Apply orderbook update (from collector logic)"""
        try:
            nonce = ob_data.get("nonce")
            begin_nonce = ob_data.get("begin_nonce")
            
            # Check nonce sequence (only if we have a previous nonce AND begin_nonce is present)
            if self.last_nonce is not None and begin_nonce is not None:
                if begin_nonce != self.last_nonce:
                    print(f"⚠️  Nonce gap: {self.last_nonce} -> {begin_nonce}")
                    return False
            
            now = time.time()
            
            # Apply changes to asks
            for e in ob_data.get("asks", []):
                p, s = float(e["price"]), float(e["size"])
                old_size = self.asks.get(p, 0)
                delta = s - old_size
                
                if s == 0:
                    self.asks.pop(p, None)
                else:
                    self.asks[p] = s
                
                if delta != 0:
                    self.ask_changes.append((now, p, delta))
            
            # Apply changes to bids
            for e in ob_data.get("bids", []):
                p, s = float(e["price"]), float(e["size"])
                old_size = self.bids.get(p, 0)
                delta = s - old_size
                
                if s == 0:
                    self.bids.pop(p, None)
                else:
                    self.bids[p] = s
                
                if delta != 0:
                    self.bid_changes.append((now, p, delta))
            
            self.last_nonce = nonce
            self.last_update_ts = now
            return True
            
        except Exception as e:
            print(f"❌ Error applying update: {e}")
            return False


# ══════════════════════════════════════════════════════════════════════════════
# TRADE TRACKER (from collector)
# ══════════════════════════════════════════════════════════════════════════════

class TradeTracker:
    def __init__(self):
        self.trades = deque(maxlen=500)
    
    def add_trade(self, timestamp: float, price: float, size: float, is_buy: bool):
        self.trades.append({
            'timestamp': timestamp,
            'price': price,
            'size': size,
            'is_buy': is_buy
        })
    
    def calculate_ofi(self, window_seconds: float):
        """Calculate OFI from trades"""
        cutoff = time.time() - window_seconds
        
        buy_volume = 0
        sell_volume = 0
        
        for trade in reversed(self.trades):
            if trade['timestamp'] < cutoff:
                break
            
            volume = trade['price'] * trade['size']
            
            if trade['is_buy']:
                buy_volume += volume
            else:
                sell_volume += volume
        
        return buy_volume - sell_volume


# ══════════════════════════════════════════════════════════════════════════════
# MEAN REVERSION BOT
# ══════════════════════════════════════════════════════════════════════════════

class MeanReversionBot:
    def __init__(self):
        print("\n🤖 Lighter Mean-Reversion Bot V5.3-MCI\n")
        print("📊 Stratégie: Z-score Mean-Reversion (bars 1min)")
        print("🎯 Entry: |z| >= 2.5 + filtres optionnels (Trend/RSI/ret_30s)")
        print("💰 Exit: TP 0.15%, SL 0.30%, Z-reversion, Time 5min\n")
        
        load_dotenv()
        
        # Lighter config
        self.base_url = os.getenv('LIGHTER_BASE_URL', 'https://mainnet.zklighter.elliot.ai')
        self.ws_url = os.getenv('LIGHTER_WS_URL', 'wss://mainnet.zklighter.elliot.ai/stream')
        
        self.market_symbol = os.getenv('MARKET_SYMBOL', 'HYPE')
        self.market_index = int(os.getenv('MARKET_INDEX', '24'))
        
        self.dry_run = os.getenv('DRY_RUN', 'true').lower() == 'true'
        self.api_key_index = int(os.getenv('API_KEY_INDEX', '3'))
        self.api_private_key = os.getenv('API_PRIVATE_KEY', '')
        self.our_account_index = int(os.getenv('OUR_ACCOUNT_INDEX', '0')) if not self.dry_run else None
        
        # Strategy params V5 (Z-SCORE MEAN-REVERSION)
        # Basé sur backtest utilisateur backtest_hype5.py
        
        # Z-score params
        self.n_ema = int(os.getenv('N_EMA', '20'))  # EMA sur 20 bars 1min
        self.n_std = int(os.getenv('N_STD', '20'))  # STD sur 20 bars 1min
        self.trend_ema = int(os.getenv('TREND_EMA', '200'))  # EMA 200 bars = 3h20
        
        # Entry/Exit thresholds
        self.z_entry = float(os.getenv('Z_ENTRY', '2.5'))  # |z| >= 2.5
        self.z_exit = float(os.getenv('Z_EXIT', '0.3'))   # exit when z near 0
        self.z_stop = float(os.getenv('Z_STOP', '3.5'))   # stop if z continues
        
        # TP/SL in percent
        self.tp_pct = float(os.getenv('TP_PCT', '0.0015'))  # 0.15%
        self.sl_pct = float(os.getenv('SL_PCT', '0.0030'))  # 0.30%
        
        # Time cap
        self.max_hold_time_sec = int(os.getenv('MAX_HOLD_TIME_SEC', '300'))  # 5min
        
        # Filters
        self.use_trend_filter = os.getenv('USE_TREND_FILTER', 'false').lower() == 'true'  # ALWAYS false in V5.3
        self.enable_short = os.getenv('ENABLE_SHORT', 'false').lower() == 'true'  # SHORT désactivé par défaut
        
        # V5: RSI filter
        self.use_rsi_filter = os.getenv('USE_RSI_FILTER', 'false').lower() == 'true'
        self.rsi_period = int(os.getenv('RSI_PERIOD', '14'))
        self.rsi_oversold = float(os.getenv('RSI_OVERSOLD', '30'))  # RSI < 30 for LONG
        self.rsi_overbought = float(os.getenv('RSI_OVERBOUGHT', '70'))  # RSI > 70 for SHORT
        
        # V5.3: ret_30s filter (return over 30 seconds)
        self.use_ret_filter = os.getenv('USE_RET_FILTER', 'false').lower() == 'true'
        self.ret_threshold = float(os.getenv('RET_THRESHOLD', '0.4'))  # 0.4% threshold
        
        # V5: State machine - Wait for rebound before entry
        # V5.3-MCI: State machine now uses MCI instead of z-score rebound
        self.use_mci_trigger = os.getenv('USE_MCI_TRIGGER', 'true').lower() == 'true'  # Use MCI for entry timing
        self.max_wait_time = int(os.getenv('MAX_WAIT_TIME', '60'))  # Secondes max d'attente
        
        # MCI (Market Curvature Index) parameters
        self.mci_ema_len = int(os.getenv('MCI_EMA_LEN', '3'))  # EMA length for smoothing
        self.mci_sigma_window = int(os.getenv('MCI_SIGMA_WINDOW', '12'))  # Window for volatility normalization
        self.mci_flip_pos = float(os.getenv('MCI_FLIP_POS', '0.1'))  # Positive flip threshold
        self.mci_flip_neg = float(os.getenv('MCI_FLIP_NEG', '-0.1'))  # Negative flip threshold
        self.mci_trigger_pos = float(os.getenv('MCI_TRIGGER_POS', '0.3'))  # Positive trigger
        self.mci_trigger_neg = float(os.getenv('MCI_TRIGGER_NEG', '-0.3'))  # Negative trigger
        
        self.position_size_usd = float(os.getenv('POSITION_SIZE_USD', '100'))
        self.slippage_tolerance_pct = float(os.getenv('SLIPPAGE_TOLERANCE_PCT', '0.5'))
        
        # Data (from WebSocket)
        self.orderbook = OrderBook(self.market_symbol)
        self.trade_tracker = TradeTracker()
        
        # V5: 1-minute bars (pour Z-score)
        self.bars_1min = deque(maxlen=250)  # 250 bars = ~4 heures
        self.current_bar_start = None
        self.current_bar_high = None
        self.current_bar_low = None
        self.current_bar_close = None
        
        # V5.3-MCI: 10-second bars (pour curvature)
        self.bars_10s = deque(maxlen=400)  # 400 bars = ~67 minutes
        self.current_10s_start = None
        self.current_10s_close = None
        
        # MCI calculation data
        self.ema10s_hist = deque(maxlen=self.mci_sigma_window + 10)  # EMA history
        self.mci = None
        self.mci_prev = None
        
        # Price sampling pour construire les bars
        self.price_history = deque(maxlen=120)  # Keep for monitoring
        self.last_price_sample = 0
        self.price_sample_interval = 0.5
        
        # Last trade time (pour cooldown)
        self.last_trade_time = 0
        
        # V5: State machine for rebound detection
        self.signal_state = "idle"  # idle, armed_long, armed_short
        self.armed_z = None
        self.armed_time = None
        self.armed_price = None
        
        # State
        self.position = None
        self.stop_event = asyncio.Event()
        self.order_lock = asyncio.Lock()
        
        # Client order index (from Copy-Lighter bot)
        self.BOT_TAG = 3_000_000  # Tag unique pour ce bot (mean-reversion)
        self.order_counter = 0
        self.order_counter_lock = threading.Lock()
        
        # Lighter SDK
        self.signer_client = None
        
        # Market metadata
        self.market_metadata = {
            'size_decimals': 2,
            'price_decimals': 4,
            'min_size': 0.50,
        }
        
        # Stats
        self.stats = {
            'trades_opened': 0,
            'trades_closed': 0,
            'wins': 0,
            'losses': 0,
            'total_pnl_usd': 0.0
        }
        
        if not self.dry_run:
            if not self.api_private_key or self.our_account_index == 0:
                print("❌ ERROR: Live mode needs API_PRIVATE_KEY and OUR_ACCOUNT_INDEX")
                sys.exit(1)
        
        print(f"🎯 Market: {self.market_symbol} (index={self.market_index})")
        print(f"🧪 Dry run: {self.dry_run}")
        print(f"💰 Position size: ${self.position_size_usd}")
        print(f"\n📊 Strategy V5 (Z-SCORE MEAN REVERSION):")
        print(f"  Z-score: EMA({self.n_ema}), STD({self.n_std})")
        print(f"  Entry: |z| >= {self.z_entry}")
        print(f"  Trend filter: OFF (removed in V5.3)")
        print(f"  RSI filter: {'ON' if self.use_rsi_filter else 'OFF'}")
        if self.use_rsi_filter:
            print(f"    RSI({self.rsi_period}): LONG<{self.rsi_oversold}, SHORT>{self.rsi_overbought}")
        print(f"  ret_30s filter: {'ON' if self.use_ret_filter else 'OFF'}")
        if self.use_ret_filter:
            print(f"    ret threshold: {self.ret_threshold}%")
        print()
        print("🎯 Entry Timing (V5.3-MCI):")
        print(f"  MCI Trigger: {'ON' if self.use_mci_trigger else 'OFF (legacy rebound)'}")
        if self.use_mci_trigger:
            print(f"    EMA length: {self.mci_ema_len}")
            print(f"    Sigma window: {self.mci_sigma_window}")
            print(f"    LONG:  mci_prev < {self.mci_flip_neg} AND mci > {self.mci_trigger_pos}")
            print(f"    SHORT: mci_prev > {self.mci_flip_pos} AND mci < {self.mci_trigger_neg}")
        print(f"  Exit: TP {self.tp_pct*100:.2f}%, SL {self.sl_pct*100:.2f}%")
        print(f"  Exit Z: reversion {self.z_exit}, stop {self.z_stop}")
        print(f"  Time cap: {self.max_hold_time_sec}s")
        print(f"  SHORT: {'ENABLED' if self.enable_short else 'DISABLED'}")
        print(f"\n🎯 Entry Timing:")
        print(f"  MCI Trigger: {'ENABLED' if self.use_mci_trigger else 'DISABLED (immediate entry)'}")
        if self.use_mci_trigger:
            print(f"  MCI EMA length: {self.mci_ema_len}")
            print(f"  MCI sigma window: {self.mci_sigma_window}")
            print(f"  Max wait time: {self.max_wait_time}s")
        print()
        
        signal.signal(signal.SIGINT, lambda s,f: asyncio.create_task(self.shutdown()))
    
    async def shutdown(self):
        """Graceful shutdown"""
        print("\n🛑 Shutting down...")
        self.stop_event.set()
    
    async def initialize_sdk(self):
        """Initialize Lighter SDK"""
        try:
            if not self.dry_run:
                print("🔧 Initializing SignerClient...")
                
                self.signer_client = lighter.SignerClient(
                    url=self.base_url,
                    api_private_keys={self.api_key_index: self.api_private_key},
                    account_index=self.our_account_index
                )
                
                err = self.signer_client.check_client()
                if err:
                    print(f"❌ Signer error: {err}")
                    return False
                
                print("✅ Signer OK\n")
            
            return True
            
        except Exception as e:
            print(f"❌ Init failed: {e}")
            traceback.print_exc()
            return False
    
    def _round_size(self, size):
        return round(size, self.market_metadata['size_decimals'])
    
    def _round_price(self, price):
        return round(price, self.market_metadata['price_decimals'])
    
    def update_1min_bar(self, timestamp, price):
        """Update 1-minute bar construction (V5)"""
        # Round timestamp to minute
        minute_start = int(timestamp / 60) * 60
        
        # New bar ?
        if self.current_bar_start != minute_start:
            # Close previous bar if exists
            if self.current_bar_start is not None and self.current_bar_close is not None:
                self.bars_1min.append({
                    'timestamp': self.current_bar_start,
                    'close': self.current_bar_close,
                    'high': self.current_bar_high,
                    'low': self.current_bar_low
                })
            
            # Start new bar
            self.current_bar_start = minute_start
            self.current_bar_close = price
            self.current_bar_high = price
            self.current_bar_low = price
        else:
            # Update current bar
            self.current_bar_close = price
            self.current_bar_high = max(self.current_bar_high, price)
            self.current_bar_low = min(self.current_bar_low, price)
    
    def calculate_ema(self, span):
        """Calculate EMA on 1-minute bars (V5)"""
        if len(self.bars_1min) < span:
            return None
        
        closes = np.array([b['close'] for b in self.bars_1min])
        
        # EMA calculation
        alpha = 2 / (span + 1)
        ema = closes[0]
        for close in closes[1:]:
            ema = alpha * close + (1 - alpha) * ema
        
        return ema
    
    def calculate_std(self, window):
        """Calculate STD on 1-minute bars (V5)"""
        if len(self.bars_1min) < window:
            return None
        
        closes = np.array([b['close'] for b in list(self.bars_1min)[-window:]])
        return float(np.std(closes))
    
    def calculate_zscore(self, use_current_bar=True):
        """Calculate Z-score mean reversion (V5)
        
        Z = (close - EMA(N_EMA)) / STD(N_STD)
        
        use_current_bar: If True, use current bar being built (real-time)
                        If False, use last closed bar (conservative)
        
        Basé sur backtest_hype5.py de l'utilisateur
        """
        if len(self.bars_1min) < max(self.n_ema, self.n_std):
            return None, None, None
        
        # Use current bar price if available (real-time Z-score)
        if use_current_bar and self.current_bar_close is not None:
            current_close = self.current_bar_close
        else:
            # Fallback to last closed bar
            current_close = self.bars_1min[-1]['close']
        
        # EMA et STD calculés sur bars FERMÉES
        ema_n = self.calculate_ema(self.n_ema)
        std_n = self.calculate_std(self.n_std)
        
        if ema_n is None or std_n is None or std_n == 0:
            return None, None, None
        
        # Z-score avec prix actuel
        z = (current_close - ema_n) / std_n
        
        return z, ema_n, std_n
    
    def calculate_rsi(self, period=None):
        """Calculate RSI (Relative Strength Index) - Wilder's method (TradingView standard)
        
        RSI = 100 - (100 / (1 + RS))
        RS = Average Gain / Average Loss
        
        Uses Wilder's smoothing (EMA) instead of simple average:
        - First avg = simple average of first 'period' values
        - Next avg = (previous avg * (period-1) + current value) / period
        
        RSI > 70 = Overbought (surachat) → Favorable SHORT
        RSI < 30 = Oversold (survente) → Favorable LONG
        """
        if period is None:
            period = self.rsi_period
        
        # Need enough bars (period + 1 for initial calculation)
        if len(self.bars_1min) < period + 1:
            return None
        
        # Get all closes
        closes = np.array([b['close'] for b in self.bars_1min])
        
        # Calculate price changes
        deltas = np.diff(closes)
        
        # Separate gains and losses
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        
        # Wilder's smoothing method
        # First value: simple average of first 'period' values
        avg_gain = np.mean(gains[:period])
        avg_loss = np.mean(losses[:period])
        
        # Subsequent values: Wilder's EMA
        for i in range(period, len(gains)):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        
        # Avoid division by zero
        if avg_loss == 0:
            return 100.0
        
        # Calculate RS and RSI
        rs = avg_gain / avg_loss
        rsi = 100.0 - (100.0 / (1.0 + rs))
        
        return rsi
    
    def calculate_trend_ema(self):
        """Calculate trend EMA(200) for filter (V5)"""
        if not self.use_trend_filter:
            return None
        
        return self.calculate_ema(self.trend_ema)
    
    def check_signal(self):
        """Check for trading signals (V5 Z-SCORE MEAN REVERSION + STATE MACHINE)
        
        State machine logic:
        - IDLE: Detect signal (z threshold crossed)
        - ARMED: Wait for rebound (z stops worsening)
        - ENTRY: Execute when z rebounds or timeout
        
        Performance: Testé par utilisateur sur ses propres données
        """
        mid = self.orderbook.mid()
        if not mid:
            return {'type': None}
        
        # Need enough bars
        if len(self.bars_1min) < max(self.n_ema, self.n_std, self.trend_ema if self.use_trend_filter else 0):
            return {'type': None}
        
        # Calculate indicators
        z, ema_n, std_n = self.calculate_zscore()  # Uses current bar by default
        if z is None:
            return {'type': None}
        
        # Get current close (bar en cours ou dernière bar fermée)
        if self.current_bar_close is not None:
            current_close = self.current_bar_close
        else:
            current_close = self.bars_1min[-1]['close']
        
        # Trend filter
        if self.use_trend_filter:
            ema_trend = self.calculate_trend_ema()
            if ema_trend is None:
                return {'type': None}
        else:
            ema_trend = None
        
        # RSI filter
        if self.use_rsi_filter:
            rsi = self.calculate_rsi()
            if rsi is None:
                return {'type': None}  # Not enough data yet
        else:
            rsi = None
        
        # ret_30s filter (matches Pine Script: close - close[1])
        if self.use_ret_filter:
            if len(self.bars_1min) < 2:
                return {'type': None}  # Need at least 2 bars
            # Pine Script: (close - close[1]) / close[1]
            # bars_1min[-1] = last closed bar (= close in Pine)
            # bars_1min[-2] = previous bar (= close[1] in Pine)
            ret_30s = (self.bars_1min[-1]['close'] - self.bars_1min[-2]['close']) / self.bars_1min[-2]['close']
        else:
            ret_30s = None
        
        # ════════════════════════════════════════════════════════════════
        # STATE MACHINE LOGIC
        # ════════════════════════════════════════════════════════════════
        
        if not self.use_mci_trigger:
            # CLASSIC MODE: Immediate entry (no MCI timing)
            return self._check_signal_immediate(z, current_close, mid, ema_trend, ema_n, rsi)
        
        # STATE MACHINE MODE
        now = time.time()
        
        # ──────────────────────────────────────────────────────────────
        # STATE: IDLE - Detect signal
        # ──────────────────────────────────────────────────────────────
        if self.signal_state == "idle":
            # LONG signal detected
            if z <= -self.z_entry:
                if self.use_trend_filter and current_close < ema_trend:
                    return {'type': None}  # Trend filter blocks
                
                # RSI filter for LONG
                if self.use_rsi_filter and rsi >= self.rsi_oversold:
                    return {'type': None}  # RSI not oversold enough
                
                # ARM the signal
                self.signal_state = "armed_long"
                self.armed_z = z
                self.armed_time = now
                self.armed_price = mid
                
                print(f"🎯 ARMED LONG: z={z:.2f} | Waiting for rebound...")
                return {'type': None}
            
            # SHORT signal detected
            if self.enable_short and z >= self.z_entry:
                if self.use_trend_filter and current_close > ema_trend:
                    return {'type': None}
                
                # RSI filter for SHORT
                if self.use_rsi_filter and rsi <= self.rsi_overbought:
                    return {'type': None}  # RSI not overbought enough
                
                # ARM the signal
                self.signal_state = "armed_short"
                self.armed_z = z
                self.armed_time = now
                self.armed_price = mid
                
                print(f"🎯 ARMED SHORT: z={z:.2f} | Waiting for rebound...")
                return {'type': None}
        
        # ──────────────────────────────────────────────────────────────
        # STATE: ARMED_LONG - Wait for MCI flip
        # ──────────────────────────────────────────────────────────────
        elif self.signal_state == "armed_long":
            elapsed = now - self.armed_time
            
            # Check if signal still valid (trend filter)
            if self.use_trend_filter and current_close < ema_trend:
                print(f"⚠️  DISARMED LONG: Trend filter failed")
                self.signal_state = "idle"
                return {'type': None}
            
            # MCI FLIP DETECTED: prix rebondit
            if self.check_mci_flip('long'):
                mci_str = f"{self.mci:.3f}" if self.mci is not None else "N/A"
                mci_prev_str = f"{self.mci_prev:.3f}" if self.mci_prev is not None else "N/A"
                
                print(f"✅ MCI FLIP DETECTED: {mci_prev_str} → {mci_str} (LONG)")
                self.signal_state = "idle"
                
                ret_str = f"{ret_30s*100:+.2f}%" if ret_30s is not None else "N/A"
                
                return {
                    'type': 'long',
                    'reason': f'LONG MCI={mci_str} z={z:.2f} ret={ret_str}',
                    'price': mid,
                    'z': z,
                    'mci': self.mci,
                    'ret_30s': ret_30s
                }
            
            # TIMEOUT: Max wait time exceeded
            if elapsed > self.max_wait_time:
                print(f"⏱️  TIMEOUT: Signal expired after {elapsed:.0f}s")
                self.signal_state = "idle"
                return {'type': None}
            
            # Still waiting for MCI flip...
            if int(elapsed) % 3 == 0 and int(elapsed) > 0:  # Log every 3s
                mci_str = f"{self.mci:.3f}" if self.mci is not None else "N/A"
                print(f"⏳ ARMED LONG: z={z:.2f} mci={mci_str} | Wait {elapsed:.0f}s/{self.max_wait_time}s")
        
        # ──────────────────────────────────────────────────────────────
        # STATE: ARMED_SHORT - Wait for MCI flip
        # ──────────────────────────────────────────────────────────────
        elif self.signal_state == "armed_short":
            elapsed = now - self.armed_time
            
            # Check if signal still valid (trend filter)
            if self.use_trend_filter and current_close > ema_trend:
                print(f"⚠️  DISARMED SHORT: Trend filter failed")
                self.signal_state = "idle"
                return {'type': None}
            
            # MCI FLIP DETECTED: prix plonge
            if self.check_mci_flip('short'):
                mci_str = f"{self.mci:.3f}" if self.mci is not None else "N/A"
                mci_prev_str = f"{self.mci_prev:.3f}" if self.mci_prev is not None else "N/A"
                
                print(f"✅ MCI FLIP DETECTED: {mci_prev_str} → {mci_str} (SHORT)")
                self.signal_state = "idle"
                
                ret_str = f"{ret_30s*100:+.2f}%" if ret_30s is not None else "N/A"
                
                return {
                    'type': 'short',
                    'reason': f'SHORT MCI={mci_str} z={z:.2f} ret={ret_str}',
                    'price': mid,
                    'z': z,
                    'mci': self.mci,
                    'ret_30s': ret_30s
                }
            
            # TIMEOUT: Max wait time exceeded
            if elapsed > self.max_wait_time:
                print(f"⏱️  TIMEOUT: Signal expired after {elapsed:.0f}s")
                self.signal_state = "idle"
                return {'type': None}
            
            # Still waiting for MCI flip...
            if int(elapsed) % 3 == 0 and int(elapsed) > 0:  # Log every 3s
                mci_str = f"{self.mci:.3f}" if self.mci is not None else "N/A"
                print(f"⏳ ARMED SHORT: z={z:.2f} mci={mci_str} | Wait {elapsed:.0f}s/{self.max_wait_time}s")
        
        return {'type': None}
    
    def _check_signal_immediate(self, z, current_close, mid, ema_trend, ema_n, rsi):
        """Classic immediate entry (no state machine)"""
        
        # LONG signal: z <= -Z_ENTRY
        if z <= -self.z_entry:
            if self.use_trend_filter and current_close < ema_trend:
                return {'type': None}  # Skip if below trend EMA
            
            # RSI filter for LONG
            if self.use_rsi_filter and rsi >= self.rsi_oversold:
                return {'type': None}  # RSI not oversold enough
            
            rsi_str = f"RSI={rsi:.1f}" if rsi is not None else "RSI=N/A"
            
            return {
                'type': 'long',
                'reason': f'LONG z={z:.2f} | {rsi_str} | ret={ret_str}',
                'price': mid,
                'z': z,
                'ema_n': ema_n,
                'ret_30s': ret_30s,
                'rsi': rsi
            }
        
        # SHORT signal: z >= +Z_ENTRY
        if self.enable_short and z >= self.z_entry:
            if self.use_trend_filter and current_close > ema_trend:
                return {'type': None}  # Skip if above trend EMA
            
            # RSI filter for SHORT
            if self.use_rsi_filter and rsi <= self.rsi_overbought:
                return {'type': None}  # RSI not overbought enough
            
            rsi_str = f"RSI={rsi:.1f}" if rsi is not None else "RSI=N/A"
            
            return {
                'type': 'short',
                'reason': f'SHORT z={z:.2f} | {rsi_str} | ret={ret_str}',
                'price': mid,
                'z': z,
                'ema_n': ema_n,
                'ret_30s': ret_30s,
                'rsi': rsi
            }
        
        return {'type': None}
    
    def check_exit(self, current_price):
        """Check if position should be closed (V5 - MULTI-EXIT)
        
        Exit logic from backtest_hype5.py:
        1. TP/SL hard stops (0.15% / 0.30%)
        2. Z-reversion: z returns near 0 (|z| <= 0.3)
        3. Z-stop: z continues against (|z| >= 3.5)
        4. Time cap: 5 minutes max
        """
        if not self.position:
            return False, None
        
        entry = self.position['entry_price']
        side = self.position['side']
        entry_time = self.position['entry_time']
        
        # Calculate PnL
        if side == 'long':
            pnl_pct = (current_price - entry) / entry
            tp_price = entry * (1 + self.tp_pct)
            sl_price = entry * (1 - self.sl_pct)
        else:
            pnl_pct = (entry - current_price) / entry
            tp_price = entry * (1 - self.tp_pct)
            sl_price = entry * (1 + self.sl_pct)
        
        # 1. HARD TP/SL
        if side == 'long':
            if current_price >= tp_price:
                return True, f'TP ({pnl_pct*100:.2f}%)'
            if current_price <= sl_price:
                return True, f'SL ({pnl_pct*100:.2f}%)'
        else:
            if current_price <= tp_price:
                return True, f'TP ({pnl_pct*100:.2f}%)'
            if current_price >= sl_price:
                return True, f'SL ({pnl_pct*100:.2f}%)'
        
        # 2. TIME CAP
        elapsed = time.time() - entry_time
        if elapsed > self.max_hold_time_sec:
            return True, f'TIME ({pnl_pct*100:.2f}%)'
        
        # 3. Z-BASED EXITS
        z, _, _ = self.calculate_zscore()
        if z is not None:
            if side == 'long':
                # Z-exit: z returns near 0
                if z >= -self.z_exit:
                    return True, f'Z_EXIT z={z:.2f} ({pnl_pct*100:.2f}%)'
                # Z-stop: z continues down
                if z <= -self.z_stop:
                    return True, f'Z_STOP z={z:.2f} ({pnl_pct*100:.2f}%)'
            else:
                # Z-exit: z returns near 0
                if z <= self.z_exit:
                    return True, f'Z_EXIT z={z:.2f} ({pnl_pct*100:.2f}%)'
                # Z-stop: z continues up
                if z >= self.z_stop:
                    return True, f'Z_STOP z={z:.2f} ({pnl_pct*100:.2f}%)'
        
        return False, None
    
    async def open_position(self, side, entry_price):
        """Open a new position (V5 - LONG & SHORT)"""
        if self.position:
            return False
        
        size = self.position_size_usd / entry_price
        size = self._round_size(size)
        
        is_buy = (side == 'long')
        
        emoji = "🟢" if side == 'long' else "🔴"
        print(f"\n{emoji} {side.upper()} SIGNAL @ ${entry_price:.4f}")
        print(f"  Size: {size} {self.market_symbol}")
        
        if self.dry_run:
            print(f"  [DRY RUN] Would open")
            success = True
        else:
            success = await self._place_order(is_buy, size, entry_price, False)
        
        if success:
            self.position = {
                'side': side,
                'entry_price': entry_price,
                'size': size,
                'entry_time': time.time()
            }
            
            self.last_trade_time = time.time()
            self.stats['trades_opened'] += 1
            
            # V5: TP/SL différents selon side
            if side == 'long':
                tgt = entry_price * (1 + self.tp_pct)
                stp = entry_price * (1 - self.sl_pct)
            else:
                tgt = entry_price * (1 - self.tp_pct)
                stp = entry_price * (1 + self.sl_pct)
            
            print(f"  🎯 Target: ${tgt:.4f} ({self.tp_pct*100:+.2f}%)")
            print(f"  🛑 Stop: ${stp:.4f} ({self.sl_pct*100:+.2f}%)")
            print(f"  ⏱️  Max hold: {self.max_hold_time_sec}s\n")
        
        return success
    
    async def close_position(self, current_price, reason):
        """Close existing position"""
        if not self.position:
            return
        
        side = self.position['side']
        entry = self.position['entry_price']
        size = self.position['size']
        
        if side == 'long':
            pnl_pct = (current_price - entry) / entry
        else:
            pnl_pct = (entry - current_price) / entry
        
        pnl_usd = pnl_pct * self.position_size_usd
        
        self.stats['trades_closed'] += 1
        self.stats['total_pnl_usd'] += pnl_usd
        
        if pnl_usd > 0:
            self.stats['wins'] += 1
            emoji = '🟢'
        else:
            self.stats['losses'] += 1
            emoji = '🔴'
        
        wr = (self.stats['wins'] / self.stats['trades_closed'] * 100) if self.stats['trades_closed'] > 0 else 0
        
        print(f"\n{emoji} CLOSING {side.upper()} @ ${current_price:.4f} [{reason}]")
        print(f"  Entry: ${entry:.4f}")
        print(f"  PnL: {pnl_pct*100:+.2f}% (${pnl_usd:+.2f})")
        print(f"  Session: {self.stats['wins']}W/{self.stats['losses']}L ({wr:.0f}% WR) | ${self.stats['total_pnl_usd']:+.2f}")
        
        if self.dry_run:
            print(f"  [DRY RUN] Would close\n")
        else:
            is_buy = (side == 'short')
            await self._place_order(is_buy, size, current_price, True)
        
        self.position = None
    
    async def _place_order(self, is_buy, size, price, is_closing):
        """Place IOC order"""
        try:
            side_str = 'BUY' if is_buy else 'SELL'
            
            # Adjust for slippage
            if is_buy:
                price *= (1 + self.slippage_tolerance_pct / 100)
            else:
                price *= (1 - self.slippage_tolerance_pct / 100)
            
            price = self._round_price(price)
            size = self._round_size(size)
            
            print(f"📤 Order: {side_str} {size} @ ${price:.4f}")
            
            size_dec = self.market_metadata['size_decimals']
            price_dec = self.market_metadata['price_decimals']
            
            base_amount = round(size * (10 ** size_dec))
            limit_price = round(price * (10 ** price_dec))
            
            # Generate unique client_order_index (from Copy-Lighter)
            with self.order_counter_lock:
                self.order_counter += 1
                client_order_index = self.BOT_TAG + (self.order_counter % 1_000_000)
            
            async with self.order_lock:
                tx, tx_hash, err = await self.signer_client.create_order(
                    market_index=self.market_index,
                    is_ask=not is_buy,
                    base_amount=base_amount,
                    price=limit_price,
                    order_type=self.signer_client.ORDER_TYPE_LIMIT,
                    time_in_force=self.signer_client.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
                    reduce_only=is_closing,
                    order_expiry=self.signer_client.DEFAULT_IOC_EXPIRY,
                    client_order_index=client_order_index  # Required parameter
                )
            
            if err:
                print(f"⚠️  Error: {err}")
                return False
            
            print(f"✅ Order OK")
            return True
            
        except Exception as e:
            print(f"❌ Order failed: {e}")
            return False
    
    async def trading_logic_task(self):
        """Separate task for trading logic (runs every 1s)"""
        last_status = time.time()
        
        while not self.stop_event.is_set():
            try:
                await asyncio.sleep(1)
                
                mid = self.orderbook.mid()
                if not mid:
                    continue
                
                # If position open, check exit
                if self.position:
                    should_exit, reason = self.check_exit(mid)
                    
                    if should_exit:
                        await self.close_position(mid, reason)
                    elif (time.time() - last_status) > 30:
                        entry = self.position['entry_price']
                        if self.position['side'] == 'long':
                            pnl = (mid - entry) / entry * 100
                        else:
                            pnl = (entry - mid) / entry * 100
                        
                        hold = int(time.time() - self.position['entry_time'])
                        print(f"{'🟢' if pnl>0 else '🔴'} Pos: {self.position['side'].upper()} @ ${entry:.4f} | ${mid:.4f} | {pnl:+.2f}% | {hold}s")
                        last_status = time.time()
                
                # If no position, check signal
                else:
                    signal = self.check_signal()
                    
                    if signal['type'] in ('long', 'short'):
                        print(f"\n📊 {signal['reason']}")
                        await self.open_position(signal['type'], signal['price'])
                    elif (time.time() - last_status) > 60:  # Logs toutes les 60s
                        # V5.3-MCI: Monitor Z-score and MCI indicators
                        z, ema_n, std_n = self.calculate_zscore()
                        rsi = self.calculate_rsi() if self.use_rsi_filter else None
                        
                        # Calculate ret_30s for display (matches Pine Script)
                        if len(self.bars_1min) >= 2:
                            ret_30s = (self.bars_1min[-1]['close'] - self.bars_1min[-2]['close']) / self.bars_1min[-2]['close']
                        else:
                            ret_30s = None
                        
                        # Data coverage
                        bars_count = len(self.bars_1min)
                        bars_10s_count = len(self.bars_10s)
                        has_data = bars_count >= max(self.n_ema, self.n_std)
                        
                        z_str = f"{z:.2f}" if z is not None else "N/A"
                        rsi_str = f"{rsi:.1f}" if rsi is not None else "N/A"
                        ret_str = f"{ret_30s*100:+.2f}%" if ret_30s is not None else "N/A"
                        mci_str = f"{self.mci:.3f}" if self.mci is not None else "N/A"
                        coverage = "✅" if has_data else "⏳"
                        
                        # State machine status
                        if self.signal_state != "idle":
                            wait_time = int(time.time() - self.armed_time) if self.armed_time else 0
                            if self.signal_state == "armed_long":
                                state_str = f"| 🎯ARMED_LONG (z={self.armed_z:.2f}, {wait_time}s/{self.max_wait_time}s)"
                            elif self.signal_state == "armed_short":
                                state_str = f"| 🎯ARMED_SHORT (z={self.armed_z:.2f}, {wait_time}s/{self.max_wait_time}s)"
                            else:
                                state_str = ""
                        else:
                            state_str = ""
                        
                        if self.use_rsi_filter:
                            print(f"{coverage} ${mid:.4f} | Z: {z_str} | RSI: {rsi_str} | MCI: {mci_str} | Bars:{bars_count}/10s:{bars_10s_count} {state_str} | {datetime.now().strftime('%H:%M:%S')}")
                        else:
                            print(f"{coverage} ${mid:.4f} | Z: {z_str} | MCI: {mci_str} | Bars:{bars_count}/10s:{bars_10s_count} {state_str} | {datetime.now().strftime('%H:%M:%S')}")
                        last_status = time.time()
                
            except Exception as e:
                print(f"❌ Error in trading logic: {e}")
                await asyncio.sleep(1)
    
    async def run_websocket(self):
        """WebSocket main loop (from collector)"""
        
        ob_channel = f"order_book/{self.market_index}"
        trades_channel = f"trade/{self.market_index}"
        
        while not self.stop_event.is_set():
            print(f"\n🔌 Connecting to {self.ws_url}...")
            
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(
                        self.ws_url,
                        heartbeat=30,
                        receive_timeout=None,
                        max_msg_size=10 * 1024 * 1024,
                    ) as ws:
                        self.orderbook.reset()
                        
                        # Subscribe
                        await ws.send_str(json.dumps({"type": "subscribe", "channel": ob_channel}))
                        print(f"✅ Subscribed to {ob_channel}")
                        
                        await ws.send_str(json.dumps({"type": "subscribe", "channel": trades_channel}))
                        print(f"✅ Subscribed to {trades_channel}\n")
                        
                        async for msg in ws:
                            if self.stop_event.is_set():
                                break
                            
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    data = json.loads(msg.data)
                                except:
                                    continue
                                
                                msg_type = data.get("type", "")
                                
                                # Ping
                                if msg_type == "ping":
                                    await ws.send_str(json.dumps({"type": "pong"}))
                                    continue
                                
                                # Orderbook updates
                                if msg_type in ("update/order_book", "snapshot/order_book"):
                                    ob_data = data.get("order_book")
                                    if not ob_data:
                                        continue
                                    
                                    if msg_type == "snapshot/order_book":
                                        self.orderbook.reset()
                                        print(f"[{self.market_symbol}] Snapshot received - reset orderbook")
                                    
                                    if not self.orderbook.apply_update(ob_data):
                                        print("Nonce gap - reconnecting...")
                                        break
                                    
                                    # Sample price at fixed intervals (not every update)
                                    mid = self.orderbook.mid()
                                    if mid:
                                        now = time.time()
                                        # Only sample every 0.5 seconds
                                        if (now - self.last_price_sample) >= self.price_sample_interval:
                                            self.price_history.append({
                                                'timestamp': now,
                                                'mid': mid
                                            })
                                            
                                            # V5: Update 1-minute bars
                                            self.update_1min_bar(now, mid)
                                            
                                            self.last_price_sample = now
                                
                                # Trades
                                elif msg_type == "update/trade":
                                    trades_list = data.get("trades", [])
                                    
                                    for trade_data in trades_list:
                                        price = float(trade_data.get("price", 0))
                                        size = float(trade_data.get("size", 0))
                                        is_ask = trade_data.get("is_ask", False)
                                        
                                        # is_ask True = sell, False = buy
                                        self.trade_tracker.add_trade(
                                            time.time(),
                                            price,
                                            size,
                                            not is_ask  # Inverse for is_buy
                                        )
                            
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                print("WebSocket closed or error")
                                break
            
            except Exception as e:
                print(f"WebSocket error: {e}")
            
            if not self.stop_event.is_set():
                print("Reconnecting in 2s...")
                await asyncio.sleep(2)
    
    async def run(self):
        """Main entry point"""
        
        # Initialize SDK
        if not await self.initialize_sdk():
            return
        
        print("🚀 Starting bot...\n")
        
        # Start both tasks
        ws_task = asyncio.create_task(self.run_websocket())
        trading_task = asyncio.create_task(self.trading_logic_task())
        
        try:
            await asyncio.gather(ws_task, trading_task)
        except Exception as e:
            print(f"Error: {e}")
        finally:
            # Close position if open
            if self.position:
                mid = self.orderbook.mid()
                if mid:
                    await self.close_position(mid, "SHUTDOWN")
            
            # Print stats
            print("\n" + "="*60)
            print("📊 SESSION SUMMARY")
            print("="*60)
            print(f"Opened: {self.stats['trades_opened']}")
            print(f"Closed: {self.stats['trades_closed']}")
            print(f"Wins: {self.stats['wins']}")
            print(f"Losses: {self.stats['losses']}")
            if self.stats['trades_closed'] > 0:
                wr = self.stats['wins'] / self.stats['trades_closed'] * 100
                print(f"Win rate: {wr:.1f}%")
            print(f"Total PnL: ${self.stats['total_pnl_usd']:+.2f}")
            print("="*60 + "\n")


    
    # ═════════════════════════════════════════════════════════════════
    # 10-SECOND BARS (for MCI)
    # ═════════════════════════════════════════════════════════════════
    
    def update_10s_bar(self, price, timestamp):
        """Update 10-second bar (called on every price update)"""
        bar_start = int(timestamp // 10) * 10
        
        if self.current_10s_start is None or bar_start != self.current_10s_start:
            # Close previous 10s bar if exists
            if self.current_10s_start is not None and self.current_10s_close is not None:
                closed_bar = {
                    'timestamp': self.current_10s_start,
                    'close': self.current_10s_close
                }
                self.bars_10s.append(closed_bar)
                
                # Update MCI when new bar closes
                self.update_mci()
            
            # Start new 10s bar
            self.current_10s_start = bar_start
            self.current_10s_close = price
        else:
            # Update current bar
            self.current_10s_close = price
    
    # ═════════════════════════════════════════════════════════════════
    # MCI CALCULATION
    # ═════════════════════════════════════════════════════════════════
    
    def calculate_ema_10s(self, length):
        """Calculate EMA on 10s closes"""
        if len(self.bars_10s) < length:
            return None
        
        closes = [b['close'] for b in self.bars_10s]
        
        alpha = 2 / (length + 1)
        ema = closes[-length]
        
        for i in range(-length + 1, 0):
            ema = alpha * closes[i] + (1 - alpha) * ema
        
        return ema
    
    def update_mci(self):
        """Update MCI (Market Curvature Index) when new 10s bar closes"""
        # Need at least mci_ema_len bars
        if len(self.bars_10s) < self.mci_ema_len:
            return
        
        # Calculate EMA
        ema = self.calculate_ema_10s(self.mci_ema_len)
        if ema is None:
            return
        
        # Store EMA in history
        self.ema10s_hist.append(ema)
        
        # Need at least 3 EMA values for curvature
        if len(self.ema10s_hist) < 3:
            return
        
        # Calculate curvature: ema_t - 2*ema_{t-1} + ema_{t-2}
        ema_t = self.ema10s_hist[-1]
        ema_t1 = self.ema10s_hist[-2]
        ema_t2 = self.ema10s_hist[-3]
        
        curvature = ema_t - 2*ema_t1 + ema_t2
        
        # Calculate sigma (mean of absolute changes over sigma_window)
        if len(self.ema10s_hist) < self.mci_sigma_window:
            # Not enough data yet
            sigma = 1.0  # Default
        else:
            # Get last sigma_window values
            recent_emas = list(self.ema10s_hist)[-self.mci_sigma_window:]
            
            # Calculate absolute changes
            abs_changes = [abs(recent_emas[i] - recent_emas[i-1]) for i in range(1, len(recent_emas))]
            
            # Mean of absolute changes
            sigma = np.mean(abs_changes) if abs_changes else 1.0
        
        # Avoid division by zero
        if sigma == 0:
            sigma = 1e-8
        
        # Store previous MCI
        self.mci_prev = self.mci
        
        # Calculate normalized MCI
        self.mci = curvature / sigma
    
    def check_mci_flip(self, direction):
        """
        Check if MCI has flipped (indicates price pivot)
        
        direction: 'long' or 'short'
        
        LONG: mci_prev < -0.1 AND mci > +0.3 (prix rebondit vers le haut)
        SHORT: mci_prev > +0.1 AND mci < -0.3 (prix plonge vers le bas)
        """
        if self.mci is None or self.mci_prev is None:
            return False
        
        if direction == 'long':
            # Looking for upward flip
            return (self.mci_prev < self.mci_flip_neg and self.mci > self.mci_trigger_pos)
        
        elif direction == 'short':
            # Looking for downward flip
            return (self.mci_prev > self.mci_flip_pos and self.mci < self.mci_trigger_neg)
        
        return False

def main():
    bot = MeanReversionBot()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")


if __name__ == "__main__":
    main()
