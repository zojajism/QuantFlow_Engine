# English-only comments

from dataclasses import dataclass, field
from typing import Dict, Optional, Literal, Protocol, Any
from datetime import datetime, timezone
import logging

from database.insert_bios_signal import insert_bios_signal_to_db_async

logger = logging.getLogger(__name__)

Signal = Literal["BUY", "SELL", "HOLD"]
Bias   = Literal["Bullish", "Bearish", "Neutral"]

# ---------- Data Access Contract ----------
class DataProvider(Protocol):
    def get_point(self, symbol: str, tf: str, name: str, offset: int = 0) -> Optional[dict]: ...

# ---------- Portfolio (paper trading) ----------
@dataclass
class Portfolio:
    balance_usd: float = 1000.0
    position_qty: float = 0.0
    entry_price: float = 0.0

    def is_flat(self) -> bool:
        return self.position_qty == 0.0

    def open_long_full(self, price: float):
        if not self.is_flat() or price <= 0:
            return
        qty = self.balance_usd / price
        self.position_qty = qty
        self.entry_price = price
        self.balance_usd = 0.0

    def close_long_all(self, price: float):
        if self.is_flat() or price <= 0:
            return
        self.balance_usd = self.position_qty * price
        self.position_qty = 0.0
        self.entry_price = 0.0

# ---------- Trade Log ----------
@dataclass
class Trade:
    timestamp: int
    symbol: str
    side: Literal["BUY", "SELL"]
    price: float
    qty: float

# ---------- Decision Engine ----------
@dataclass
class DecisionEngine:
    exchange: str         # NEW: so we don’t hardcode "Binance"
    symbol: str
    dp: DataProvider
    portfolio: Portfolio = field(default_factory=Portfolio)

    # Bias state updated only on HTF candle close
    bias_state: Dict[str, Bias] = field(default_factory=lambda: {"15m": "Neutral", "1h": "Neutral"})
    trades: list = field(default_factory=list)

    # Indicators that can carry a reliable close_price/close_time
    _price_carriers: tuple = (
        "ema8", "ema21", "sma50", "sma200",
        "macd_line", "macd_signal",
        "rsi14", "stoch_k", "stoch_d", "cci",
        "bb_upper", "bb_lower",
        "atr", "atr_ma",
        "obv", "mfi",
        "vol", "vol_ma",
    )

    # ------- Public entrypoint (async) -------
    async def on_candle_close(self, tf: str, ts: int) -> Signal:
        """
        Call this on every candle close.
          - If tf in ('15m','1h'): update bias; insert bias row; return HOLD.
          - If tf == '1m': compute score, map to signal, apply bias filter, execute, insert signal row.
        Note: ts is a unix epoch seconds (optional fallback); we prefer close_time from indicator points.
        """
        # Get canonical timestamp & close_price from indicator points (offset=0)
        timestamp_dt = self._get_close_time(tf, offset=0)
        if timestamp_dt is None and isinstance(ts, (int, float)):
            # Fallback to provided epoch seconds (assumed UTC)
            timestamp_dt = datetime.fromtimestamp(ts, tz=timezone.utc)

        # --- HTF: update bias & insert row ---
        if tf in ("15m", "1h"):
            score_for_bias = self._compute_total_score(tf)
            bias = self._score_to_bias(score_for_bias)
            self.bias_state[tf] = bias

            close_price = self._get_close_price(tf, offset=0) or 0.0

            logger.info(f"[BIAS] symbol: {self.symbol}, timeframe={tf}, close_price={close_price}, score={score_for_bias:.2f} → {bias}")

            if timestamp_dt:
                await insert_bios_signal_to_db_async(
                    exchange=self.exchange,
                    symbol=self.symbol,
                    timeframe=tf,
                    timestamp=timestamp_dt,       # already UTC datetime
                    close_price=close_price,
                    score=score_for_bias,
                    bios=bias,                    # "Bullish"/"Bearish"/"Neutral"
                    raw_signal="",                    # empty for HTF
                    final_signal="",                    # empty for HTF
                )
            return "HOLD"

        # --- LTF: compute signal, filter by bias, execute & insert row ---
        if tf == "1m":
            total_score = self._compute_total_score(tf="1m")
            raw_signal = self._score_to_signal(total_score)
            final_signal = self._apply_bias_filter(raw_signal)

            close_price = self._get_close_price("1m", offset=0) or 0.0

            self._paper_execute(final_signal, ts, tf="1m")

            logger.info(f"[SIGNAL] symbol: {self.symbol}, timeframe=1m, close_price={close_price}, score={total_score:.2f}, raw signal={raw_signal}, final signal={final_signal}")

            if timestamp_dt:
                await insert_bios_signal_to_db_async(
                    exchange=self.exchange,
                    symbol=self.symbol,
                    timeframe="1m",
                    timestamp=timestamp_dt,      # already UTC datetime
                    close_price=close_price,
                    score=total_score,
                    raw_signal=raw_signal,       # "BUY"/"SELL"/"HOLD"
                    final_signal=final_signal,   # "BUY"/"SELL"/"HOLD"
                    bios="",                     # empty for 1m
                )
            return final_signal

        return "HOLD"

    # ------- Helpers to fetch price/time/value from indicator points -------
    def _get_close_price(self, tf: str, offset: int = 0) -> Optional[float]:
        for name in self._price_carriers:
            pt = self.dp.get_point(self.symbol, tf, name, offset=offset)
            if pt and isinstance(pt, dict):
                price = pt.get("close_price")
                if isinstance(price, (int, float)) and price > 0:
                    return float(price)
        return None

    def _get_close_time(self, tf: str, offset: int = 0) -> Optional[datetime]:
        for name in self._price_carriers:
            pt = self.dp.get_point(self.symbol, tf, name, offset=offset)
            if pt and isinstance(pt, dict):
                ct = pt.get("close_time")
                if isinstance(ct, datetime):
                    # Assume it's already UTC (as per your system)
                    return ct
        return None

    def _val(self, tf: str, name: str, offset: int = 0) -> Optional[float]:
        pt = self.dp.get_point(self.symbol, tf, name, offset=offset)
        if pt and isinstance(pt, dict):
            v = pt.get("value")
            if isinstance(v, (int, float)):
                return float(v)
        return None

    # ------- Scoring / Bias -------
    def _compute_total_score(self, tf: str) -> float:
        total = 0.0

        price      = self._get_close_price(tf, offset=0)
        prev_price = self._get_close_price(tf, offset=1)

        # --- Trend ---
        ema8  = self._val(tf, "ema8")
        ema21 = self._val(tf, "ema21")
        if ema8 is not None and ema21 is not None:
            if ema8 > ema21:   total += 2.0
            elif ema8 < ema21: total -= 2.0

        sma50  = self._val(tf, "sma50")
        sma200 = self._val(tf, "sma200")
        if price is not None and sma50 is not None and sma200 is not None:
            if price > sma50 and price > sma200:   total += 1.0
            elif price < sma50 and price < sma200: total -= 1.0

        macd_line = self._val(tf, "macd_line")
        macd_sig  = self._val(tf, "macd_signal")
        if macd_line is not None and macd_sig is not None:
            if macd_line > macd_sig:   total += 1.0
            elif macd_line < macd_sig: total -= 1.0

        # --- Momentum ---
        rsi = self._val(tf, "rsi14")
        if rsi is not None:
            if rsi > 50:   total += 1.0
            elif rsi < 50: total -= 1.0

        st_k = self._val(tf, "stoch_k")
        st_d = self._val(tf, "stoch_d")
        if st_k is not None and st_d is not None:
            if st_k > st_d and st_k < 20:   total += 1.0
            elif st_k < st_d and st_k > 80: total -= 1.0

        cci = self._val(tf, "cci")
        if cci is not None:
            if cci > 100:     total += 1.0
            elif cci < -100:  total -= 1.0

        # --- Volatility ---
        bb_up = self._val(tf, "bb_upper")
        bb_lo = self._val(tf, "bb_lower")
        if price is not None and bb_up is not None and bb_lo is not None:
            if price > bb_up:   total += 1.0
            elif price < bb_lo: total -= 1.0

        atr    = self._val(tf, "atr")
        atr_ma = self._val(tf, "atr_ma")
        if atr is not None and atr_ma is not None:
            if atr > atr_ma:   total += 0.5
            elif atr < atr_ma: total -= 0.5

        # --- Volume ---
        if price is not None and prev_price is not None:
            if price > prev_price:   total += 1.0
            elif price < prev_price: total -= 1.0

        mfi = self._val(tf, "mfi")
        if mfi is not None:
            if mfi > 50:   total += 1.0
            elif mfi < 50: total -= 1.0

        vol    = self._val(tf, "vol")
        vol_ma = self._val(tf, "vol_ma")
        if vol is not None and vol_ma is not None and price is not None and prev_price is not None:
            if vol > vol_ma and price > prev_price:   total += 0.5
            elif vol > vol_ma and price < prev_price: total -= 0.5

        return total

    def _score_to_signal(self, total: float) -> Signal:
        if total >= 3.0:   return "BUY"
        if total <= -3.0:  return "SELL"
        return "HOLD"

    def _score_to_bias(self, total: float) -> Bias:
        if total >= 2.0:   return "Bullish"
        if total <= -2.0:  return "Bearish"
        return "Neutral"

    def _apply_bias_filter(self, raw: Signal) -> Signal:
        b15 = self.bias_state.get("15m", "Neutral")
        b1h = self.bias_state.get("1h", "Neutral")
        if raw == "BUY":
            return "BUY" if (b15 == "Bullish" and b1h == "Bullish") else "HOLD"
        if raw == "SELL":
            return "SELL" if (b15 == "Bearish" and b1h == "Bearish") else "HOLD"
        return "HOLD"

    # ------- Paper execution -------
    def _paper_execute(self, signal: Signal, ts: int, tf: str):
        price = self._get_close_price(tf, offset=0)
        if price is None or price <= 0:
            return

        if signal == "BUY" and self.portfolio.is_flat():
            self.portfolio.open_long_full(price)
            self.trades.append(Trade(ts, self.symbol, "BUY", price, self.portfolio.position_qty))

        elif signal == "SELL" and not self.portfolio.is_flat():
            qty = self.portfolio.position_qty
            self.portfolio.close_long_all(price)
            self.trades.append(Trade(ts, self.symbol, "SELL", price, qty))
