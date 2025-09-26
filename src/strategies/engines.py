# English-only comments

# e.g., src/runtime/engines.py
from typing import Dict
from buffers.indicator_buffer_provider import IndicatorBufferProvider
from strategies.decision_engine import DecisionEngine
import buffers.buffer_initializer as buffers

ENGINES: Dict[str, DecisionEngine] = {}  # key: f"{exchange}:{symbol}"

def get_engine(exchange: str, symbol: str) -> DecisionEngine:
    key = f"{exchange}:{symbol}"
    if key not in ENGINES:
        dp = IndicatorBufferProvider(exchange=exchange, indicator_buffer=buffers.INDICATOR_BUFFER)
        ENGINES[key] = DecisionEngine(exchange=exchange, symbol=symbol, dp=dp)
    return ENGINES[key]
