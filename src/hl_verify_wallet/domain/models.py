from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

@dataclass(frozen=True)
class Fill:
    # שדות בסיסיים (קיימים)
    wallet: str
    coin: str
    side: str                 # "A" (sell/ask) | "B" (buy/bid)
    px: Decimal
    sz: Decimal
    ts_ms: int
    hash: Optional[str] = None
    base_tid: Optional[int] = None

    # שדות שנוספו כדי לאפשר נירמול מלא כמו ב-DB
    role: Optional[str] = None            # "maker" | "taker"
    counterparty: Optional[str] = None    # לא קיים ב-HL info, יישאר None
    trade_id: Optional[int] = None        # נגזור מ-base_tid+side
    notional_usd: Optional[Decimal] = None  # px * sz (דיוק 6 ספרות)

@dataclass(frozen=True)
class TradeRow:
    """השורה האחודה להשוואה (כמו ב-DB):"""
    trade_id: int
    base_tid: int
    wallet: str
    role: str               # maker/taker
    counterparty: Optional[str]
    coin: str
    side: str               # A/B
    px: Decimal
    sz: Decimal
    notional_usd: Decimal
    hash: Optional[str]
    ts_ms: int              # נשמור ב-ms; ניתן גם לגזור timestamp במקום אחר
