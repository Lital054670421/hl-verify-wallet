from decimal import Decimal, ROUND_HALF_UP
from typing import Optional
from ...domain.models import Fill, TradeRow

_DEC6 = Decimal("0.000001")

def _compute_trade_id(base_tid: int, side: str) -> int:
    """
    כללים שנתת: suffix 1 = BID (B), suffix 2 = ASK (A)
    דוגמה: base_tid=639119470331929, side=B  => 6391194703319291
    """
    suffix = 1 if side == "B" else 2
    return base_tid * 10 + suffix

def _q6(x: Decimal) -> Decimal:
    # כימות ל-6 ספרות עשרוניות (כמו numeric(18,6) אצלכם)
    return x.quantize(_DEC6, rounding=ROUND_HALF_UP)

def to_trade_row_from_fill(f: Fill) -> TradeRow:
    """
    ממיר Fill מכל מקור ל-TradeRow אחיד לפי ה-Columns Dictionary שלך.
    """
    if f.base_tid is None:
        raise ValueError("base_tid is required to compute trade_id")

    # side כבר A/B. נגזור trade_id אם חסר
    trade_id = f.trade_id if f.trade_id is not None else _compute_trade_id(f.base_tid, f.side)

    # role: אם קיבלנו מהמקור – מצוין; אם לא, נשאיר None (Redshift נותן, HL אנו גוזרים)
    role = f.role or "taker"  # ברירת מחדל בטוחה, אבל ל-HL נספק למטה

    # notional_usd: px*sz → ל-6 ספרות
    notional = f.notional_usd if f.notional_usd is not None else _q6(f.px * f.sz)

    return TradeRow(
        trade_id=trade_id,
        base_tid=f.base_tid,
        wallet=f.wallet,
        role=role,
        counterparty=f.counterparty,
        coin=f.coin,
        side=f.side,
        px=_q6(f.px),
        sz=_q6(f.sz),
        notional_usd=_q6(notional),
        hash=f.hash,
        ts_ms=f.ts_ms,
    )

def set_role_from_crossed(fill: Fill, crossed: Optional[bool]) -> Fill:
    """
    HL info: 'crossed' מציין אם ההזמנה חצתה את הספר (taker) או נחה בספר (maker).
    crossed=True => taker, False => maker.
    """
    if crossed is None:
        return fill
    role = "taker" if crossed else "maker"
    return Fill(
        wallet=fill.wallet, coin=fill.coin, side=fill.side,
        px=fill.px, sz=fill.sz, ts_ms=fill.ts_ms, hash=fill.hash,
        base_tid=fill.base_tid, role=role, counterparty=fill.counterparty,
        trade_id=fill.trade_id, notional_usd=fill.notional_usd
    )
