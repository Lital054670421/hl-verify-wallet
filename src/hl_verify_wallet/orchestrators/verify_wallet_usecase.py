from typing import Optional, Dict, Any, List
from ..domain.time_window import TimeWindow
from ..ports.fill_provider import FillProvider
from ..services.normalize.trade_row import to_trade_row_from_fill
from decimal import Decimal

def _normalize_all(fills) -> List:
    # אם תרצה להשוות על Fill “כמו שהוא” השאר ככה.
    # אם תרצה להשוות על TradeRow, החלף לשורה:
    # return [to_trade_row_from_fill(f) for f in fills]
    return list(fills)

def run(wallet: str, window: TimeWindow, us_provider: FillProvider,
        hl_provider: FillProvider, matcher, coin: Optional[str]=None) -> Dict[str, Any]:
    us_fills = list(us_provider.fetch_fills(wallet, window, coin))
    hl_fills = list(hl_provider.fetch_fills(wallet, window, coin))

    us_rows = _normalize_all(us_fills)
    hl_rows = _normalize_all(hl_fills)

    from ..services.compare_service import compare
    return compare(us_rows, hl_rows, matcher)
