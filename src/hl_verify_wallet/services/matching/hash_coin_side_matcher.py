from decimal import Decimal
from ...ports.match_strategy import MatchStrategy

class HashCoinSideMatchStrategy(MatchStrategy):
    """
    משווה לפי מפתח (hash, coin, side), ומאפשר טולרנסים ל-px/sz/ts
    """
    def __init__(self, tol_px: Decimal, tol_sz: Decimal, tol_ts_ms: int):
        self.tol_px, self.tol_sz, self.tol_ts = tol_px, tol_sz, tol_ts_ms

    def key(self, f) -> tuple:
        return (f.hash or "", f.coin, f.side)

    def equals(self, a, b) -> bool:
        # ברמת איגוד אין צורך בהשוואת px/sz/ts לכל fill – אבל נשמור גמישות
        return (a.coin == b.coin and a.side == b.side and (a.hash == b.hash))
