# src/hl_verify_wallet/adapters/hyperliquid_info_provider.py
from typing import Iterable, Optional
from decimal import Decimal
from ..ports.fill_provider import FillProvider
from ..domain.models import Fill
from ..domain.time_window import TimeWindow
from .hyperliquid_client import HLClient
from ..services.backfill.hl_backfill import HLBackfillService
from .sinks.memory_sink import MemorySink

class HyperliquidInfoProvider(FillProvider):
    def __init__(self, base_url: str, timeout_sec: float = 15.0, retries: int = 5):
        self._base = base_url.rstrip("/")
        self._timeout = timeout_sec
        self._retries = retries

    def fetch_fills(self, wallet: str, window: TimeWindow, coin: Optional[str] = None) -> Iterable[Fill]:
        # הערה: userFillsByTime לא תומך בפרמטר 'n' — ההחזר מוגבל ע"י השרת.  :contentReference[oaicite:13]{index=13}
        with HLClient(self._base, timeout=self._timeout) as client:
            svc = HLBackfillService(client, retries=self._retries, timeout_sec=self._timeout)
            sink = MemorySink()
            total = svc.process_wallet(wallet, window, sink)
            # סינון coin אם התבקש
            rows = sink.rows
            if coin:
                rows = [r for r in rows if r.coin == coin]
            return rows
