# src/hl_verify_wallet/services/backfill/hl_backfill.py
import json, random, time, logging
from typing import List, Optional, Dict, Any
from decimal import Decimal
from ...domain.models import Fill
from ...domain.time_window import TimeWindow
from ...ports.sink import FillSink
from ...adapters.hyperliquid_client import HLClient
from ...services.normalize.trade_row import set_role_from_crossed, _compute_trade_id, _q6
from ...ports.state_repository import StateRepository, WalletState

RETRY_STATUSES = {403, 407, 429, 500, 502, 503, 504}

class HLBackfillService:
    def __init__(
        self,
        client: HLClient,
        *,
        retries: int = 5,
        timeout_sec: float = 15.0,
        tol_sleep_cap: float = 5.0,
    ):
        self.client = client
        self.retries = retries
        self.timeout_sec = timeout_sec
        self.tol_sleep_cap = tol_sleep_cap

    # ---------- low-level page fetch ----------
    def fetch_page(self, wallet: str, start_ms: int, end_ms: Optional[int] = None) -> List[Dict[str, Any]]:
        payload: Dict[str, Any] = {
            "type": "userFillsByTime",
            "user": wallet,
            "startTime": int(start_ms),
            "aggregateByTime": False,
        }
        if end_ms is not None:
            payload["endTime"] = int(end_ms)

        last_err = None
        for a in range(self.retries):
            try:
                r = self.client.request_info(payload, timeout=self.timeout_sec)
                status = getattr(r, "status_code", None)
                data = r.text
                if status == 200:
                    return json.loads(data) or []
                if status in RETRY_STATUSES:
                    last_err = f"http {status}"
                    time.sleep(min(2 ** a, self.tol_sleep_cap) + random.random())
                    continue
                raise RuntimeError(f"HTTP {status}: {str(data)[:200]}")
            except Exception as e:
                last_err = str(e)
                time.sleep(min(2 ** a, self.tol_sleep_cap) + random.random())
        raise RuntimeError(last_err or "request failed")

    # ---------- parse & normalize one fill ----------
    def _parse_fill(self, wallet: str, raw: Dict[str, Any]) -> Optional[Fill]:
        coin = str(raw.get("coin", ""))
        if coin.startswith("@"):
            return None  # ספוט → לא רלוונטי

        # נרמול side ל-A/B
        side_raw = raw.get("side")
        if isinstance(side_raw, str):
            low = side_raw.lower()
            if low in ("ask", "a"):
                side = "A"
            elif low in ("bid", "b"):
                side = "B"
            else:
                side = side_raw
        else:
            side = side_raw

        px = Decimal(str(raw.get("px")))
        sz = Decimal(str(raw.get("sz")))
        ts_ms = int(raw.get("time"))
        base_tid = raw.get("tid")
        base_tid = int(base_tid) if base_tid is not None else None
        h = raw.get("hash")
        crossed = raw.get("crossed")  # יכול להיות None ב-info

        f = Fill(
            wallet=wallet,
            coin=coin,
            side=side,
            px=px,
            sz=sz,
            ts_ms=ts_ms,
            hash=h,
            base_tid=base_tid,
        )
        f = set_role_from_crossed(f, crossed)  # taker/maker אם קיים crossed

        trade_id = _compute_trade_id(f.base_tid, f.side) if f.base_tid is not None else None
        notional = _q6(px * sz)

        return Fill(
            wallet=f.wallet,
            coin=f.coin,
            side=f.side,
            px=f.px,
            sz=f.sz,
            ts_ms=f.ts_ms,
            hash=f.hash,
            base_tid=f.base_tid,
            role=f.role,
            counterparty=None,
            trade_id=trade_id,
            notional_usd=notional,
        )

    # ---------- simple (single-window) backfill ----------
    def process_wallet(self, wallet: str, window: TimeWindow, sink: FillSink) -> int:
        cursor = int(window.start_ms)
        total = 0
        last_cursor: Optional[int] = None

        while True:
            page = self.fetch_page(wallet, cursor, window.end_ms)
            n = len(page)
            if n == 0:
                break

            for f_raw in page:
                parsed = self._parse_fill(wallet, f_raw)
                if parsed is None:
                    continue
                sink.add(parsed)
                total += 1
                if sink.should_flush():
                    sink.flush()

            last_time = int(page[-1]["time"])
            cursor = last_time + 1

            if last_cursor is not None and cursor <= last_cursor:
                logging.warning("cursor did not advance, breaking to avoid loop")
                break
            last_cursor = cursor

        if sink.should_flush():
            sink.flush()

        logging.info("wallet=%s rows=%d", wallet, total)
        return total

    # ---------- upgraded CHUNKED backfill with state ----------
    def process_wallet_chunked(
        self,
        wallet_id: int,
        wallet: str,
        *,
        sink: FillSink,
        state_repo: StateRepository,
        start_ms: int,
        chunk_ms: int,
        ahead_safety_ms: int,
        max_empty_chunks: int,
        start_ms_override: int = 0,
        now_ms_provider=lambda: int(time.time() * 1000),
    ) -> int:
        """
        לוגיקת backfill משודרגת בסגנון השותף:
        - ריצה ב-chunks של זמן (chunk_ms)
        - שמירת state (next_start_ms, finished)
        - עצירה כשהגענו 'קרוב מדי להווה' (ahead_safety_ms)
        - ספירת רצפים של chunks ריקים → סימן שסיימנו
        """
        st = state_repo.load(wallet_id)
        if st.finished:
            logging.info("wallet=%s already finished", wallet)
            return 0

        cursor = int(start_ms_override) if start_ms_override > 0 else (
            int(st.next_start_ms) if st.next_start_ms is not None else int(start_ms)
        )

        total = 0
        empty_chunks = 0

        while True:
            if now_ms_provider() - cursor < ahead_safety_ms:
                # קרוב מדי להווה – מסמנים שסיימנו (נוכל להמשיך בעתיד)
                state_repo.save(wallet_id, cursor, True)
                break

            chunk_start = cursor
            chunk_end = chunk_start + chunk_ms - 1
            page_total = 0
            page_cursor = chunk_start
            sample = None

            while True:
                page = self.fetch_page(wallet, page_cursor, chunk_end)
                n = len(page)
                if n == 0:
                    break

                for f_raw in page:
                    parsed = self._parse_fill(wallet, f_raw)
                    if parsed is None:
                        continue
                    sink.add(parsed)
                    total += 1
                    page_total += 1
                    if sample is None:
                        sample = parsed
                    if sink.should_flush():
                        sink.flush()

                last_time = int(page[-1]["time"])
                page_cursor = last_time + 1
                if last_time >= chunk_end:
                    break

            if sink.should_flush():
                sink.flush()

            if page_total == 0:
                empty_chunks += 1
            else:
                empty_chunks = 0
                if sample:
                    logging.info("sample=%s", json.dumps({
                        "ts_ms": sample.ts_ms, "coin": sample.coin, "side": sample.side,
                        "px": str(sample.px), "sz": str(sample.sz),
                        "base_tid": sample.base_tid, "trade_id": sample.trade_id,
                        "hash": sample.hash
                    }, separators=(",", ":")))
                logging.info("wallet=%s chunk_start=%d rows=%d", wallet, chunk_start, page_total)

            cursor = chunk_end + 1
            state_repo.save(wallet_id, cursor, False)

            if empty_chunks >= max_empty_chunks:
                state_repo.save(wallet_id, cursor, True)
                break

        logging.info("wallet=%s rows=%d", wallet, total)
        return total
