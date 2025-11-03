# tests/unit/test_hl_backfill.py
import json
from types import SimpleNamespace
from hl_verify_wallet.services.backfill.hl_backfill import HLBackfillService
from hl_verify_wallet.adapters.hyperliquid_client import HLClient
from hl_verify_wallet.adapters.sinks.memory_sink import MemorySink
from hl_verify_wallet.domain.time_window import TimeWindow

class FakeResp:
    def __init__(self, status, payload):
        self.status_code = status
        self.text = json.dumps(payload)

class FakeClient(HLClient):
    def __init__(self, pages):
        # pages: dict[startTime] -> list[rows]
        self.pages = pages
    def request_info(self, payload, timeout=None):
        st = payload["startTime"]
        end = payload.get("endTime")
        data = self.pages.get(st, [])
        return FakeResp(200, data)

def test_backfill_paginates_and_collects():
    # עמוד ראשון (2), עמוד שני (1), ואז ריק
    pages = {
        1000: [
            {"coin": "BTC", "side":"A", "px":"100", "sz":"1", "time": 1000},
            {"coin": "BTC", "side":"B", "px":"101", "sz":"2", "time": 2000},
        ],
        2001: [
            {"coin": "ETH", "side":"A", "px":"200", "sz":"3", "time": 3000}
        ],
        3001: []
    }
    client = FakeClient(pages)
    svc = HLBackfillService(client)
    sink = MemorySink()
    n = svc.process_wallet("0xabc", TimeWindow(1000, 9999999999999), sink)
    assert n == 3
    assert len(sink.rows) == 3
    assert sink.rows[0].coin == "BTC"
    assert sink.rows[2].coin == "ETH"

def test_spot_rows_are_filtered_out():
    pages = {
        0: [
            {"coin": "@107", "side":"A", "px":"1", "sz":"1", "time": 10},  # spot => מסונן
            {"coin": "SOL", "side":"B", "px":"150", "sz":"0.5", "time": 11}
        ],
        12: []
    }
    client = FakeClient(pages)
    svc = HLBackfillService(client)
    sink = MemorySink()
    n = svc.process_wallet("0xabc", TimeWindow(0, 999999), sink)
    assert n == 1
    assert len(sink.rows) == 1
    assert sink.rows[0].coin == "SOL"
