# src/hl_verify_wallet/domain/time_window.py
from dataclasses import dataclass

@dataclass(frozen=True)
class TimeWindow:
    start_ms: int
    end_ms: int
