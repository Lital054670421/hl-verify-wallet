# src/hl_verify_wallet/adapters/sinks/memory_sink.py
from typing import List
from ...ports.sink import FillSink
from ...domain.models import Fill

class MemorySink(FillSink):
    def __init__(self, batch_size: int = 1000):
        self.rows: List[Fill] = []
        self._batch = batch_size

    def add(self, f: Fill) -> None:
        self.rows.append(f)

    def should_flush(self) -> bool:
        return len(self.rows) % self._batch == 0

    def flush(self) -> None:
        # לזיכרון אין פעולה אמיתית — שומר הכל.
        pass
