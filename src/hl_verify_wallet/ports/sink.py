# src/hl_verify_wallet/ports/sink.py
from abc import ABC, abstractmethod
from ..domain.models import Fill

class FillSink(ABC):
    """יעד כתיבה גנרי (לזיכרון/DB/קובץ)."""
    @abstractmethod
    def add(self, f: Fill) -> None: ...
    @abstractmethod
    def should_flush(self) -> bool: ...
    @abstractmethod
    def flush(self) -> None: ...
