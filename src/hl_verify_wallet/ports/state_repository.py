from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

@dataclass(frozen=True)
class WalletState:
    wallet_id: int
    next_start_ms: Optional[int]
    finished: bool

class StateRepository(ABC):
    @abstractmethod
    def load(self, wallet_id: int) -> WalletState: ...
    @abstractmethod
    def save(self, wallet_id: int, next_start_ms: int, finished: bool) -> None: ...
