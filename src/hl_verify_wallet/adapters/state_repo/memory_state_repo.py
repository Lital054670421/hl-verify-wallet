from typing import Dict
from ...ports.state_repository import StateRepository, WalletState

class MemoryStateRepository(StateRepository):
    def __init__(self):
        self._db: Dict[int, WalletState] = {}

    def load(self, wallet_id: int) -> WalletState:
        st = self._db.get(wallet_id)
        if st is None:
            return WalletState(wallet_id=wallet_id, next_start_ms=None, finished=False)
        return st

    def save(self, wallet_id: int, next_start_ms: int, finished: bool) -> None:
        self._db[wallet_id] = WalletState(wallet_id=wallet_id, next_start_ms=next_start_ms, finished=finished)
