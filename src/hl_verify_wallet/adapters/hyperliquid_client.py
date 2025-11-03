# src/hl_verify_wallet/adapters/hyperliquid_client.py
from typing import Dict, Any, Optional
import httpx

class HLClient:
    """לקוח מינימלי ל-/info של Hyperliquid (POST JSON)."""
    def __init__(self, base_url: str, timeout: float = 15.0):
        self._base = base_url.rstrip("/")
        self._client = httpx.Client(timeout=timeout)

    def request_info(self, payload: Dict[str, Any], timeout: Optional[float] = None) -> httpx.Response:
        # הדוקס: POST https://api.hyperliquid.xyz/info עם JSON בגוף.  :contentReference[oaicite:5]{index=5}
        return self._client.post(self._base, json=payload, timeout=timeout)

    def close(self) -> None:
        self._client.close()

    def __enter__(self): return self
    def __exit__(self, exc_type, exc, tb): self.close()
