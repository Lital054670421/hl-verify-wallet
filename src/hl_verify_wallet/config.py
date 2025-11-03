# src/hl_verify_wallet/config.py  (רק התוספות/ברירות מחדל)
class Config(BaseModel):
    hl_info_url: str = os.getenv("HL_INFO_URL", "https://api.hyperliquid.xyz/info")
    hl_timeout_sec: float = float(os.getenv("HL_TIMEOUT_SEC", "15"))
    hl_retries: int = int(os.getenv("HL_RETRIES", "5"))
    # ... השאר כבעבר
