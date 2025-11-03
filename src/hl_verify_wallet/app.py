import os, json
from decimal import Decimal
from .config import load_config
from .domain.time_window import TimeWindow
from .adapters.redshift_data_api_provider import RedshiftDataApiProvider
from .adapters.hyperliquid_info_provider import HyperliquidInfoProvider
from .services.matching.default_matcher import DefaultMatchStrategy
from .services.matching.hash_coin_side_matcher import HashCoinSideMatchStrategy
from .orchestrators.verify_wallet_usecase import run

def _build_providers(cfg):
    us = RedshiftDataApiProvider(
        workgroup_or_cluster=cfg.redshift_workgroup_or_cluster,
        database=cfg.redshift_database,
        secret_arn=cfg.redshift_secret_arn,
        schema=cfg.redshift_schema,
        table=cfg.redshift_table_trades,
    )
    hl = HyperliquidInfoProvider(
        base_url=cfg.hl_info_url,
        timeout_sec=cfg.hl_timeout_sec,
        retries=cfg.hl_retries,
    )
    return us, hl

def _choose_matcher(mode: str, cfg):
    tol_px = Decimal(os.getenv("TOL_PX", "0.000001"))
    tol_sz = Decimal(os.getenv("TOL_SZ", "0.00000001"))
    tol_ts = int(os.getenv("TOL_TS_MS", "2000"))
    if mode == "grouped":
        return HashCoinSideMatchStrategy(tol_px, tol_sz, tol_ts)
    return DefaultMatchStrategy(tol_px, tol_sz, tol_ts)

def lambda_handler(event, _context=None):
    """
    event:
      {
        "wallet": "...",
        "start_ms": 0,
        "end_ms": 1762093860372,
        "coin": null,
        "mode": "fills" | "grouped"
      }
    """
    cfg = load_config()
    us, hl = _build_providers(cfg)

    wallet = event["wallet"]
    coin = event.get("coin")
    start_ms = int(event["start_ms"])
    end_ms = int(event["end_ms"])
    mode = (event.get("mode") or "fills").lower()
    window = TimeWindow(start_ms, end_ms)

    matcher = _choose_matcher(mode, cfg)
    res = run(wallet, window, us, hl, matcher, coin)

    # פלט JSON מסודר
    return {"statusCode": 200, "body": json.dumps({
        "wallet": wallet,
        "mode": mode,
        **res
    }, ensure_ascii=False)}

if __name__ == "__main__":
    # הרצה לוקאלית: הדבק JSON ל-stdin
    import sys
    payload = json.loads(sys.stdin.read())
    out = lambda_handler(payload, None)
    print(out["body"])
