# scripts/fetch_wallet_demo.py
import argparse, time, json
from collections import defaultdict

from hl_verify_wallet.adapters.hyperliquid_client import HLClient
from hl_verify_wallet.services.backfill.hl_backfill import HLBackfillService
from hl_verify_wallet.adapters.sinks.memory_sink import MemorySink
from hl_verify_wallet.domain.time_window import TimeWindow

def now_ms() -> int:
    return int(time.time() * 1000)

def row_to_dict(r):
    return {
        "ts_ms": r.ts_ms, "coin": r.coin, "side": r.side,
        "px": str(r.px), "sz": str(r.sz),
        "base_tid": r.base_tid, "trade_id": r.trade_id,
        "notional_usd": str(r.notional_usd),
        "hash": r.hash
    }

def main():
    p = argparse.ArgumentParser(description="Live fetch demo from Hyperliquid (fills backfill)")
    p.add_argument("--wallet", required=True, help="Wallet address to fetch")
    p.add_argument("--start-ms", type=int, default=0, help="Start timestamp (ms since epoch). Default=0")
    p.add_argument("--end-ms", type=int, default=None, help="End timestamp (ms since epoch). Default=now")
    p.add_argument("--base-url", default="https://api.hyperliquid.xyz/info", help="HL info URL")
    p.add_argument("--timeout", type=float, default=15.0)
    p.add_argument("--retries", type=int, default=5)
    p.add_argument("--sample", type=int, default=3, help="How many sample rows to print from head/tail")
    args = p.parse_args()

    end_ms = args.end_ms or int(time.time() * 1000)
    print(f"[i] Fetching fills for wallet={args.wallet}, window=[{args.start_ms}, {end_ms}]")

    with HLClient(args.base_url, timeout=args.timeout) as client:
        svc = HLBackfillService(client, retries=args.retries, timeout_sec=args.timeout)
        sink = MemorySink(batch_size=1000)
        svc.process_wallet(args.wallet, TimeWindow(args.start_ms, end_ms), sink)

    rows = sink.rows

    total_fills = len(rows)
    coins = sorted({r.coin for r in rows})
    has_spot = any(r.coin.startswith("@") for r in rows)
    distinct_base_tid = len({r.base_tid for r in rows if r.base_tid is not None})
    distinct_trade_id = len({r.trade_id for r in rows if r.trade_id is not None})
    distinct_hash = len({r.hash for r in rows if r.hash})
    distinct_hash_coin_side = len({(r.hash, r.coin, r.side) for r in rows if r.hash})

    print(json.dumps({
        "wallet": args.wallet,
        "fills_total": total_fills,
        "distinct_base_tid": distinct_base_tid,
        "distinct_trade_id": distinct_trade_id,
        "distinct_hash": distinct_hash,
        "distinct_hash_coin_side": distinct_hash_coin_side,
        "unique_coins": coins,
        "contains_spot_rows": has_spot
    }, ensure_ascii=False, indent=2))

    if rows:
        print("\n[i] First rows:")
        print(json.dumps([row_to_dict(x) for x in rows[:args.sample]], ensure_ascii=False, indent=2))
        print("\n[i] Last rows:")
        print(json.dumps([row_to_dict(x) for x in rows[-args.sample:]], ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
