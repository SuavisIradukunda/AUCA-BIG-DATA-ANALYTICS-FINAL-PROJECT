"""Part 1 - Create the HBase tables and load data.

  user_sessions    ALL sessions of a representative user sample (config.HBASE_SAMPLE_USERS,
                   default 2,000 users = 20% of users, ~400k sessions spanning the full
                   90 days). Sampling by user (not by file) keeps every loaded user's
                   activity stream complete, which is what per-user queries need.
  product_metrics  daily views / cart adds / purchases / units / revenue for EVERY
                   product-day, computed from all 2M sessions and 500k transactions.

Works against real HBase (happybase over Thrift) or the local emulator.

Usage:  python -m part1_hbase.load_hbase [--users N] [--recreate]
"""
import argparse
import collections
import glob
import json
import random
import time

from common import config
from common.hbase_client import get_connection
from part1_hbase.keys import (METRIC_FAMILIES, SESSION_FAMILIES, metric_row_key,
                              session_row_key)


def iter_jsonl(pattern):
    for path in sorted(glob.glob(pattern)):
        with open(path, encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    yield json.loads(line)


def ensure_table(conn, name, families, recreate):
    existing = [t.decode() for t in conn.tables()]
    if name in existing and recreate:
        conn.delete_table(name, disable=True)
        existing.remove(name)
    if name not in existing:
        conn.create_table(name, families)
        print(f"created table {name} with families {list(families)}")
    return conn.table(name)


def session_cells(s):
    """Map one session document to HBase cells (family:qualifier -> value)."""
    d = s["device_profile"]
    g = s["geo_data"]
    cells = {
        b"s:start": s["start_time"], b"s:end": s["end_time"],
        b"s:dur": str(s["duration_seconds"]),
        b"s:device": d["type"], b"s:os": d["os"], b"s:browser": d["browser"],
        b"s:referrer": s["referrer"], b"s:status": s["conversion_status"],
        b"s:city": g["city"], b"s:state": g["state"], b"s:ip": g["ip_address"],
        b"s:n_pages": str(len(s["page_views"])),
        b"s:n_products": str(len(s["viewed_products"])),
    }
    for i, pv in enumerate(s["page_views"]):
        cells[f"pv:{i:03d}".encode()] = "|".join([
            pv["timestamp"], pv["page_type"], pv["product_id"] or "",
            pv["category_id"] or "", str(pv["view_duration"])])
    for pid, item in s["cart_contents"].items():
        cells[f"c:{pid}".encode()] = f"{item['quantity']}|{item['price']}"
    return {k: str(v).encode() for k, v in cells.items()}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--users", type=int, default=config.HBASE_SAMPLE_USERS)
    ap.add_argument("--recreate", action="store_true")
    args = ap.parse_args()
    t0 = time.time()

    conn = get_connection()
    sessions_tbl = ensure_table(conn, "user_sessions", SESSION_FAMILIES, args.recreate)
    metrics_tbl = ensure_table(conn, "product_metrics", METRIC_FAMILIES, args.recreate)

    # Deterministic user sample
    all_users = [u["user_id"] for u in iter_jsonl(config.raw_path("users"))]
    sample = set(random.Random(7).sample(all_users, min(args.users, len(all_users))))
    print(f"sampling {len(sample):,} of {len(all_users):,} users for user_sessions")

    metrics = collections.defaultdict(collections.Counter)   # (pid, yyyymmdd) -> counters
    loaded = 0
    with sessions_tbl.batch(batch_size=2000) as batch:
        for n, s in enumerate(iter_jsonl(config.raw_path("sessions_*")), 1):
            # product metrics from every session
            for pv in s["page_views"]:
                if pv["page_type"] == "product_detail" and pv["product_id"]:
                    metrics[(pv["product_id"], pv["timestamp"][:10])]["views"] += 1
            day = s["start_time"][:10]
            for pid, item in s["cart_contents"].items():
                metrics[(pid, day)]["cart_adds"] += 1
                metrics[(pid, day)]["cart_units"] += item["quantity"]
            # session rows only for the user sample
            if s["user_id"] in sample:
                batch.put(session_row_key(s["user_id"], s["start_time"], s["session_id"]),
                          session_cells(s))
                loaded += 1
            if n % 250000 == 0:
                print(f"  scanned {n:,} sessions, loaded {loaded:,} ({time.time() - t0:.0f}s)")
    print(f"user_sessions: {loaded:,} rows")

    for t in iter_jsonl(config.raw_path("transactions")):
        day = t["timestamp"][:10]
        ratio = t["total"] / t["subtotal"] if t["subtotal"] else 1.0
        for it in t["items"]:
            m = metrics[(it["product_id"], day)]
            m["purchases"] += 1
            m["units_sold"] += it["quantity"]
            m["revenue_cents"] += round(it["subtotal"] * ratio * 100)

    with metrics_tbl.batch(batch_size=5000) as batch:
        for (pid, day), c in metrics.items():
            cells = {f"m:{k}".encode(): str(v).encode()
                     for k, v in c.items() if k != "revenue_cents"}
            cells[b"m:revenue"] = f"{c['revenue_cents'] / 100:.2f}".encode()
            batch.put(metric_row_key(pid, day), cells)
    print(f"product_metrics: {len(metrics):,} rows")
    conn.close()
    print(f"done in {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
