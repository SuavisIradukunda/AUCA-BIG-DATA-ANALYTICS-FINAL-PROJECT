"""Part 1 - HBase query examples (happybase API; runs on HBase or the local emulator).

  1. latest_sessions        last N sessions of a user        -> prefix scan + LIMIT
  2. sessions_in_range      a user's sessions in a window    -> STARTROW/STOPROW scan
  3. session_journey        page-view journey of one session -> get, family 'pv'
  4. product_conversion     view->cart->purchase by week     -> product_metrics range scan
  5. user_engagement        per-user engagement summary      -> prefix scan, family 's' only

Usage:  python -m part1_hbase.query_hbase [--user user_000042] [--product prod_00123]
"""
import argparse
import collections
import datetime
import os
import time

from common import config
from common.hbase_client import get_connection
from part1_hbase.keys import metric_row_key, user_time_range


def latest_sessions(tbl, user_id, n=5):
    cols = [b"s:start", b"s:dur", b"s:device", b"s:referrer", b"s:status", b"s:n_pages"]
    return [(k.decode(), {c.decode(): v.decode() for c, v in d.items()})
            for k, d in tbl.scan(row_prefix=f"{user_id}#".encode(), columns=cols, limit=n)]


def sessions_in_range(tbl, user_id, start, end):
    row_start, row_stop = user_time_range(user_id, start, end)
    return [(k.decode(), d[b"s:start"].decode(), d[b"s:status"].decode())
            for k, d in tbl.scan(row_start=row_start, row_stop=row_stop,
                                 columns=[b"s:start", b"s:status"])]


def session_journey(tbl, row_key):
    data = tbl.row(row_key.encode(), columns=[b"pv", b"c"])
    views = []
    for col in sorted(c for c in data if c.startswith(b"pv:")):
        ts, page, pid, cat, dur = data[col].decode().split("|")
        views.append({"timestamp": ts, "page_type": page, "product_id": pid or None,
                      "category_id": cat or None, "view_duration": int(dur)})
    cart = {c.decode()[2:]: data[c].decode() for c in data if c.startswith(b"c:")}
    return views, cart


def product_conversion(tbl, product_id, start, end):
    """Weekly view -> cart -> purchase conversion for one product."""
    weeks = collections.defaultdict(collections.Counter)
    for key, d in tbl.scan(row_start=metric_row_key(product_id, start),
                           row_stop=metric_row_key(product_id, end)):
        day = datetime.datetime.strptime(key.decode().split("#")[1], "%Y%m%d")
        week = (day - datetime.timedelta(days=day.weekday())).strftime("%Y-%m-%d")
        for col, val in d.items():
            weeks[week][col.decode()[2:]] += float(val)
    rows = []
    for week, c in sorted(weeks.items()):
        views = c["views"]
        rows.append({"week": week, "views": int(views), "cart_adds": int(c["cart_adds"]),
                     "purchases": int(c["purchases"]), "revenue": round(c["revenue"], 2),
                     "view_to_cart_%": round(100 * c["cart_adds"] / views, 1) if views else None,
                     "view_to_purchase_%": round(100 * c["purchases"] / views, 1) if views else None})
    return rows


def user_engagement(tbl, user_id):
    """Engagement summary from a user's session stream (only family 's' is read)."""
    n = dur = pages = converted = abandoned = 0
    first = last = None
    for _, d in tbl.scan(row_prefix=f"{user_id}#".encode(), columns=[b"s"]):
        n += 1
        dur += int(d[b"s:dur"])
        pages += int(d[b"s:n_pages"])
        status = d[b"s:status"].decode()
        converted += status == "converted"
        abandoned += status == "abandoned"
        start = d[b"s:start"].decode()
        last = last or start                      # newest-first order
        first = start
    return {"user_id": user_id, "sessions": n,
            "avg_session_sec": round(dur / n, 1) if n else 0,
            "avg_pages": round(pages / n, 2) if n else 0,
            "converted_sessions": converted, "abandoned_sessions": abandoned,
            "cart_abandonment_rate": round(abandoned / (converted + abandoned), 3)
            if converted + abandoned else None,
            "first_session": first, "last_session": last}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--user", default=None, help="defaults to the first user in the HBase sample")
    ap.add_argument("--product", default="prod_00123")
    args = ap.parse_args()

    conn = get_connection()
    sessions_tbl, metrics_tbl = conn.table("user_sessions"), conn.table("product_metrics")
    user = args.user or next(sessions_tbl.scan(limit=1))[0].decode().split("#")[0]

    out = []
    p = lambda *a: (print(*a), out.append(" ".join(str(x) for x in a)))

    t = time.perf_counter()
    rows = latest_sessions(sessions_tbl, user, 5)
    p(f"\n[1] 5 most recent sessions of {user}  ({(time.perf_counter() - t) * 1000:.1f} ms)")
    for k, d in rows:
        p(f"  {k}  {d}")

    t = time.perf_counter()
    rng = sessions_in_range(sessions_tbl, user, datetime.datetime(2026, 10, 1),
                            datetime.datetime(2026, 10, 16))
    p(f"\n[2] {user} sessions 2026-10-01..2026-10-15: {len(rng)} rows "
      f"({(time.perf_counter() - t) * 1000:.1f} ms)")
    for r in rng[:5]:
        p("  ", r)

    views, cart = session_journey(sessions_tbl, rows[0][0])
    p(f"\n[3] journey of {rows[0][0]}: {len(views)} page views, cart={cart}")
    for v in views:
        p("  ", v)

    t = time.perf_counter()
    conv = product_conversion(metrics_tbl, args.product, "2026-08-01", "2026-11-01")
    p(f"\n[4] weekly conversion for {args.product} ({(time.perf_counter() - t) * 1000:.1f} ms)")
    for r in conv:
        p("  ", r)

    t = time.perf_counter()
    p(f"\n[5] engagement of {user} ({(time.perf_counter() - t) * 1000:.1f} ms):",
      user_engagement(sessions_tbl, user))

    with open(os.path.join(config.RESULTS_DIR, "hbase_query_examples.txt"), "w") as f:
        f.write("\n".join(out) + "\n")


if __name__ == "__main__":
    main()
