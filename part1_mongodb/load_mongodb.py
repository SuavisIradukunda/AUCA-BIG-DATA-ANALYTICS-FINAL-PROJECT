"""Part 1 - Load the e-commerce dataset into MongoDB using the document model.

Collections and design decisions (see report section 2.1):

  categories    one document per category with its subcategories EMBEDDED
                (small, bounded array of 3-5 items, always read together).

  products      product catalogue; the category name and the full subcategory
                (name, profit_margin) are DENORMALISED into the product, and the
                price_history array stays EMBEDDED (bounded: 1-3 entries).
                -> a product page or a margin calculation needs one read, no $lookup.

  transactions  one document per order with line items EMBEDDED. Each item also
                carries category_id / subcategory_id / profit_margin copied from the
                catalogue at load time ("snapshot" pattern) so revenue and margin by
                category are a single $unwind + $group.

  users         profile + EMBEDDED `purchase_summary` (orders, spend, AOV, first /
                last purchase, top categories) computed from the transactions
                ("computed pattern"): segmentation queries read only `users`.

Sessions are NOT stored in MongoDB: they are a high-volume, append-only, time-
ordered event stream, which is what HBase is for (see part1_hbase).

Usage:  python -m part1_mongodb.load_mongodb [--drop]
"""
import argparse
import collections
import datetime
import json
import time

from pymongo import ASCENDING, DESCENDING, IndexModel, MongoClient
from pymongo.errors import BulkWriteError

from common import config

BATCH = 10000


def read_jsonl(name):
    with open(config.raw_path(name), encoding="utf-8") as f:
        for line in f:
            if line.strip():
                yield json.loads(line)


def to_dt(s):
    return datetime.datetime.fromisoformat(s) if s else None


def insert_batches(coll, docs):
    """Insert an iterable of documents in unordered batches (robust to duplicates)."""
    batch, total = [], 0
    for doc in docs:
        batch.append(doc)
        if len(batch) >= BATCH:
            total += _flush(coll, batch)
            batch = []
    if batch:
        total += _flush(coll, batch)
    return total


def _flush(coll, batch):
    try:
        return len(coll.insert_many(batch, ordered=False).inserted_ids)
    except BulkWriteError as e:   # e.g. duplicate _id on re-run without --drop
        dup = sum(1 for err in e.details["writeErrors"] if err["code"] == 11000)
        print(f"  warning: {len(e.details['writeErrors'])} write errors ({dup} duplicates) skipped")
        return e.details["nInserted"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--drop", action="store_true", help="drop the database first")
    args = ap.parse_args()

    client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    if args.drop:
        client.drop_database(config.MONGO_DB)
    db = client[config.MONGO_DB]
    t0 = time.time()

    # ---- categories ---------------------------------------------------------
    categories = list(read_jsonl("categories"))
    sub_index = {}
    for c in categories:
        c["_id"] = c["category_id"]
        for s in c["subcategories"]:
            sub_index[s["subcategory_id"]] = {**s, "category_id": c["category_id"],
                                              "category_name": c["name"]}
    insert_batches(db.categories, categories)
    print(f"categories:   {db.categories.count_documents({}):,}")

    # ---- products -----------------------------------------------------------
    product_index = {}

    def product_docs():
        for p in read_jsonl("products"):
            sub = sub_index.get(p.get("subcategory_id"), {})
            doc = {
                "_id": p["product_id"],
                "name": p["name"],
                "category": {"category_id": p["category_id"],
                             "name": sub.get("category_name")},
                "subcategory": {"subcategory_id": p.get("subcategory_id"),
                                "name": sub.get("name"),
                                "profit_margin": sub.get("profit_margin")},
                "base_price": p["base_price"],
                "current_stock": p["current_stock"],
                "is_active": p["is_active"],
                "price_history": [{"price": h["price"], "date": to_dt(h["date"])}
                                  for h in p["price_history"]],
                "creation_date": to_dt(p["creation_date"]),
            }
            product_index[p["product_id"]] = doc
            yield doc

    insert_batches(db.products, product_docs())
    db.products.create_indexes([
        IndexModel([("category.category_id", ASCENDING), ("base_price", ASCENDING)]),
        IndexModel([("subcategory.subcategory_id", ASCENDING)]),
        IndexModel([("is_active", ASCENDING), ("current_stock", ASCENDING)]),
    ])
    print(f"products:     {db.products.count_documents({}):,}")

    # ---- transactions (+ accumulate per-user purchase summaries) -----------
    summary = collections.defaultdict(lambda: {
        "total_orders": 0, "total_spent": 0.0, "total_items": 0,
        "first_purchase": None, "last_purchase": None,
        "category_spend": collections.Counter()})

    def transaction_docs():
        for t in read_jsonl("transactions"):
            ts = to_dt(t["timestamp"])
            items = []
            for it in t["items"]:
                prod = product_index.get(it["product_id"], {})
                items.append({
                    **it,
                    "category_id": prod.get("category", {}).get("category_id"),
                    "subcategory_id": prod.get("subcategory", {}).get("subcategory_id"),
                    "profit_margin": prod.get("subcategory", {}).get("profit_margin"),
                })
            s = summary[t["user_id"]]
            s["total_orders"] += 1
            s["total_spent"] += t["total"]
            s["total_items"] += sum(i["quantity"] for i in items)
            s["first_purchase"] = min(filter(None, [s["first_purchase"], ts]))
            s["last_purchase"] = max(filter(None, [s["last_purchase"], ts]))
            for i in items:
                s["category_spend"][i["category_id"]] += i["subtotal"]
            yield {
                "_id": t["transaction_id"],
                "session_id": t["session_id"],
                "user_id": t["user_id"],
                "timestamp": ts,
                "channel": "web_session" if t["session_id"] else "direct",
                "items": items,
                "item_count": len(items),
                "subtotal": t["subtotal"],
                "discount": t["discount"],
                "total": t["total"],
                "payment_method": t["payment_method"],
                "status": t["status"],
            }

    n = insert_batches(db.transactions, transaction_docs())
    db.transactions.create_indexes([
        IndexModel([("user_id", ASCENDING), ("timestamp", DESCENDING)]),
        IndexModel([("timestamp", ASCENDING)]),
        IndexModel([("session_id", ASCENDING)], sparse=True),
        IndexModel([("items.product_id", ASCENDING)]),
    ])
    print(f"transactions: {n:,}")

    # ---- users with embedded purchase summary -------------------------------
    def age_group(age):
        for lo, hi in ((18, 24), (25, 34), (35, 44), (45, 54)):
            if lo <= age <= hi:
                return f"{lo}-{hi}"
        return "55+"

    def user_docs():
        for u in read_jsonl("users"):
            s = summary.get(u["user_id"])
            ps = {"total_orders": 0, "total_spent": 0.0, "avg_order_value": 0.0,
                  "total_items": 0, "first_purchase": None, "last_purchase": None,
                  "top_categories": []}
            if s:
                ps = {
                    "total_orders": s["total_orders"],
                    "total_spent": round(s["total_spent"], 2),
                    "avg_order_value": round(s["total_spent"] / s["total_orders"], 2),
                    "total_items": s["total_items"],
                    "first_purchase": s["first_purchase"],
                    "last_purchase": s["last_purchase"],
                    "top_categories": [{"category_id": c, "spent": round(v, 2)}
                                       for c, v in s["category_spend"].most_common(3)],
                }
            yield {
                "_id": u["user_id"],
                "age": u.get("age"),
                "age_group": age_group(u["age"]) if u.get("age") else None,
                "geo_data": u["geo_data"],
                "registration_date": to_dt(u["registration_date"]),
                "last_active": to_dt(u["last_active"]),
                "purchase_summary": ps,
            }

    insert_batches(db.users, user_docs())
    db.users.create_indexes([
        IndexModel([("age_group", ASCENDING)]),
        IndexModel([("geo_data.state", ASCENDING)]),
        IndexModel([("purchase_summary.total_spent", DESCENDING)]),
    ])
    print(f"users:        {db.users.count_documents({}):,}")
    print(f"Loaded in {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
