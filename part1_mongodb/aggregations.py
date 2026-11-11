"""Part 1 - MongoDB aggregation pipelines.

  1. revenue_by_category_month  - revenue analytics: net revenue and gross margin per
                                  category per month (order discount allocated to items).
  2. top_products               - product popularity: best sellers by units and revenue,
                                  with distinct buyers, joined to the catalogue.
  3. user_segmentation          - segmentation with $facet: age groups, purchase-frequency
                                  buckets and a data-driven spending quartiles, in one pass.

Results are written to outputs/results/mongo_*.csv and the pipelines themselves to
outputs/results/mongo_pipelines.json (so they can be pasted into mongosh / Compass).

Usage:  python -m part1_mongodb.aggregations
"""
import datetime
import json
import os
import time

import pandas as pd
from pymongo import MongoClient

from common import config

# Reference date for recency calculations (end of the generated 90-day window)
REF_DATE = datetime.datetime(2026, 10, 31, 23, 59, 59)

# --------------------------------------------------------------------------- #
# 1. Revenue and gross margin by category and month                           #
# --------------------------------------------------------------------------- #
REVENUE_BY_CATEGORY_MONTH = [
    {"$match": {"status": {"$in": ["completed", "processing", "shipped", "delivered"]}}},
    # share of the order-level discount that each item carries
    {"$addFields": {"net_ratio": {"$cond": [{"$gt": ["$subtotal", 0]},
                                            {"$divide": ["$total", "$subtotal"]}, 1]}}},
    {"$unwind": "$items"},
    {"$group": {
        "_id": {"category_id": "$items.category_id",
                "month": {"$dateToString": {"format": "%Y-%m", "date": "$timestamp"}}},
        "net_revenue": {"$sum": {"$multiply": ["$items.subtotal", "$net_ratio"]}},
        "gross_margin": {"$sum": {"$multiply": ["$items.subtotal", "$net_ratio",
                                                "$items.profit_margin"]}},
        "units": {"$sum": "$items.quantity"},
        "line_items": {"$sum": 1},
    }},
    {"$lookup": {"from": "categories", "localField": "_id.category_id",
                 "foreignField": "_id", "as": "cat"}},
    {"$project": {
        "_id": 0,
        "category_id": "$_id.category_id",
        "category_name": {"$first": "$cat.name"},
        "month": "$_id.month",
        "net_revenue": {"$round": ["$net_revenue", 2]},
        "gross_margin": {"$round": ["$gross_margin", 2]},
        "margin_pct": {"$round": [{"$multiply": [{"$divide": ["$gross_margin", "$net_revenue"]}, 100]}, 1]},
        "units": 1, "line_items": 1,
    }},
    {"$sort": {"month": 1, "net_revenue": -1}},
]

# --------------------------------------------------------------------------- #
# 2. Product popularity: top sellers                                          #
# --------------------------------------------------------------------------- #
TOP_PRODUCTS = [
    {"$unwind": "$items"},
    {"$group": {
        "_id": "$items.product_id",
        "units_sold": {"$sum": "$items.quantity"},
        "revenue": {"$sum": "$items.subtotal"},
        "orders": {"$sum": 1},
        "buyers": {"$addToSet": "$user_id"},
        "avg_unit_price": {"$avg": "$items.unit_price"},
    }},
    {"$sort": {"units_sold": -1, "revenue": -1}},
    {"$limit": 25},
    {"$lookup": {"from": "products", "localField": "_id", "foreignField": "_id",
                 "pipeline": [{"$project": {"name": 1, "category.name": 1,
                                            "current_stock": 1, "is_active": 1}}],
                 "as": "product"}},
    {"$unwind": "$product"},
    {"$project": {
        "_id": 0, "product_id": "$_id", "name": "$product.name",
        "category": "$product.category.name",
        "units_sold": 1, "orders": 1,
        "revenue": {"$round": ["$revenue", 2]},
        "avg_unit_price": {"$round": ["$avg_unit_price", 2]},
        "distinct_buyers": {"$size": "$buyers"},
        "current_stock": "$product.current_stock",
        "is_active": "$product.is_active",
    }},
]

# --------------------------------------------------------------------------- #
# 3. User segmentation ($facet = three segmentations in one collection scan)   #
# --------------------------------------------------------------------------- #
USER_SEGMENTATION = [
    {"$addFields": {"days_since_purchase": {"$cond": [
        "$purchase_summary.last_purchase",
        {"$dateDiff": {"startDate": "$purchase_summary.last_purchase",
                       "endDate": REF_DATE, "unit": "day"}},
        None]}}},
    {"$facet": {
        "by_age_group": [
            {"$group": {"_id": "$age_group",
                        "users": {"$sum": 1},
                        "avg_spent": {"$avg": "$purchase_summary.total_spent"},
                        "avg_orders": {"$avg": "$purchase_summary.total_orders"},
                        "avg_order_value": {"$avg": "$purchase_summary.avg_order_value"},
                        "total_revenue": {"$sum": "$purchase_summary.total_spent"}}},
            {"$sort": {"_id": 1}},
        ],
        "by_purchase_frequency": [
            {"$bucket": {"groupBy": "$purchase_summary.total_orders",
                         "boundaries": [0, 1, 30, 45, 60, 75, 10000],
                         "default": "other",
                         "output": {"users": {"$sum": 1},
                                    "avg_spent": {"$avg": "$purchase_summary.total_spent"},
                                    "revenue": {"$sum": "$purchase_summary.total_spent"}}}},
        ],
        # Data-driven spending quartiles (fixed thresholds do not work on this dataset:
        # every user buys ~50 times in 90 days, so recency is ~1 day for almost everyone)
        "by_spend_quartile": [
            {"$bucketAuto": {"groupBy": "$purchase_summary.total_spent", "buckets": 4,
                             "output": {"users": {"$sum": 1},
                                        "avg_spent": {"$avg": "$purchase_summary.total_spent"},
                                        "avg_orders": {"$avg": "$purchase_summary.total_orders"},
                                        "avg_order_value": {"$avg": "$purchase_summary.avg_order_value"},
                                        "revenue": {"$sum": "$purchase_summary.total_spent"},
                                        "avg_days_since_purchase": {"$avg": "$days_since_purchase"}}}},
            {"$project": {"_id": 0, "min_spent": "$_id.min", "max_spent": "$_id.max",
                          "users": 1, "avg_spent": 1, "avg_orders": 1, "avg_order_value": 1,
                          "revenue": 1, "avg_days_since_purchase": 1}},
        ],
    }},
]


def run(db, coll, pipeline, name):
    t0 = time.time()
    rows = list(db[coll].aggregate(pipeline, allowDiskUse=True))
    print(f"{name}: {len(rows)} rows in {time.time() - t0:.1f}s")
    return rows


def main():
    db = MongoClient(config.MONGO_URI)[config.MONGO_DB]

    rev = pd.DataFrame(run(db, "transactions", REVENUE_BY_CATEGORY_MONTH, "revenue_by_category_month"))
    rev.to_csv(os.path.join(config.RESULTS_DIR, "mongo_revenue_by_category_month.csv"), index=False)
    print(rev.groupby("month")[["net_revenue", "gross_margin"]].sum().round(0), "\n")

    top = pd.DataFrame(run(db, "transactions", TOP_PRODUCTS, "top_products"))
    top.to_csv(os.path.join(config.RESULTS_DIR, "mongo_top_products.csv"), index=False)
    print(top.head(5).to_string(index=False), "\n")

    seg = run(db, "users", USER_SEGMENTATION, "user_segmentation")[0]
    for facet, rows in seg.items():
        df = pd.DataFrame(rows).rename(columns={"_id": facet.replace("by_", "")}).round(2)
        df.to_csv(os.path.join(config.RESULTS_DIR, f"mongo_segment_{facet}.csv"), index=False)
        print(df.to_string(index=False), "\n")

    with open(os.path.join(config.RESULTS_DIR, "mongo_pipelines.json"), "w") as f:
        json.dump({"revenue_by_category_month": REVENUE_BY_CATEGORY_MONTH,
                   "top_products": TOP_PRODUCTS,
                   "user_segmentation": USER_SEGMENTATION}, f, indent=2, default=str)


if __name__ == "__main__":
    main()
