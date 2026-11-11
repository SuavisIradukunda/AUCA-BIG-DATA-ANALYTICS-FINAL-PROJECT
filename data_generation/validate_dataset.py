import json, glob, sys, collections, datetime
d = sys.argv[1]
L = lambda f: [json.loads(l) for l in open(f)] if f.endswith(".jsonl") else json.load(open(f))
X = "jsonl" if glob.glob(f"{d}/users.jsonl") else "json"
users = {u["user_id"]: u for u in L(f"{d}/users.{X}")}
cats = {c["category_id"]: c for c in L(f"{d}/categories.{X}")}
prods = {p["product_id"]: p for p in L(f"{d}/products.{X}")}
txns = L(f"{d}/transactions.{X}")
err = collections.Counter()
subs = {s["subcategory_id"]: c["category_id"] for c in cats.values() for s in c["subcategories"]}
for p in prods.values():
    if subs.get(p.get("subcategory_id")) != p["category_id"]: err["product subcategory mismatch"] += 1
    if p["current_stock"] == 0 and p["is_active"]: err["zero stock but active"] += 1
    if p["current_stock"] < 0: err["negative stock"] += 1
txn_by_sess = collections.Counter(t["session_id"] for t in txns if t["session_id"])
sold = collections.Counter()
ref = "2026-10-31T23:59:59"
for t in txns:
    if t["user_id"] not in users: err["txn unknown user"] += 1
    if t["timestamp"] > ref: err["future txn"] += 1
    if round(sum(i["subtotal"] for i in t["items"]), 2) != t["subtotal"]: err["subtotal mismatch"] += 1
    if round(t["subtotal"] - t["discount"], 2) != t["total"]: err["total mismatch"] += 1
    for i in t["items"]: sold[i["product_id"]] += i["quantity"]
last_act = {}
ns = 0; statuses = collections.Counter(); sess_ids = set(); dur0 = 0
prev_ts = ""
for f in sorted(glob.glob(f"{d}/sessions_*.{X}"), key=lambda x: int(x.split("_")[-1].split(".")[0])):
    for s in L(f):
        ns += 1; sess_ids.add(s["session_id"]); statuses[s["conversion_status"]] += 1
        if s["start_time"] < prev_ts: err["sessions not chronological"] += 1
        prev_ts = s["start_time"]
        if s["end_time"] > ref: err["future session"] += 1
        if s["user_id"] not in users: err["session unknown user"] += 1
        n = txn_by_sess.get(s["session_id"], 0)
        if n > 1: err["session >1 txn"] += 1
        if (s["conversion_status"] == "converted") != (n == 1): err["status/txn mismatch"] += 1
        if s["conversion_status"] == "abandoned" and not s["cart_contents"]: err["abandoned w/ empty cart"] += 1
        if s["conversion_status"] == "browsed" and s["cart_contents"]: err["browsed w/ cart"] += 1
        if not set(s["cart_contents"]) <= set(s["viewed_products"]): err["cart item not viewed"] += 1
        pv = [p["product_id"] for p in s["page_views"] if p["page_type"] == "product_detail"]
        if list(dict.fromkeys(pv)) != s["viewed_products"]: err["viewed order"] += 1
        if any(p["view_duration"] <= 0 for p in s["page_views"]): dur0 += 1
        if sum(p["view_duration"] for p in s["page_views"]) != s["duration_seconds"]: err["duration sum"] += 1
        last_act[s["user_id"]] = max(last_act.get(s["user_id"], ""), s["end_time"])
for t in txns: last_act[t["user_id"]] = max(last_act.get(t["user_id"], ""), t["timestamp"])
for uid, u in users.items():
    if u["last_active"] != max(last_act.get(uid, u["registration_date"]), u["registration_date"]): err["last_active wrong"] += 1
if set(txn_by_sess) - sess_ids: err["txn references missing session"] += 1
print("sessions", ns, dict(statuses), "txns", len(txns), "zero-duration views", dur0)
print("ERRORS:", dict(err) or "none")
