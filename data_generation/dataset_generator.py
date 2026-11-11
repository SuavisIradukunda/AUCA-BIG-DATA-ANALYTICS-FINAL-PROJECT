# dataset_generator.py
"""
AUCA Big Data Analytics Final Project - E-commerce dataset generator (corrected).

Generates users, categories, products, sessions and transactions that follow the
schema and relationships described in the project brief:

  * User -> Sessions -> Transactions      (a session yields zero or one transaction)
  * Product -> Category -> Subcategory    (every product has category_id AND subcategory_id)
  * Session -> viewed products -> cart -> transaction line items
  * Inventory chain: stock only decreases through transactions, a product can only be
    sold while active and in stock, and a product whose stock reaches 0 is marked inactive.

Corrections made to the original script
---------------------------------------
 1. Products now carry `subcategory_id` (required by the brief, missing before).
 2. Sessions are generated in chronological order, so stock is consumed in time order
    and "sold out -> inactive" happens at a consistent point in time.
 3. Items can only be added to a cart / sold if the product is active and in stock
    (the old fallback let inactive products into carts and transactions).
 4. Products whose stock reaches 0 are flagged `is_active = False`.
 5. `conversion_status` is now consistent with the transactions file:
      converted  -> exactly one transaction with this session_id exists
      abandoned  -> non-empty cart but no transaction
      browsed    -> empty cart
    (before, sessions with a 0-quantity cart entry were "abandoned", and "converted"
    sessions could have no transaction when the stock update failed).
 6. Prices in carts / transactions use the price in effect at that moment
    (from price_history), not today's price.
 7. `viewed_products` keeps chronological order (a set lost the order).
 8. Page-view time slots are unique, so no page view has a 0-second duration.
 9. No timestamps in the future: sessions end before the reference date.
10. `users.last_active` is the end of the user's latest session or transaction
    (it was a random date unrelated to the activity data).
11. User geo data is internally consistent (US city/state with country "US");
    an `age` field is added so the "users by age group" analyses in the brief are possible.
12. Journeys are consistent with the outcome: a "confirmation" page only appears in
    converted sessions and "checkout" only with a non-empty cart.
13. Monetary sums are rounded to 2 decimals.
14. Sessions and transactions are streamed to disk instead of kept in memory,
    so 2,000,000 sessions no longer exhaust RAM.
15. IDs come from the seeded RNG, so runs are reproducible (uuid4 ignored the seed),
    and are guaranteed unique (10-hex-char session ids collided at 2M sessions).
    Use --reference-date to reproduce a run exactly.
16. Removed unused imports (threading, numpy); only Faker is required.

Usage
-----
    pip install faker
    python dataset_generator.py                       # full dataset into ./data
    python dataset_generator.py --scale 0.01          # 1% sample for quick tests
    python dataset_generator.py --format jsonl        # one JSON object per line (best for Spark)
"""

import argparse
import bisect
import datetime
import json
import os
import random
import sys

from faker import Faker

# --- Configuration -----------------------------------------------------------
NUM_USERS = 10000
NUM_PRODUCTS = 5000
NUM_CATEGORIES = 25
NUM_TRANSACTIONS = 500000
NUM_SESSIONS = 2000000
TIMESPAN_DAYS = 90
CHUNK_SIZE = 100000          # sessions per sessions_N.json file
SEED = 42

PAGE_TYPES = ["home", "search", "category_listing", "product_detail",
              "cart", "checkout", "confirmation"]

# Page flow (Markov chain): previous page -> (next pages, weights)
PAGE_TRANSITIONS = {
    "home":             (["category_listing", "search", "product_detail"], [0.5, 0.3, 0.2]),
    "category_listing": (["product_detail", "category_listing", "search", "home"], [0.7, 0.1, 0.1, 0.1]),
    "search":           (["product_detail", "search", "category_listing", "home"], [0.6, 0.2, 0.1, 0.1]),
    "product_detail":   (["product_detail", "cart", "category_listing", "search", "home"], [0.3, 0.3, 0.2, 0.1, 0.1]),
    "cart":             (["checkout", "product_detail", "category_listing", "home"], [0.6, 0.2, 0.1, 0.1]),
    "checkout":         (["confirmation", "cart", "home"], [0.8, 0.1, 0.1]),
    "confirmation":     (["home", "product_detail", "category_listing"], [0.6, 0.2, 0.2]),
}

ADD_TO_CART_PROB = 0.3       # chance a product_detail view adds the product to the cart
CHECKOUT_COMPLETE_PROB = 0.7 # chance a session that reached checkout with a cart buys
DISCOUNT_PROB = 0.2
DISCOUNT_RATES = [0.05, 0.1, 0.15, 0.2]


# --- Helpers -----------------------------------------------------------------
def iso(dt):
    """ISO-8601 timestamp without microseconds, e.g. 2025-03-12T14:37:22."""
    return dt.replace(microsecond=0).isoformat()


_issued_ids = set()


def random_hex(n):
    """Reproducible, collision-free hex id (uses the seeded global RNG).

    With 2M sessions, 10 random hex chars would produce duplicate session_ids
    (birthday paradox), so previously issued ids are tracked and redrawn.
    """
    while True:
        value = f"{random.getrandbits(4 * n):0{n}x}"
        if (n, value) not in _issued_ids:
            _issued_ids.add((n, value))
            return value


def random_datetime(start, end):
    """Uniform random datetime between start and end."""
    span = int((end - start).total_seconds())
    return start + datetime.timedelta(seconds=random.randint(0, max(span, 0)))


def apply_discount(subtotal):
    """Return (discount, total) with an optional random discount."""
    discount = 0.0
    if random.random() < DISCOUNT_PROB:
        discount = round(subtotal * random.choice(DISCOUNT_RATES), 2)
    return discount, round(subtotal - discount, 2)


class StreamingJsonWriter:
    """Writes a list of records to disk one at a time.

    format="json"  -> a valid JSON array with one record per line (matches the brief's *.json files)
    format="jsonl" -> JSON Lines, one record per line (splittable, ideal for spark.read.json)
    """

    def __init__(self, path, fmt):
        self.fmt = fmt
        self.f = open(path, "w", encoding="utf-8")
        self.count = 0
        if fmt == "json":
            self.f.write("[\n")

    def write(self, record):
        line = json.dumps(record, ensure_ascii=False)
        if self.fmt == "json" and self.count:
            self.f.write(",\n")
        self.f.write(line)
        if self.fmt == "jsonl":
            self.f.write("\n")
        self.count += 1

    def close(self):
        if self.fmt == "json":
            self.f.write("\n]\n")
        self.f.close()


def dump_all(path, records, fmt):
    writer = StreamingJsonWriter(path, fmt)
    for r in records:
        writer.write(r)
    writer.close()


# --- Inventory Management ----------------------------------------------------
class InventoryManager:
    """Single source of truth for stock and prices.

    Stock only changes through `reserve()`; a product that sells out is deactivated.
    """

    def __init__(self, products):
        self.products = {p["product_id"]: p for p in products}
        # Pre-parsed price history for fast "price at time t" lookups
        self._price_dates = {}
        self._price_values = {}
        for p in products:
            self._price_dates[p["product_id"]] = [
                datetime.datetime.fromisoformat(h["date"]) for h in p["price_history"]]
            self._price_values[p["product_id"]] = [h["price"] for h in p["price_history"]]

    def get_product(self, product_id):
        return self.products.get(product_id)

    def is_available(self, product_id, quantity=1):
        p = self.products.get(product_id)
        return bool(p) and p["is_active"] and p["current_stock"] >= quantity

    def price_at(self, product_id, when):
        """Price in effect at `when` (latest price_history entry dated on/before `when`)."""
        dates = self._price_dates[product_id]
        idx = bisect.bisect_right(dates, when) - 1
        return self._price_values[product_id][max(idx, 0)]

    def reserve(self, items):
        """Atomically decrement stock for all {product_id: qty}; all-or-nothing."""
        if not all(self.is_available(pid, qty) for pid, qty in items.items()):
            return False
        for pid, qty in items.items():
            p = self.products[pid]
            p["current_stock"] -= qty
            if p["current_stock"] == 0:
                p["is_active"] = False          # sold out -> inactive
        return True


# --- Page flow ---------------------------------------------------------------
def determine_page_type(position, previous_pages):
    """Next page type based on the position in the journey and the previous page."""
    if position == 0 or not previous_pages:
        return random.choice(["home", "search", "category_listing"])
    prev_page = previous_pages[-1]["page_type"]
    pages, weights = PAGE_TRANSITIONS.get(prev_page, (["home"], [1.0]))
    return random.choices(pages, weights=weights)[0]


def get_page_content(page_type, products_list, categories_by_id, inventory):
    """Return (product, category) shown on the page.

    product_detail pages prefer active, in-stock products; if none is found after
    a few attempts an out-of-stock/inactive product is shown (it can be viewed but
    not added to the cart).
    """
    if page_type == "product_detail":
        product = None
        for _ in range(10):
            candidate = random.choice(products_list)
            if inventory.is_available(candidate["product_id"]):
                product = candidate
                break
        if product is None:
            product = random.choice(products_list)
        return product, categories_by_id[product["category_id"]]

    if page_type == "category_listing":
        return None, random.choice(list(categories_by_id.values()))

    return None, None


# --- Entity generation -------------------------------------------------------
def generate_categories(fake, n):
    categories = []
    for cat_id in range(n):
        category = {
            "category_id": f"cat_{cat_id:03d}",
            "name": fake.company(),
            "subcategories": [],
        }
        for sub_id in range(random.randint(3, 5)):
            category["subcategories"].append({
                "subcategory_id": f"sub_{cat_id:03d}_{sub_id:02d}",
                "name": fake.bs().title(),
                "profit_margin": round(random.uniform(0.1, 0.4), 2),
            })
        categories.append(category)
    return categories


def generate_products(fake, n, categories, ref_date):
    products = []
    creation_start = ref_date - datetime.timedelta(days=TIMESPAN_DAYS * 2)
    creation_end = creation_start + datetime.timedelta(days=TIMESPAN_DAYS // 3)

    for prod_id in range(n):
        category = random.choice(categories)
        subcategory = random.choice(category["subcategories"])

        # Price history with 1-3 price points (+/-20% around the base price)
        base_price = round(random.uniform(5, 500), 2)
        last_date = random_datetime(creation_start, creation_end)
        price_history = [{"price": base_price, "date": iso(last_date)}]
        for _ in range(random.randint(0, 2)):
            last_date = random_datetime(last_date, ref_date)
            price_history.append({
                "price": round(base_price * random.uniform(0.8, 1.2), 2),
                "date": iso(last_date),
            })
        price_history.sort(key=lambda x: x["date"])

        products.append({
            "product_id": f"prod_{prod_id:05d}",
            "name": fake.catch_phrase().title(),
            "category_id": category["category_id"],
            "subcategory_id": subcategory["subcategory_id"],
            "base_price": price_history[-1]["price"],   # current price
            "current_stock": random.randint(10, 1000),
            "is_active": random.random() < 0.95,         # ~5% discontinued by the merchant
            "price_history": price_history,
            "creation_date": price_history[0]["date"],
        })
    return products


def generate_users(fake, n, ref_date):
    users = []
    reg_start = ref_date - datetime.timedelta(days=TIMESPAN_DAYS * 3)
    reg_end = ref_date - datetime.timedelta(days=TIMESPAN_DAYS)
    for user_id in range(n):
        reg_date = random_datetime(reg_start, reg_end)
        users.append({
            "user_id": f"user_{user_id:06d}",
            "age": random.choices(
                [random.randint(18, 24), random.randint(25, 34), random.randint(35, 44),
                 random.randint(45, 54), random.randint(55, 75)],
                weights=[0.20, 0.32, 0.24, 0.14, 0.10])[0],
            "geo_data": {
                "city": fake.city(),
                "state": fake.state_abbr(include_territories=False),
                "country": "US",
            },
            "registration_date": iso(reg_date),
            "last_active": iso(reg_date),       # updated from activity below
        })
    return users


# --- Session generation --------------------------------------------------------
def build_session(fake, user, session_start, products, categories_by_id, inventory):
    """Generate one session's journey and cart. Returns (session_dict, cart, end_time)."""
    session_duration = random.randint(30, 3600)   # 30 sec to 1 hour
    session_end = session_start + datetime.timedelta(seconds=session_duration)

    # 4-16 page views at unique offsets -> every view_duration >= 1 second
    n_views = min(random.randint(4, 16), session_duration)
    offsets = sorted(random.sample(range(1, session_duration), n_views - 1))
    time_slots = [0] + offsets + [session_duration]

    page_views, viewed_products, cart = [], [], {}
    for i in range(len(time_slots) - 1):
        view_time = session_start + datetime.timedelta(seconds=time_slots[i])
        page_type = determine_page_type(i, page_views)
        product, category = get_page_content(page_type, products, categories_by_id, inventory)

        if page_type == "product_detail" and product:
            pid = product["product_id"]
            if pid not in viewed_products:
                viewed_products.append(pid)           # chronological, de-duplicated
            # Only viewed, active, in-stock products can be added to the cart
            if random.random() < ADD_TO_CART_PROB:
                in_cart = cart.get(pid, {}).get("quantity", 0)
                max_possible = min(3, product["current_stock"] - in_cart) if product["is_active"] else 0
                if max_possible > 0:
                    entry = cart.setdefault(pid, {"quantity": 0,
                                                  "price": inventory.price_at(pid, view_time)})
                    entry["quantity"] += random.randint(1, max_possible)

        page_views.append({
            "timestamp": iso(view_time),
            "page_type": page_type,
            "product_id": product["product_id"] if product else None,
            "category_id": category["category_id"] if category else None,
            "view_duration": time_slots[i + 1] - time_slots[i],
        })

    session_geo = dict(user["geo_data"], ip_address=fake.ipv4_public())
    session = {
        "session_id": f"sess_{random_hex(10)}",
        "user_id": user["user_id"],
        "start_time": iso(session_start),
        "end_time": iso(session_end),
        "duration_seconds": session_duration,
        "geo_data": session_geo,
        "device_profile": random_device(),
        "viewed_products": viewed_products,
        "page_views": page_views,
        "cart_contents": cart,
        "conversion_status": "browsed",           # finalised by the caller
        "referrer": random.choice(["direct", "email", "social", "search_engine", "affiliate"]),
    }
    return session, cart, session_end


def random_device():
    """Device type / OS / browser combinations that actually exist."""
    device = random.choices(["mobile", "desktop", "tablet"], weights=[0.55, 0.35, 0.10])[0]
    if device == "desktop":
        os_name = random.choices(["Windows", "macOS"], weights=[0.7, 0.3])[0]
    else:
        os_name = random.choice(["iOS", "Android"])
    browsers = {"iOS": ["Safari", "Chrome"], "Android": ["Chrome", "Firefox", "Edge"],
                "Windows": ["Chrome", "Edge", "Firefox"], "macOS": ["Safari", "Chrome", "Firefox"]}
    return {"type": device, "os": os_name, "browser": random.choice(browsers[os_name])}


def make_transaction(session_id, user_id, timestamp, line_items, payment_methods, status):
    items = [{
        "product_id": pid,
        "quantity": qty,
        "unit_price": price,
        "subtotal": round(qty * price, 2),
    } for pid, qty, price in line_items]
    subtotal = round(sum(i["subtotal"] for i in items), 2)
    discount, total = apply_discount(subtotal)
    return {
        "transaction_id": f"txn_{random_hex(12)}",
        "session_id": session_id,
        "user_id": user_id,
        "timestamp": iso(timestamp),
        "items": items,
        "subtotal": subtotal,
        "discount": discount,
        "total": total,
        "payment_method": random.choice(payment_methods),
        "status": status,
    }


# --- Main ----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Generate the AUCA e-commerce dataset.")
    parser.add_argument("--scale", type=float, default=1.0,
                        help="Scale sessions/transactions/users/products (e.g. 0.01 for a test run).")
    parser.add_argument("--output-dir", default="data")
    parser.add_argument("--format", choices=["json", "jsonl"], default="json",
                        help="json = JSON arrays (as in the brief), jsonl = JSON Lines (best for Spark).")
    parser.add_argument("--reference-date", default=None,
                        help="End of the 90-day window, e.g. 2026-10-31T23:59:59 (default: now).")
    # parse_known_args so the script also runs inside Jupyter/Colab
    args, _ = parser.parse_known_args()

    random.seed(SEED)
    Faker.seed(SEED)
    fake = Faker("en_US")

    ref_date = (datetime.datetime.fromisoformat(args.reference_date)
                if args.reference_date else datetime.datetime.now().replace(microsecond=0))
    n_users = max(10, int(NUM_USERS * args.scale))
    n_products = max(50, int(NUM_PRODUCTS * args.scale))
    n_sessions = max(100, int(NUM_SESSIONS * args.scale))
    n_transactions = max(25, int(NUM_TRANSACTIONS * args.scale))
    ext = "json" if args.format == "json" else "jsonl"
    os.makedirs(args.output_dir, exist_ok=True)
    out = lambda name: os.path.join(args.output_dir, f"{name}.{ext}")

    print("Initializing dataset generation...")
    categories = generate_categories(fake, NUM_CATEGORIES)
    categories_by_id = {c["category_id"]: c for c in categories}
    print(f"Generated {len(categories)} categories")

    products = generate_products(fake, n_products, categories, ref_date)
    print(f"Generated {len(products)} products")

    users = generate_users(fake, n_users, ref_date)
    users_by_id = {u["user_id"]: u for u in users}
    print(f"Generated {len(users)} users")

    inventory = InventoryManager(products)
    active_products = products  # availability is checked through the inventory manager

    # Session start times, sorted -> stock is consumed in chronological order.
    # Sessions start no later than 1 hour before ref_date so none ends in the future.
    window_start = ref_date - datetime.timedelta(days=TIMESPAN_DAYS)
    window_secs = int((ref_date - window_start).total_seconds()) - 3600
    start_offsets = sorted(random.randint(0, window_secs) for _ in range(n_sessions))

    print("Generating sessions and transactions...")
    txn_writer = StreamingJsonWriter(out("transactions"), args.format)
    session_writer = None
    stats = {"converted": 0, "abandoned": 0, "browsed": 0, "session_txn": 0,
             "direct_txn": 0, "stock_failures": 0}
    prev_time = window_start

    for idx, offset in enumerate(start_offsets):
        if idx % CHUNK_SIZE == 0:                       # new sessions_N file
            if session_writer:
                session_writer.close()
            session_writer = StreamingJsonWriter(out(f"sessions_{idx // CHUNK_SIZE}"), args.format)

        session_start = window_start + datetime.timedelta(seconds=offset)
        user = random.choice(users)
        session, cart, session_end = build_session(
            fake, user, session_start, active_products, categories_by_id, inventory)

        # --- Conversion: needs a cart AND a checkout/confirmation page view
        txn_count = stats["session_txn"] + stats["direct_txn"]
        reached_checkout = any(p["page_type"] in ("checkout", "confirmation")
                               for p in session["page_views"])
        status = "abandoned" if cart else "browsed"
        if (cart and reached_checkout and txn_count < n_transactions
                and random.random() < CHECKOUT_COMPLETE_PROB):
            if inventory.reserve({pid: d["quantity"] for pid, d in cart.items()}):
                txn = make_transaction(
                    session["session_id"], user["user_id"], session_end,
                    [(pid, d["quantity"], d["price"]) for pid, d in cart.items()],
                    ["credit_card", "paypal", "apple_pay", "crypto"], "completed")
                txn_writer.write(txn)
                stats["session_txn"] += 1
                status = "converted"
                user["last_active"] = max(user["last_active"], txn["timestamp"])
            else:
                stats["stock_failures"] += 1           # sold out meanwhile -> abandoned
        session["conversion_status"] = status
        stats[status] += 1
        # Journey consistency: an order-confirmation page only exists in converted
        # sessions (otherwise the user left at checkout), and checkout needs a cart
        # (with an empty cart the user only sees the cart page).
        for pv in session["page_views"]:
            if pv["page_type"] == "confirmation" and status != "converted":
                pv["page_type"] = "checkout"
            if pv["page_type"] == "checkout" and not cart:
                pv["page_type"] = "cart"
        session_writer.write(session)
        user["last_active"] = max(user["last_active"], session["end_time"])

        # --- Transactions not linked to a session (e.g. app / phone orders).
        # The probability adapts so that the transaction target is reached at the END of
        # the window (not early): it budgets for the session conversions still expected,
        # using the conversion rate observed so far.
        txn_count = stats["session_txn"] + stats["direct_txn"]
        remaining_sessions = n_sessions - idx
        session_rate = stats["session_txn"] / (idx + 1)
        expected_session_txn = session_rate * (remaining_sessions - 1)
        p_direct = min(1.0, max(0.0, (n_transactions - txn_count - expected_session_txn)
                                / remaining_sessions))
        if txn_count < n_transactions and random.random() < p_direct:
            txn_time = random_datetime(prev_time, session_start)   # keeps chronological order
            buyer = random.choice(users)
            items = {}
            for product in random.sample(products, k=random.randint(1, 3)):
                qty = random.randint(1, 3)
                if inventory.is_available(product["product_id"], qty):
                    items[product["product_id"]] = qty
            if items and inventory.reserve(items):
                txn = make_transaction(
                    None, buyer["user_id"], txn_time,
                    [(pid, qty, inventory.price_at(pid, txn_time)) for pid, qty in items.items()],
                    ["credit_card", "paypal", "bank_transfer", "gift_card"],
                    random.choice(["completed", "processing", "shipped", "delivered"]))
                txn_writer.write(txn)
                stats["direct_txn"] += 1
                buyer["last_active"] = max(buyer["last_active"], txn["timestamp"])
        prev_time = session_start

        if (idx + 1) % 50000 == 0:
            print(f"Progress: {idx + 1:,}/{n_sessions:,} sessions, "
                  f"{stats['session_txn'] + stats['direct_txn']:,}/{n_transactions:,} transactions")

    session_writer.close()
    txn_writer.close()

    # --- Static entities (written after activity so stock/last_active are final)
    print("Saving datasets...")
    dump_all(out("users"), users, args.format)
    dump_all(out("products"), inventory.products.values(), args.format)
    dump_all(out("categories"), categories, args.format)

    n_txn = stats["session_txn"] + stats["direct_txn"]
    print(f"""
Dataset generation complete! Files written to: {os.path.abspath(args.output_dir)}
- Users:        {len(users):,}
- Products:     {len(products):,} ({sum(not p['is_active'] for p in products):,} inactive, {sum(p['current_stock'] == 0 for p in products):,} sold out)
- Categories:   {len(categories):,}
- Sessions:     {n_sessions:,} (converted {stats['converted']:,} / abandoned {stats['abandoned']:,} / browsed {stats['browsed']:,})
- Transactions: {n_txn:,} (target {n_transactions:,}; from sessions {stats['session_txn']:,}, direct {stats['direct_txn']:,})
- Remaining stock units: {sum(p['current_stock'] for p in products):,}
""")
    if n_txn < n_transactions:
        print("Warning: transaction target not reached (stock ran out).", file=sys.stderr)


if __name__ == "__main__":
    main()
