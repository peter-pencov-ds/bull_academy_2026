# Databricks notebook source
# MAGIC %md
# MAGIC # Lab data generator — "Northwind Outdoors" e-commerce
# MAGIC
# MAGIC Writes realistic, **deliberately messy** raw files into the `landing` volume, one batch at a time,
# MAGIC so every ingestion lab has something to fix.
# MAGIC
# MAGIC | Batch | What lands | What it teaches |
# MAGIC | --- | --- | --- |
# MAGIC | 1 | 50 products (CSV), 500 customers (CSV), ~2,000 orders (JSON, nested) | Initial load, schema inference, nested data |
# MAGIC | 2 | 60 customer **updates** + a new `loyalty_tier` column; ~800 new orders | Schema evolution, SCD Type 2, incremental loads |
# MAGIC | 3 | ~800 orders incl. **duplicates**, **late** orders, prices as strings, null customers | Dedup, data quality, rescued data, watermarks |
# MAGIC
# MAGIC Defects are seeded with a fixed random seed, so every intern gets the same data and the same "right answers".
# MAGIC
# MAGIC Usage (after `%run ./_config`): `%run ./_data_generator` then `land_batch(1, landing)`.

# COMMAND ----------

import csv
import json
import os
import random
from datetime import datetime, timedelta

COUNTRIES = {
    "EU": ["AT", "DE", "RO", "FR", "IT"],
    "NA": ["US", "CA"],
    "APAC": ["JP", "AU", "SG"],
}
CATEGORIES = ["Tents", "Backpacks", "Footwear", "Clothing", "Cooking", "Climbing"]
STATUSES = ["placed", "paid", "shipped", "delivered", "cancelled"]
FIRST = ["Ana", "Mihai", "Lena", "Jonas", "Sara", "Tom", "Yuki", "Liam", "Ioana", "Paul", "Mia", "Noah"]
LAST = ["Popescu", "Huber", "Schmidt", "Rossi", "Tanaka", "Smith", "Ionescu", "Martin", "Brown", "Weber"]
BASE_TS = datetime(2026, 9, 1, 8, 0, 0)


def _write_csv(path, header, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def _write_jsonl(path, records):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


def _products(rng):
    rows = []
    for i in range(1, 51):
        cat = CATEGORIES[(i - 1) % len(CATEGORIES)]
        rows.append([f"P{i:03d}", f"{cat[:-1]} model {i}", cat, round(rng.uniform(9, 450), 2)])
    return rows


def _customer(rng, cid, ts, with_tier=False):
    region = rng.choice(list(COUNTRIES))
    first, last = rng.choice(FIRST), rng.choice(LAST)
    email = f"{first}.{last}{cid}@example.com".lower()
    row = [f"C{cid:05d}", f"{first} {last}", email, rng.choice(COUNTRIES[region]), region,
           (BASE_TS - timedelta(days=rng.randint(30, 900))).date().isoformat(), ts.isoformat()]
    if with_tier:
        row.append(rng.choice(["bronze", "silver", "gold"]))
    return row


def _order(rng, oid, ts, n_customers, price_lookup):
    items = []
    for _ in range(rng.randint(1, 4)):
        pid = f"P{rng.randint(1, 50):03d}"
        items.append({"product_id": pid, "qty": rng.randint(1, 3), "unit_price": price_lookup[pid]})
    region = rng.choice(list(COUNTRIES))
    return {
        "order_id": f"O{oid:07d}",
        "customer_id": f"C{rng.randint(1, n_customers):05d}",
        "order_ts": ts.isoformat(),
        "status": rng.choice(STATUSES),
        "items": items,
        "shipping": {"country": rng.choice(COUNTRIES[region]), "express": rng.random() < 0.2},
    }


def land_batch(batch: int, base_path: str, seed: int = 42) -> dict:
    """Write batch 1, 2, 3 or into base_path. Returns a dict of files written and seeded defects."""
    if batch not in (1, 2, 3, 4):
        raise ValueError("batch must be 1, 2, 3 or 4")
    rng = random.Random(seed * 100 + batch)
    products = _products(random.Random(seed))  # identical in every batch
    price = {p[0]: p[3] for p in products}
    tag = f"batch_{batch:03d}"
    written, defects = [], {}
    cust_header = ["customer_id", "name", "email", "country", "region", "signup_date", "updated_at"]

    if batch == 1:
        p = f"{base_path}/products/products.csv"
        _write_csv(p, ["product_id", "product_name", "category", "list_price"], products)
        written.append(p)

        ts = BASE_TS
        customers = [_customer(rng, i, ts) for i in range(1, 501)]
        for r in rng.sample(customers, 15):
            r[2] = ""                      # missing email
        customers += rng.sample(customers, 10)  # exact duplicate rows
        defects.update(customer_missing_email=15, customer_duplicate_rows=10)
        p = f"{base_path}/customers/customers_{tag}.csv"
        _write_csv(p, cust_header, customers)
        written.append(p)

        orders = [_order(rng, i, BASE_TS + timedelta(minutes=7 * i), 500, price) for i in range(1, 2001)]
        p = f"{base_path}/orders/orders_{tag}.json"
        _write_jsonl(p, orders)
        written.append(p)

    elif batch == 2:
        ts = BASE_TS + timedelta(days=14)
        updates = []
        for cid in rng.sample(range(1, 501), 60):     # same customers, new country/region
            updates.append(_customer(rng, cid, ts, with_tier=True))
        new = [_customer(rng, i, ts, with_tier=True) for i in range(501, 541)]
        defects.update(customer_updates=60, customer_new=40, new_column="loyalty_tier")
        p = f"{base_path}/customers/customers_{tag}.csv"
        _write_csv(p, cust_header + ["loyalty_tier"], updates + new)
        written.append(p)

        orders = [_order(rng, i, ts + timedelta(minutes=5 * (i - 2000)), 540, price) for i in range(2001, 2801)]
        p = f"{base_path}/orders/orders_{tag}.json"
        _write_jsonl(p, orders)
        written.append(p)

    elif batch == 3:
        ts = BASE_TS + timedelta(days=21)
        orders = [_order(rng, i, ts + timedelta(minutes=5 * (i - 2800)), 540, price) for i in range(2801, 3601)]
        late = rng.sample(orders, 40)
        for o in late:                      # arrive now, but happened 10+ days ago
            o["order_ts"] = (BASE_TS + timedelta(days=rng.randint(1, 8))).isoformat()
        for o in rng.sample(orders, 25):
            o["customer_id"] = None         # orphan orders
        for o in rng.sample(orders, 20):
            o["items"][0]["unit_price"] = str(o["items"][0]["unit_price"]).replace(".", ",")  # "12,50"
        dupes = [json.loads(json.dumps(o)) for o in rng.sample(orders, 30)]
        orders += dupes
        rng.shuffle(orders)
        defects.update(order_late=40, order_null_customer=25, order_price_as_string=20, order_duplicates=30)
        p = f"{base_path}/orders/orders_{tag}.json"
        _write_jsonl(p, orders)
        written.append(p)

    else:
        ts = BASE_TS + timedelta(days=28)
        orders = [
            _order(rng, i, ts + timedelta(minutes=5 * (i - 3600)), 540, price)
            for i in range(3601, 3801)
        ]
        p = f"{base_path}/orders/orders_{tag}.json"
        _write_jsonl(p, orders)
        written.append(p)

    for f in written:
        print(f"wrote {f}")
    print(f"seeded defects: {defects}")
    return {"files": written, "defects": defects}


def reset_landing(base_path: str):
    """Delete every generated file so you can start the labs over."""
    import shutil
    for sub in ("products", "customers", "orders"):
        shutil.rmtree(f"{base_path}/{sub}", ignore_errors=True)
    print(f"cleared {base_path}")