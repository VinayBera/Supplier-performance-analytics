import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import random

random.seed(42)
np.random.seed(42)

BASE = Path(__file__).resolve().parents[1] / "data"
BASE.mkdir(parents=True, exist_ok=True)

def random_date(start, end):
    delta = end - start
    int_delta = delta.days
    day = random.randrange(int_delta + 1)
    return start + timedelta(days=day)

def main():
    suppliers = []
    for sid in range(1, 21):
        suppliers.append({
            "supplier_id": sid,
            "supplier_name": f"Supplier {sid:03d}",
            "country": random.choice(["US", "MX", "CA", "CN", "IN", "VN", "DE"]),
            "lead_time_days": random.randint(5, 30),
            "quality_score": round(np.clip(np.random.normal(0.9, 0.05), 0.6, 0.99), 2),
        })
    pd.DataFrame(suppliers).to_csv(BASE / "suppliers.csv", index=False)

    start_base = datetime(2024, 1, 1)
    end_base = datetime(2025, 10, 1)
    contracts = []
    for cid in range(1, 31):
        s_id = random.randint(1, 20)
        c_start = random_date(start_base, datetime(2025, 1, 1))
        c_end = c_start + timedelta(days=random.randint(120, 540))
        committed_value = random.randint(20000, 250000)
        contracts.append({
            "contract_id": cid,
            "supplier_id": s_id,
            "start_date": c_start.date().isoformat(),
            "end_date": c_end.date().isoformat(),
            "committed_value": committed_value,
            "currency": "USD"
        })
    pd.DataFrame(contracts).to_csv(BASE / "contracts.csv", index=False)

    orders = []
    for oid in range(1, 501):
        s_id = random.randint(1, 20)
        order_date = random_date(start_base, end_base)
        qty = random.randint(10, 1000)
        unit_price = round(random.uniform(5, 50), 2)
        possible_contracts = [c["contract_id"] for c in contracts if c["supplier_id"] == s_id]
        contract_id = random.choice(possible_contracts + [None, None])
        orders.append({
            "order_id": oid,
            "supplier_id": s_id,
            "order_date": order_date.date().isoformat(),
            "qty_ordered": qty,
            "unit_price": unit_price,
            "contract_id": contract_id
        })
    pd.DataFrame(orders).to_csv(BASE / "orders.csv", index=False)

    shipments = []
    for oid in range(1, 501):
        num_parts = random.choice([1,1,1,2,2,3])
        ordered_row = [o for o in orders if o["order_id"] == oid][0]
        s_id = ordered_row["supplier_id"]
        base_ship = datetime.fromisoformat(ordered_row["order_date"]) + timedelta(days=random.randint(ordered_row["supplier_id"] % 10 + 5, 40))
        remaining = ordered_row["qty_ordered"]
        quality = [s for s in suppliers if s["supplier_id"] == s_id][0]["quality_score"]
        for part in range(num_parts):
            ship_qty = remaining if part == num_parts - 1 else random.randint(1, max(1, remaining - (num_parts - part - 1)))
            remaining -= ship_qty
            shipped_date = base_ship + timedelta(days=part*random.randint(1,5))
            delivered_date = shipped_date + timedelta(days=random.randint(0, 5))
            defects = int(np.random.binomial(ship_qty, 1 - quality))
            shipments.append({
                "shipment_id": f"{oid}-{part+1}",
                "order_id": oid,
                "supplier_id": s_id,
                "shipped_date": shipped_date.date().isoformat(),
                "delivered_date": delivered_date.date().isoformat(),
                "qty_delivered": ship_qty,
                "defect_units": defects
            })
    pd.DataFrame(shipments).to_csv(BASE / "shipments.csv", index=False)

    inv_rows = []
    for day_offset in range((end_base - start_base).days + 1):
        date = (start_base + timedelta(days=day_offset)).date().isoformat()
        for sku in range(1, 11):
            on_hand = max(0, int(np.random.normal(500, 150)))
            backorder = max(0, int(np.random.normal(30, 20)))
            inv_rows.append({
                "date": date, "sku": f"SKU-{sku:03d}", "on_hand": on_hand, "backorder": backorder
            })
    pd.DataFrame(inv_rows).to_csv(BASE / "inventory.csv", index=False)

if __name__ == "__main__":
    main()
