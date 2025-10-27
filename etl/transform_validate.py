import pandas as pd
from pathlib import Path

BASE = Path(__file__).resolve().parents[1] / "data"
CURATED = BASE / "curated"
CURATED.mkdir(exist_ok=True, parents=True)

def assert_no_nulls(df, cols, name):
    null_counts = df[cols].isnull().sum()
    if null_counts.any():
        raise AssertionError(f"[DQ] Nulls in {name}: {null_counts.to_dict()}")

def assert_positive(df, cols, name):
    for c in cols:
        if (df[c] < 0).any():
            bad = df[df[c] < 0].head(5).to_dict(orient="records")
            raise AssertionError(f"[DQ] Negative values in {name}.{c}: sample={bad}")

def main():
    suppliers = pd.read_csv(BASE / "suppliers.csv")
    contracts = pd.read_csv(BASE / "contracts.csv", parse_dates=["start_date", "end_date"])
    orders    = pd.read_csv(BASE / "orders.csv", parse_dates=["order_date"])
    shipments = pd.read_csv(BASE / "shipments.csv", parse_dates=["shipped_date", "delivered_date"])
    inventory = pd.read_csv(BASE / "inventory.csv", parse_dates=["date"])

    assert_no_nulls(suppliers, ["supplier_id", "supplier_name"], "suppliers")
    assert_positive(suppliers, ["lead_time_days", "quality_score"], "suppliers")
    assert_positive(orders, ["qty_ordered", "unit_price"], "orders")
    assert_positive(shipments, ["qty_delivered"], "shipments")

    missing_suppliers = set(orders["supplier_id"]) - set(suppliers["supplier_id"])
    if missing_suppliers:
        raise AssertionError(f"[DQ] Missing supplier dimension for orders: {missing_suppliers}")

    suppliers.to_parquet(CURATED / "dim_suppliers.parquet", index=False)
    contracts.to_parquet(CURATED / "stg_contracts.parquet", index=False)
    orders.to_parquet(CURATED / "stg_orders.parquet", index=False)
    shipments.to_parquet(CURATED / "stg_shipments.parquet", index=False)
    inventory.to_parquet(CURATED / "stg_inventory.parquet", index=False)

    print("[ETL] Curated tables written to", CURATED)

if __name__ == "__main__":
    main()
