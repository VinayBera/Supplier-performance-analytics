import duckdb, pandas as pd, numpy as np
from pathlib import Path
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, classification_report
import pickle

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
FEATURE_DIR = DATA / "feature_store"
MODEL_DIR = DATA / "models"
FEATURE_DIR.mkdir(parents=True, exist_ok=True)
MODEL_DIR.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(str(DATA / "opspulse.duckdb"))
con.execute("CREATE SCHEMA IF NOT EXISTS analytics;")
con.execute("SET schema='analytics';")

features = con.execute("""
WITH delivered AS (
  SELECT supplier_id,
         CAST(delivered_date AS DATE) as d,
         SUM(qty_delivered) as qty_delivered,
         SUM(defect_units) as defect_units,
         AVG(
           CASE 
             WHEN CAST(delivered_date AS DATE) 
                  <= CAST(shipped_date AS DATE) + CAST(lead_time_days AS INTEGER)
             THEN 1.0 ELSE 0.0 
           END
         ) AS on_time
  FROM analytics.fact_shipment
  JOIN analytics.dim_supplier USING (supplier_id)
  GROUP BY 1,2
),
ordered AS (
  SELECT supplier_id,
         CAST(order_date AS DATE) as d,
         SUM(qty_ordered) as qty_ordered,
         SUM(order_value) as order_value
  FROM analytics.fact_order
  GROUP BY 1,2
),
agg AS (
  SELECT coalesce(o.supplier_id, d.supplier_id) as supplier_id,
         coalesce(o.d, d.d) as date,
         coalesce(qty_ordered, 0) as qty_ordered,
         coalesce(order_value, 0) as order_value,
         coalesce(qty_delivered, 0) as qty_delivered,
         coalesce(defect_units, 0) as defect_units,
         coalesce(on_time, 0) as on_time
  FROM ordered o
  FULL OUTER JOIN delivered d
  ON o.supplier_id = d.supplier_id AND o.d = d.d
)
SELECT * FROM agg ORDER BY supplier_id, date
""").df()

features["date"] = pd.to_datetime(features["date"])
features["target_late"] = 0

for sid in features["supplier_id"].unique():
    s = features[features["supplier_id"] == sid].sort_values("date").copy()
    for idx in range(len(s)):
        window = s.iloc[idx+1:idx+8]["on_time"]
        if len(window) >= 3 and window.mean() < 0.8:
            features.loc[s.index[idx], "target_late"] = 1

features["defect_rate"] = features["defect_units"] / (features["qty_delivered"].replace(0, np.nan))
features["defect_rate"] = features["defect_rate"].fillna(0.0)
features["fill_rate"] = features["qty_delivered"] / (features["qty_ordered"].replace(0, np.nan))
features["fill_rate"] = features["fill_rate"].fillna(0.0)

features = features.sort_values(["supplier_id","date"])
features["roll_on_time_7"] = features.groupby("supplier_id")["on_time"].transform(lambda x: x.rolling(7, min_periods=1).mean())
features["roll_defect_7"] = features.groupby("supplier_id")["defect_rate"].transform(lambda x: x.rolling(7, min_periods=1).mean())
features["roll_fill_7"] = features.groupby("supplier_id")["fill_rate"].transform(lambda x: x.rolling(7, min_periods=1).mean())

FEATURE_DIR.mkdir(exist_ok=True)
features.to_parquet(FEATURE_DIR / "supplier_daily_features.parquet", index=False)

cut = features["date"].quantile(0.8)
train = features[features["date"] <= cut]
test  = features[features["date"] >  cut]

X_cols = ["roll_on_time_7", "roll_defect_7", "roll_fill_7", "qty_ordered", "qty_delivered", "order_value"]
X_train, y_train = train[X_cols], train["target_late"]
X_test,  y_test  = test[X_cols],  test["target_late"]

if y_train.nunique() < 2:
    y_train = (train["roll_on_time_7"] < train["roll_on_time_7"].median()).astype(int)
if y_test.nunique() < 2:
    y_test = (test["roll_on_time_7"] < test["roll_on_time_7"].median()).astype(int)

clf = LogisticRegression(max_iter=1000)
clf.fit(X_train, y_train)
proba = clf.predict_proba(X_test)[:,1]
auc = roc_auc_score(y_test, proba)
print(f"[ML] Late-shipment risk AUC: {auc:.3f}")
print("[ML] Classification report (threshold=0.5):")
print(classification_report(y_test, (proba >= 0.5).astype(int)))

with open(MODEL_DIR / "late_shipment_model.pkl", "wb") as f:
    pickle.dump(clf, f)
print("[ML] Model saved to", MODEL_DIR / "late_shipment_model.pkl")
