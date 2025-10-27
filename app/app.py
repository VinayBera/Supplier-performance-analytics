import streamlit as st
import duckdb
import pandas as pd
from matplotlib.ticker import PercentFormatter
from pathlib import Path
import matplotlib.pyplot as plt

st.set_page_config(page_title="OpsPulse — KPI Dashboard", layout="wide")
st.title("OpsPulse — KPI Dashboard")

DB = Path(__file__).resolve().parents[1] / "data" / "opspulse.duckdb"
if not DB.exists():
    st.error("DuckDB not found. Please run: `python run_pipeline.py`")
    st.stop()

con = duckdb.connect(str(DB))
con.execute("CREATE SCHEMA IF NOT EXISTS analytics;")
con.execute("SET schema='analytics';")


# =========================
# Sidebar controls
# =========================
st.sidebar.header("Filters")

# Supplier picker supports All or multi-select
suppliers = con.execute("""
    SELECT supplier_id, supplier_name
    FROM dim_supplier ORDER BY supplier_id
""").df()
suppliers["label"] = suppliers["supplier_id"].astype(str) + " — " + suppliers["supplier_name"]

supplier_labels = st.sidebar.multiselect(
    "Supplier(s)",
    options=["All"] + suppliers["label"].tolist(),
    default=["All"]
)

# Chart options
smoothing_w = st.sidebar.slider("Smoothing (months)", min_value=1, max_value=6, value=3)
ontime_target = st.sidebar.slider("On-Time Target (%)", 50, 100, 95)
fill_target = st.sidebar.slider("Fill-Rate Target (%)", 50, 100, 95)
clip_rates = st.sidebar.checkbox("Clip rates to 100%", value=True)
show_tables = st.sidebar.checkbox("Show tables under charts", value=False)

def to_sup_ids(labels):
    if "All" in labels or len(labels) == 0:
        return None
    ids = suppliers.loc[suppliers["label"].isin(labels), "supplier_id"].astype(int).tolist()
    return ids if ids else None

sup_ids = to_sup_ids(supplier_labels)

# =========================
# Helpers
# =========================
def ym_to_ts(df, col="year_month"):
    out = df.copy()
    out[col] = pd.to_datetime(out[col] + "-01")
    return out

def rate_series(df, col):
    s = df[col].astype(float)
    if clip_rates:
        s = s.clip(lower=0.0, upper=1.0)
    return s

def smooth(df, ycol):
    if smoothing_w <= 1 or len(df) < 2:
        df["smooth"] = df[ycol]
    else:
        df["smooth"] = df[ycol].rolling(smoothing_w, min_periods=1).mean()
    return df

def business_line(ax, x, y, title, ylabel="Rate", target=None):
    ax.plot(x, y, linewidth=2, label="Monthly")
    if "smooth" in y.index or isinstance(y, pd.Series) and "smooth" in y.name:
        pass  # handled in caller
    ax.set_title(title, fontsize=18, pad=10)
    ax.set_ylabel(ylabel, fontsize=12)
    ax.set_xlabel("Year-Month", fontsize=11)
    ax.yaxis.set_major_formatter(PercentFormatter(xmax=1.0))
    ax.set_ylim(0, 1.05)
    ax.grid(True, which="major", alpha=0.25)
    if target is not None:
        ax.axhline(target, linestyle="--", linewidth=2, label=f"Target {int(target*100)}%")
    ax.legend(loc="upper right", frameon=True, fancybox=True, shadow=False)

def agg_all_or_filter(df, ids, id_col="supplier_id", how="mean"):
    if ids is None:
        # aggregate across all suppliers by month
        grouped = df.groupby("year_month")[["on_time_rate","fill_rate"]].mean() if {"on_time_rate","fill_rate"}.issubset(df.columns) \
                   else df.groupby("year_month").mean(numeric_only=True)
        grouped = grouped.reset_index()
        grouped[id_col] = "All"
        cols = [id_col, "year_month"] + [c for c in grouped.columns if c not in [id_col, "year_month"]]
        return grouped[cols]
    else:
        return df[df[id_col].isin(ids)].copy()

def kpi_card(col, label, value):
    if pd.isna(value):
        col.metric(label, "—")
    else:
        col.metric(label, f"{value*100:.1f}%")

# =========================
# Load Views
# =========================
ot = con.execute("""
    SELECT supplier_id, year_month, on_time_rate
    FROM v_kpi_supplier_ontime
    ORDER BY year_month, supplier_id
""").df()

fr = con.execute("""
    SELECT supplier_id, year_month, fill_rate
    FROM v_kpi_supplier_fillrate
    ORDER BY year_month, supplier_id
""").df()

cu = con.execute("""
    SELECT contract_id, supplier_id, supplier_name, committed_value, actual_spend, utilization_ratio
    FROM v_kpi_contract_utilization
    ORDER BY contract_id
""").df()

sl = con.execute("""
    SELECT year_month, spend_in_contract, spend_outside_contract, total_spend, leakage_rate
    FROM v_kpi_spend_leakage
    ORDER BY year_month
""").df()

# =========================
# KPI Cards
# =========================
st.title("OpsPulse — KPI Dashboard")

# Latest values (use All or selected)
def latest_rate(df, col):
    if df.empty: return float("nan")
    d = df.sort_values("year_month")
    return float(d[col].iloc[-1])

ot_view = agg_all_or_filter(ot.copy(), sup_ids)
fr_view = agg_all_or_filter(fr.copy(), sup_ids)

cur_ot = latest_rate(ot_view, "on_time_rate") if "on_time_rate" in ot_view.columns else float("nan")
cur_fr = latest_rate(fr_view, "fill_rate") if "fill_rate" in fr_view.columns else float("nan")
avg_cu = cu["utilization_ratio"].mean() if not cu.empty else float("nan")
cur_sl = latest_rate(sl.copy(), "leakage_rate") if not sl.empty else float("nan")

c1,c2,c3,c4 = st.columns(4)
kpi_card(c1, "On-Time (latest)", cur_ot)
kpi_card(c2, "Fill Rate (latest)", cur_fr)
kpi_card(c3, "Contract Utilization (avg)", avg_cu)
kpi_card(c4, "Spend Leakage (latest)", cur_sl)

# =========================
# On-Time Delivery
# =========================
st.subheader("On-Time Delivery Rate (Monthly)")

if not ot_view.empty:
    dfp = ym_to_ts(ot_view, "year_month")
    dfp["rate"] = rate_series(dfp, "on_time_rate")
    dfp = dfp.sort_values(["supplier_id","year_month"])
    # plot per-supplier when multiple selected, else aggregate line
    fig, ax = plt.subplots(figsize=(9,4))
    if sup_ids is None:
        dfp = smooth(dfp, "rate")
        ax.plot(dfp["year_month"], dfp["rate"], linewidth=2, label="Monthly")
        ax.plot(dfp["year_month"], dfp["rate"].rolling(smoothing_w, min_periods=1).mean(),
                linewidth=2, linestyle="--", label=f"{smoothing_w}-mo avg")
        business_line(ax, dfp["year_month"], dfp["rate"], "On-Time Delivery Rate", target=ontime_target/100)
    else:
        for sid, grp in dfp.groupby("supplier_id"):
            grp = smooth(grp, "rate")
            ax.plot(grp["year_month"], grp["rate"], linewidth=2, label=f"Supplier {sid}")
        ax.axhline(ontime_target/100, linestyle="--", linewidth=2, label=f"Target {ontime_target}%")
        ax.set_title("On-Time Delivery Rate", fontsize=18, pad=10)
        ax.set_ylabel("Rate"); ax.set_xlabel("Year-Month")
        ax.yaxis.set_major_formatter(PercentFormatter(1.0)); ax.set_ylim(0,1.05); ax.grid(True, alpha=.25)
        ax.legend(loc="upper right", frameon=True, fancybox=True, shadow=False)
    st.pyplot(fig)
else:
    st.info("No data for selection.")

if show_tables:
    st.dataframe(ot_view, use_container_width=True)

# =========================
# Fill Rate
# =========================
st.subheader("Fill Rate (Monthly)")

fr_view = ym_to_ts(agg_all_or_filter(fr.copy(), sup_ids), "year_month")
if not fr_view.empty:
    fr_view["rate"] = rate_series(fr_view, "fill_rate")
    fr_view = fr_view.sort_values(["supplier_id","year_month"])
    fig, ax = plt.subplots(figsize=(9,4))
    if sup_ids is None:
        fr_view = smooth(fr_view, "rate")
        ax.plot(fr_view["year_month"], fr_view["rate"], linewidth=2, label="Monthly")
        ax.plot(fr_view["year_month"], fr_view["rate"].rolling(smoothing_w, min_periods=1).mean(),
                linewidth=2, linestyle="--", label=f"{smoothing_w}-mo avg")
        business_line(ax, fr_view["year_month"], fr_view["rate"], "Fill Rate", target=fill_target/100)
    else:
        for sid, grp in fr_view.groupby("supplier_id"):
            grp = smooth(grp, "rate")
            ax.plot(grp["year_month"], grp["rate"], linewidth=2, label=f"Supplier {sid}")
        ax.axhline(fill_target/100, linestyle="--", linewidth=2, label=f"Target {fill_target}%")
        ax.set_title("Fill Rate", fontsize=18, pad=10)
        ax.set_ylabel("Rate"); ax.set_xlabel("Year-Month")
        ax.yaxis.set_major_formatter(PercentFormatter(1.0)); ax.set_ylim(0,1.05); ax.grid(True, alpha=.25)
        ax.legend(loc="upper right", frameon=True, fancybox=True, shadow=False)
    st.pyplot(fig)
else:
    st.info("No data for selection.")

if show_tables:
    st.dataframe(fr_view, use_container_width=True)

# =========================
# Contract Utilization (table stays)
# =========================
st.subheader("Contract Utilization")
st.dataframe(
    cu.assign(utilization_pct=(cu["utilization_ratio"]*100).round(1).astype(str)+"%")
      [["contract_id","supplier_id","supplier_name","committed_value","actual_spend","utilization_pct"]],
    use_container_width=True
)

# =========================
# Spend Leakage
# =========================
st.subheader("Spend Leakage (Monthly)")
slp = ym_to_ts(sl.copy(), "year_month")
if not slp.empty:
    slp["rate"] = rate_series(slp, "leakage_rate")
    slp = smooth(slp, "rate")
    fig, ax = plt.subplots(figsize=(9,4))
    ax.plot(slp["year_month"], slp["rate"], linewidth=2, label="Monthly")
    ax.plot(slp["year_month"], slp["rate"].rolling(smoothing_w, min_periods=1).mean(),
            linewidth=2, linestyle="--", label=f"{smoothing_w}-mo avg")
    business_line(ax, slp["year_month"], slp["rate"], "Spend Leakage Over Time", ylabel="Leakage Rate")
    st.pyplot(fig)
else:
    st.info("No data for selection.")

# =========================
# Download buttons (unchanged)
# =========================
st.download_button("Download On-Time CSV", ot.to_csv(index=False).encode(), "on_time.csv", "text/csv")
st.download_button("Download Fill Rate CSV", fr.to_csv(index=False).encode(), "fill_rate.csv", "text/csv")
st.download_button("Download Leakage CSV", sl.to_csv(index=False).encode(), "leakage.csv", "text/csv")

