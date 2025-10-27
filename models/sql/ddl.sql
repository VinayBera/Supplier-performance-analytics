CREATE SCHEMA IF NOT EXISTS analytics;

CREATE OR REPLACE VIEW analytics.stg_suppliers AS
  SELECT * FROM read_parquet('data/curated/dim_suppliers.parquet');

CREATE OR REPLACE VIEW analytics.stg_contracts AS
  SELECT * FROM read_parquet('data/curated/stg_contracts.parquet');

CREATE OR REPLACE VIEW analytics.stg_orders AS
  SELECT * FROM read_parquet('data/curated/stg_orders.parquet');

CREATE OR REPLACE VIEW analytics.stg_shipments AS
  SELECT * FROM read_parquet('data/curated/stg_shipments.parquet');

CREATE OR REPLACE VIEW analytics.stg_inventory AS
  SELECT * FROM read_parquet('data/curated/stg_inventory.parquet');

CREATE OR REPLACE TABLE analytics.dim_supplier AS
SELECT DISTINCT supplier_id, supplier_name, country, lead_time_days, quality_score
FROM analytics.stg_suppliers;

CREATE OR REPLACE TABLE analytics.dim_contract AS
SELECT DISTINCT contract_id, supplier_id, start_date, end_date, committed_value, currency
FROM analytics.stg_contracts;

CREATE OR REPLACE TABLE analytics.dim_date AS
WITH dates AS (
  SELECT MIN(order_date) AS min_d, MAX(order_date) AS max_d FROM analytics.stg_orders
)
SELECT d::DATE AS date,
       EXTRACT(year FROM d) AS year,
       EXTRACT(month FROM d) AS month,
       strftime(d, '%Y-%m') AS year_month
FROM dates, range(dates.min_d::DATE, dates.max_d::DATE + INTERVAL 1 DAY, INTERVAL 1 DAY) t(d);

CREATE OR REPLACE TABLE analytics.fact_order AS
SELECT o.order_id, o.supplier_id, o.order_date,
       o.qty_ordered, o.unit_price, o.contract_id,
       (o.qty_ordered * o.unit_price) AS order_value
FROM analytics.stg_orders o;

CREATE OR REPLACE TABLE analytics.fact_shipment AS
SELECT s.shipment_id, s.order_id, s.supplier_id,
       s.shipped_date, s.delivered_date, s.qty_delivered, s.defect_units
FROM analytics.stg_shipments s;

CREATE OR REPLACE TABLE analytics.fact_inventory AS
SELECT i.date, i.sku, i.on_hand, i.backorder
FROM analytics.stg_inventory i;
