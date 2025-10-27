CREATE OR REPLACE VIEW analytics.v_kpi_supplier_ontime AS
SELECT
  s.supplier_id,
  ds.supplier_name,
  strftime(fsh.delivered_date, '%Y-%m') AS year_month,
  AVG(
    CASE 
      WHEN CAST(fsh.delivered_date AS DATE) 
           <= CAST(fsh.shipped_date AS DATE) + CAST(ds.lead_time_days AS INTEGER)
      THEN 1.0 ELSE 0.0 
    END
  ) AS on_time_rate
FROM analytics.fact_shipment fsh
JOIN analytics.dim_supplier ds USING (supplier_id)
JOIN analytics.fact_order fo ON fo.order_id = fsh.order_id
JOIN analytics.dim_date dd ON dd.date = fo.order_date
JOIN analytics.dim_supplier s USING (supplier_id)
GROUP BY 1,2,3;

CREATE OR REPLACE VIEW analytics.v_kpi_supplier_fillrate AS
WITH ord AS (
  SELECT supplier_id, strftime(order_date, '%Y-%m') AS ym, SUM(qty_ordered) AS qty_ordered
  FROM analytics.fact_order GROUP BY 1,2
),
ship AS (
  SELECT supplier_id, strftime(delivered_date, '%Y-%m') AS ym, SUM(qty_delivered) AS qty_delivered
  FROM analytics.fact_shipment GROUP BY 1,2
)
SELECT
  o.supplier_id, ds.supplier_name, o.ym AS year_month,
  COALESCE(s.qty_delivered, 0)::DOUBLE / NULLIF(o.qty_ordered, 0) AS fill_rate
FROM ord o
LEFT JOIN ship s ON s.supplier_id = o.supplier_id AND s.ym = o.ym
JOIN analytics.dim_supplier ds ON ds.supplier_id = o.supplier_id;

CREATE OR REPLACE VIEW analytics.v_kpi_contract_utilization AS
WITH spend AS (
  SELECT contract_id, SUM(order_value) AS actual_spend
  FROM analytics.fact_order WHERE contract_id IS NOT NULL GROUP BY 1
)
SELECT
  dc.contract_id, dc.supplier_id, ds.supplier_name,
  dc.committed_value, COALESCE(sp.actual_spend, 0) AS actual_spend,
  COALESCE(sp.actual_spend, 0)::DOUBLE / NULLIF(dc.committed_value, 0) AS utilization_ratio
FROM analytics.dim_contract dc
LEFT JOIN spend sp USING (contract_id)
JOIN analytics.dim_supplier ds USING (supplier_id);

CREATE OR REPLACE VIEW analytics.v_kpi_spend_leakage AS
WITH order_with_contract AS (
  SELECT
    fo.*, dc.start_date, dc.end_date,
    CASE WHEN fo.contract_id IS NOT NULL
         AND fo.order_date BETWEEN dc.start_date AND dc.end_date THEN 1 ELSE 0 END AS in_active_contract
  FROM analytics.fact_order fo
  LEFT JOIN analytics.dim_contract dc USING (contract_id)
)
SELECT
  strftime(order_date, '%Y-%m') AS year_month,
  SUM(CASE WHEN in_active_contract=1 THEN order_value ELSE 0 END) AS spend_in_contract,
  SUM(CASE WHEN in_active_contract=0 THEN order_value ELSE 0 END) AS spend_outside_contract,
  SUM(order_value) AS total_spend,
  SUM(CASE WHEN in_active_contract=0 THEN order_value ELSE 0 END)::DOUBLE / NULLIF(SUM(order_value),0) AS leakage_rate
FROM order_with_contract
GROUP BY 1
ORDER BY 1;
