-- Creates or updates the Customer Dimension table, including RFM segmentation.
-- RFM (Recency, Frequency, Monetary) analysis is performed using window functions and NTILE.

CREATE OR REPLACE TABLE
  `{{ params.project_id }}.{{ params.dwh_dataset }}.dim_customers`
AS
WITH
  customer_base AS (
  SELECT
    CustomerID,
    MIN(DATE(InvoiceDate)) AS first_invoice_date,
    MAX(DATE(InvoiceDate)) AS last_invoice_date,
    COUNT(DISTINCT InvoiceNo) AS total_orders,
    SUM(Quantity * UnitPrice) AS total_spent
  FROM
    `{{ params.project_id }}.{{ params.staging_dataset }}.stg_transactions`
  WHERE CustomerID != 'ANONYMOUS' 
  GROUP BY
    CustomerID ),
  rfm_calc AS (
  SELECT
    CustomerID,
    last_invoice_date,
    DATE_DIFF(CURRENT_DATE(), last_invoice_date, DAY) AS recency_days,
    total_orders AS frequency,
    total_spent AS monetary
  FROM
    customer_base ),
  rfm_scores AS (
  SELECT
    CustomerID,
    last_invoice_date,
    recency_days,
    frequency,
    monetary,
    -- Recency: Reverse order, so lower days gets higher score (5 = most recent)
    NTILE(5) OVER (ORDER BY recency_days DESC) AS r_score,
    -- Frequency: Higher frequency gets higher score (5 = most frequent)
    NTILE(5) OVER (ORDER BY frequency ASC) AS f_score,
    -- Monetary: Higher monetary value gets higher score (5 = highest spending)
    NTILE(5) OVER (ORDER BY monetary ASC) AS m_score
  FROM
    rfm_calc ),
  rfm_segmentation AS (
  SELECT
    CustomerID,
    last_invoice_date,
    recency_days AS rfm_recency,
    frequency AS rfm_frequency,
    monetary AS rfm_monetary,
    CONCAT(CAST(r_score AS STRING), CAST(f_score AS STRING), CAST(m_score AS STRING)) AS rfm_score,
    CASE
      -- Champions: Bought recently, buy often and spend the most
      WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
      -- Loyal Customers: Buy regularly, responsive to promotions
      WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 3 THEN 'Loyal Customers'
      -- Potential Loyalists: Recent customers with average frequency and monetary value
      WHEN r_score >= 4 AND f_score >= 2 AND m_score >= 2 THEN 'Potential Loyalists'
      -- New Customers: High recency, low frequency and monetary
      WHEN r_score >= 4 AND f_score <= 1 AND m_score <= 1 THEN 'New Customers'
      -- Promising: Recent, but haven't spent much
      WHEN r_score >= 3 AND f_score <= 2 AND m_score <= 2 THEN 'Promising'
      -- At Risk: Spent a lot and bought often, but long time ago
      WHEN r_score <= 2 AND f_score >= 3 AND m_score >= 3 THEN 'At Risk'
      -- Can't Lose Them: Used to buy a lot, but haven't returned for a long time
      WHEN r_score <= 2 AND f_score >= 4 AND m_score >= 4 THEN "Can't Lose Them"
      -- Hibernating: Low spenders, low frequency, last bought long time ago
      WHEN r_score <= 2 AND f_score <= 2 AND m_score <= 2 THEN 'Hibernating'
    ELSE
    'Other'
  END AS rfm_segment
  FROM
    rfm_scores )
SELECT
  s.CustomerID,
  t.Country, 
  b.first_invoice_date,
  s.last_invoice_date,
  s.rfm_recency,
  s.rfm_frequency,
  s.rfm_monetary,
  s.rfm_score,
  s.rfm_segment
FROM
  rfm_segmentation AS s
JOIN
  customer_base AS b
  ON s.CustomerID = b.CustomerID
LEFT JOIN (SELECT DISTINCT CustomerID, Country FROM `{{ params.project_id }}.{{ params.staging_dataset }}.stg_transactions` WHERE CustomerID != 'ANONYMOUS') AS t
  ON s.CustomerID = t.CustomerID