-- ============================================================
-- TRY FUNNEL DAILY MONITORING QUERY
-- ============================================================
-- Sources:
--   Spreedly: maelys-data.spreedly.transactions_s
--   PayPal:   cdc.PaymentTransactions_v (EcType LIKE 'PayPal%')
-- Output: 4 rows comparing Previous Month | MTD | 7d | Yesterday
-- Stages:
--   Spreedly AUTH (AO+AM), CAPTURE_SHIPPING
--   PayPal   Authorize (type 7), Receipt < $10 (type 0, no fraud split)
-- Timezone: Asia/Jerusalem (change in params CTE if needed)
--
-- Methodology (per payment-analysis + try-payments-performance skills):
--   - Order-level success via MAX CASE (equivalent to any())
--   - OrderID suffix stripped to numeric for Spreedly deduplication
--   - Apple Pay $0 Auth excluded from Spreedly AUTH population
--   - Spreedly shipping failures split: fraud-blocked vs. payment-failed
--   - PayPal TRY orders identified via cdc.OrdersNew_v (OrderType=1, SitePart NOT IN (10,12))
-- ============================================================

WITH params AS (
  SELECT DATE_SUB(CURRENT_DATE('Asia/Jerusalem'), INTERVAL 1 DAY) AS yesterday
),
periods AS (
  SELECT 'P1. Previous month' AS period,
         DATE_TRUNC(DATE_SUB(DATE_TRUNC(yesterday, MONTH), INTERVAL 1 DAY), MONTH) AS d_start,
         DATE_SUB(DATE_TRUNC(yesterday, MONTH), INTERVAL 1 DAY) AS d_end,
         1 AS sort_order
  FROM params
  UNION ALL
  SELECT 'P2. MTD (excl yesterday)',
         DATE_TRUNC(yesterday, MONTH),
         DATE_SUB(yesterday, INTERVAL 1 DAY),
         2
  FROM params
  UNION ALL
  SELECT 'P3. 7d ended before yesterday',
         DATE_SUB(yesterday, INTERVAL 7 DAY),
         DATE_SUB(yesterday, INTERVAL 1 DAY),
         3
  FROM params
  UNION ALL
  SELECT 'P4. Yesterday',
         yesterday,
         yesterday,
         4
  FROM params
),

-- ========== SPREEDLY ==========
raw AS (
  SELECT
    REGEXP_EXTRACT(order_id, r'(\d+)') AS order_id_numeric,
    date,
    transaction_metadata_sub_transaction_type AS sub_type,
    LOWER(succeeded) = 'true' AS is_success,
    LOWER(COALESCE(message, '')) AS msg_lower,
    payment_method_payment_method_type AS pmt_type,
    SAFE_CAST(amount AS FLOAT64) AS amt
  FROM `maelys-data.spreedly.transactions_s`
  WHERE date >= (SELECT MIN(d_start) FROM periods)
    AND date <= (SELECT MAX(d_end) FROM periods)
    AND transaction_metadata_order_type = 'TRY'
    AND transaction_metadata_sub_transaction_type IN (
      'AUTH_ORIGINAL', 'AUTH_MODIFIED', 'CAPTURE_SHIPPING'
    )
),
raw_tagged AS (
  SELECT r.*, p.period, p.sort_order
  FROM raw r
  JOIN periods p ON r.date BETWEEN p.d_start AND p.d_end
),

-- Spreedly AUTH (exclude Apple Pay $0)
auth_order_level AS (
  SELECT
    period, sort_order, order_id_numeric,
    MAX(CASE WHEN sub_type = 'AUTH_ORIGINAL' AND is_success THEN 1 ELSE 0 END) AS ao_success,
    MAX(CASE WHEN sub_type = 'AUTH_MODIFIED'  AND is_success THEN 1 ELSE 0 END) AS am_success
  FROM raw_tagged
  WHERE sub_type IN ('AUTH_ORIGINAL', 'AUTH_MODIFIED')
    AND NOT (pmt_type = 'apple_pay' AND COALESCE(amt, 0) = 0)
    AND order_id_numeric IS NOT NULL
  GROUP BY period, sort_order, order_id_numeric
),
auth_agg AS (
  SELECT
    period, sort_order,
    COUNT(*) AS auth_orders,
    SUM(ao_success) AS ao_success_orders,
    SUM(CASE WHEN ao_success = 1 OR am_success = 1 THEN 1 ELSE 0 END) AS combined_pass_orders
  FROM auth_order_level
  GROUP BY period, sort_order
),

-- Spreedly CAPTURE_SHIPPING
cs_order_level AS (
  SELECT
    period, sort_order, order_id_numeric,
    MAX(CASE WHEN is_success THEN 1 ELSE 0 END) AS succeeded,
    MAX(CASE WHEN NOT is_success AND STRPOS(msg_lower, 'fraud check') > 0 THEN 1 ELSE 0 END) AS had_fraud_fail
  FROM raw_tagged
  WHERE sub_type = 'CAPTURE_SHIPPING' AND order_id_numeric IS NOT NULL
  GROUP BY period, sort_order, order_id_numeric
),
cs_agg AS (
  SELECT
    period, sort_order,
    COUNT(*) AS shipping_orders,
    SUM(succeeded) AS shipping_success_orders,
    SUM(CASE WHEN succeeded = 0 AND had_fraud_fail = 1 THEN 1 ELSE 0 END) AS fraud_blocked,
    SUM(CASE WHEN succeeded = 0 AND had_fraud_fail = 0 THEN 1 ELSE 0 END) AS payment_fail
  FROM cs_order_level
  GROUP BY period, sort_order
),

-- ========== PAYPAL ==========
pp_raw AS (
  SELECT
    t.OrderID,
    DATE(TIMESTAMP(t.TransactionTime), 'Israel') AS date,
    t.IsSuccessful AS is_success,
    t.TransactionType,
    SAFE_CAST(t.Sum AS FLOAT64) AS amt
  FROM `cdc.PaymentTransactions_v` t
  JOIN (
    SELECT DISTINCT ID AS OrderID
    FROM `cdc.OrdersNew_v`
    WHERE OrderType = 1 AND SitePart NOT IN (10, 12)
  ) o USING(OrderID)
  WHERE DATE(TIMESTAMP(t.TransactionTime), 'Israel')
        BETWEEN (SELECT MIN(d_start) FROM periods) AND (SELECT MAX(d_end) FROM periods)
    AND STARTS_WITH(t.EcType, 'PayPal')
    AND t.TransactionType IN (7, 0)
),
pp_raw_tagged AS (
  SELECT r.*, p.period, p.sort_order
  FROM pp_raw r
  JOIN periods p ON r.date BETWEEN p.d_start AND p.d_end
),

-- PayPal AUTH (TransactionType = 7 = Authorize)
pp_auth_order_level AS (
  SELECT
    period, sort_order, OrderID,
    MAX(CASE WHEN is_success THEN 1 ELSE 0 END) AS auth_success
  FROM pp_raw_tagged
  WHERE TransactionType = 7
  GROUP BY period, sort_order, OrderID
),
pp_auth_agg AS (
  SELECT
    period, sort_order,
    COUNT(*) AS pp_auth_orders,
    SUM(auth_success) AS pp_auth_success_orders
  FROM pp_auth_order_level
  GROUP BY period, sort_order
),

-- PayPal SHIPPING (TransactionType = 0 = Receipt, amt < 10, no fraud split)
pp_ship_order_level AS (
  SELECT
    period, sort_order, OrderID,
    MAX(CASE WHEN is_success THEN 1 ELSE 0 END) AS ship_success
  FROM pp_raw_tagged
  WHERE TransactionType = 0 AND amt < 10
  GROUP BY period, sort_order, OrderID
),
pp_ship_agg AS (
  SELECT
    period, sort_order,
    COUNT(*) AS pp_ship_orders,
    SUM(ship_success) AS pp_ship_success_orders
  FROM pp_ship_order_level
  GROUP BY period, sort_order
)

-- ========== FINAL OUTPUT ==========
SELECT
  p.period                                                                          AS Period,
  -- Spreedly Auth
  a.auth_orders                                                                     AS SPL_Auth_Orders,
  ROUND(SAFE_DIVIDE(a.ao_success_orders,    a.auth_orders)    * 100, 2)             AS SPL_AO_Rate_Pct,
  ROUND(SAFE_DIVIDE(a.combined_pass_orders, a.auth_orders)    * 100, 2)             AS SPL_Combined_Auth_Rate_Pct,
  -- Spreedly Shipping
  c.shipping_orders                                                                 AS SPL_Shipping_Orders,
  ROUND(SAFE_DIVIDE(c.shipping_success_orders, c.shipping_orders) * 100, 2)         AS SPL_Shipping_Success_Pct,
  ROUND(SAFE_DIVIDE(c.fraud_blocked,           c.shipping_orders) * 100, 2)         AS SPL_Shipping_Fraud_Fail_Pct,
  ROUND(SAFE_DIVIDE(c.payment_fail,            c.shipping_orders) * 100, 2)         AS SPL_Shipping_Payment_Fail_Pct,
  -- PayPal Auth
  pa.pp_auth_orders                                                                 AS PP_Auth_Orders,
  ROUND(SAFE_DIVIDE(pa.pp_auth_success_orders, pa.pp_auth_orders) * 100, 2)         AS PP_Auth_Rate_Pct,
  -- PayPal Shipping
  ps.pp_ship_orders                                                                 AS PP_Shipping_Orders,
  ROUND(SAFE_DIVIDE(ps.pp_ship_success_orders, ps.pp_ship_orders) * 100, 2)         AS PP_Shipping_Success_Pct
FROM periods p
LEFT JOIN auth_agg    a  USING (period, sort_order)
LEFT JOIN cs_agg      c  USING (period, sort_order)
LEFT JOIN pp_auth_agg pa USING (period, sort_order)
LEFT JOIN pp_ship_agg ps USING (period, sort_order)
ORDER BY p.sort_order DESC;
