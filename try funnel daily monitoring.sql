-- ============================================================
-- TRY FUNNEL DAILY MONITORING QUERY
-- ============================================================
-- Methodology aligned with TBYB Success Rate Timeline (Redash #1610)
-- Sources:
--   cdc.PaymentTransactions_v        (all providers)
--   spreedly.transaction_report_v    (joined via OrchestratorToken for success/sub-type override)
-- Output: 4 rows — Yesterday | 7d (excl. yesterday) | MTD (excl. yesterday) | Previous month
-- Timezone: Asia/Jerusalem
--
-- Key methodology choices (matching #1610):
--   - TRY orders: QUALIFY MAX(TransactionType)=7 per order (must have an auth)
--   - sum > 0 filter: zero-amount transactions excluded
--   - Date attribution: order's first transaction date (not per-transaction date)
--   - Spreedly success/sub-type: prefer spreedly.transaction_report_v where token matches
--   - PayPal AUTH_MODIFIED: detected via window function (prior failed auth + lower amount)
--   - PayPal shipping: CAPTURE_SHIPPING (Receipt < $10) + CAPTURE_FOLLOW_UP (last Receipt > $10)
--   - Spreedly CC shipping denominator: fraud-blocked CC orders excluded
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

-- ===== UNIFIED BASE =====
trans_raw AS (
  SELECT
    pt.OrderID,
    pt.TransactionTime,
    pt.TransactionType,
    CASE
      WHEN s.Succeeded = 'True'  THEN true
      WHEN s.Succeeded = 'False' THEN false
      ELSE pt.IsSuccessful
    END AS succeeded,
    CASE WHEN IFNULL(LOWER(s.Message), '') LIKE '%fraud%' THEN true ELSE false END AS fraud_flag,
    COALESCE(
      s.Metadata_sub_transaction_type,
      CASE
        WHEN pt.TransactionType = 7               THEN 'AUTH_ORIGINAL'
        WHEN pt.TransactionType = 0 AND pt.Sum < 10 THEN 'CAPTURE_SHIPPING'
        ELSE                                           'CAPTURE_FOLLOW_UP'
      END
    ) AS sub_type,
    CASE WHEN LOWER(pt.EcType) LIKE '%paypal%'   THEN 'PayPal'      ELSE 'Spreedly'    END AS provider,
    CASE WHEN LOWER(pt.EcType) LIKE '%applepay%' THEN 'ApplePay'    ELSE 'Credit Card' END AS spl_pmt_method,
    pt.Sum AS amt
  FROM `cdc.PaymentTransactions_v` pt
  LEFT JOIN `spreedly.transaction_report_v` s ON pt.OrchestratorToken = s.token
  WHERE pt.TransactionType IN (0, 7)
    AND pt.Sum > 0
  QUALIFY MAX(pt.TransactionType) OVER (PARTITION BY pt.OrderID) = 7
),

trans AS (
  SELECT
    *,
    MIN(DATE(TIMESTAMP(TransactionTime), 'Asia/Jerusalem')) OVER (PARTITION BY OrderID) AS first_date,
    -- PayPal AUTH_MODIFIED: prior failed auth exists AND current amount is lower
    CASE
      WHEN provider = 'PayPal'
        AND TransactionType = 7
        AND sub_type = 'AUTH_ORIGINAL'
        AND MAX(CASE WHEN TransactionType = 7 AND NOT succeeded THEN 1 ELSE 0 END)
            OVER (PARTITION BY OrderID ORDER BY TIMESTAMP(TransactionTime)
                  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) = 1
        AND amt < FIRST_VALUE(CASE WHEN TransactionType = 7 THEN amt END IGNORE NULLS)
            OVER (PARTITION BY OrderID ORDER BY TIMESTAMP(TransactionTime)
                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
      THEN 'AUTH_MODIFIED'
      ELSE sub_type
    END AS sub_type_final,
    -- PayPal CAPTURE_FOLLOW_UP: last Receipt > $10 per order
    CASE
      WHEN provider = 'PayPal'
        AND TransactionType = 0
        AND amt > 10
        AND TIMESTAMP(TransactionTime) = MAX(CASE WHEN TransactionType = 0 AND amt > 10
            THEN TIMESTAMP(TransactionTime) END) OVER (PARTITION BY OrderID)
      THEN true
      ELSE false
    END AS is_pp_capture_followup
  FROM trans_raw
),

trans_tagged AS (
  SELECT t.*, p.period, p.sort_order
  FROM trans t
  JOIN periods p ON t.first_date BETWEEN p.d_start AND p.d_end
),

-- ===== SPREEDLY AUTH =====
spl_auth_orders AS (
  SELECT
    period, sort_order, OrderID,
    MAX(CASE WHEN sub_type_final = 'AUTH_ORIGINAL'                      THEN 1 ELSE 0 END) AS ao_attempt,
    MAX(CASE WHEN sub_type_final = 'AUTH_ORIGINAL' AND     succeeded    THEN 1 ELSE 0 END) AS ao_success,
    MAX(CASE WHEN sub_type_final = 'AUTH_MODIFIED'  AND     succeeded    THEN 1 ELSE 0 END) AS am_success
  FROM trans_tagged
  WHERE provider = 'Spreedly'
    AND TransactionType = 7
    AND sub_type_final IN ('AUTH_ORIGINAL', 'AUTH_MODIFIED')
  GROUP BY period, sort_order, OrderID
),
spl_auth_agg AS (
  SELECT
    period, sort_order,
    SUM(ao_attempt)                                                     AS auth_orders,
    SUM(ao_success)                                                     AS ao_success_orders,
    SUM(CASE WHEN ao_success = 1 OR am_success = 1 THEN 1 ELSE 0 END)  AS combined_pass_orders
  FROM spl_auth_orders
  GROUP BY period, sort_order
),

-- ===== SPREEDLY SHIPPING =====
-- CC fraud-blocked orders are excluded from the denominator (matching #1610)
spl_ship_orders AS (
  SELECT
    period, sort_order, OrderID,
    MAX(CASE WHEN spl_pmt_method != 'Credit Card' OR (succeeded OR NOT fraud_flag) THEN 1 ELSE 0 END) AS ship_attempt,
    MAX(CASE WHEN     succeeded                                         THEN 1 ELSE 0 END) AS ship_success,
    MAX(CASE WHEN NOT succeeded AND     fraud_flag                      THEN 1 ELSE 0 END) AS fraud_blocked
  FROM trans_tagged
  WHERE provider = 'Spreedly'
    AND sub_type_final = 'CAPTURE_SHIPPING'
  GROUP BY period, sort_order, OrderID
),
spl_ship_agg AS (
  SELECT
    period, sort_order,
    SUM(ship_attempt)                                                                               AS shipping_orders,
    SUM(CASE WHEN ship_attempt=1 AND ship_success=1                      THEN 1 ELSE 0 END)          AS shipping_success_orders,
    SUM(CASE WHEN ship_attempt=1 AND ship_success=0 AND fraud_blocked=1  THEN 1 ELSE 0 END)          AS fraud_blocked_orders,
    SUM(CASE WHEN ship_attempt=1 AND ship_success=0 AND fraud_blocked=0  THEN 1 ELSE 0 END)          AS payment_fail_orders
  FROM spl_ship_orders
  GROUP BY period, sort_order
),

-- ===== PAYPAL AUTH =====
pp_auth_orders AS (
  SELECT
    period, sort_order, OrderID,
    MAX(CASE WHEN sub_type_final IN ('AUTH_ORIGINAL','AUTH_MODIFIED')               THEN 1 ELSE 0 END) AS auth_attempt,
    MAX(CASE WHEN sub_type_final IN ('AUTH_ORIGINAL','AUTH_MODIFIED') AND succeeded  THEN 1 ELSE 0 END) AS auth_success
  FROM trans_tagged
  WHERE provider = 'PayPal'
    AND TransactionType = 7
  GROUP BY period, sort_order, OrderID
),
pp_auth_agg AS (
  SELECT period, sort_order,
    SUM(auth_attempt) AS pp_auth_orders,
    SUM(auth_success) AS pp_auth_success_orders
  FROM pp_auth_orders
  GROUP BY period, sort_order
),

-- ===== PAYPAL SHIPPING (CAPTURE_SHIPPING < $10 + CAPTURE_FOLLOW_UP: last Receipt > $10) =====
pp_ship_orders AS (
  SELECT
    period, sort_order, OrderID,
    MAX(CASE WHEN sub_type_final = 'CAPTURE_SHIPPING' OR is_pp_capture_followup               THEN 1 ELSE 0 END) AS ship_attempt,
    MAX(CASE WHEN (sub_type_final = 'CAPTURE_SHIPPING' OR is_pp_capture_followup) AND succeeded THEN 1 ELSE 0 END) AS ship_success
  FROM trans_tagged
  WHERE provider = 'PayPal'
    AND TransactionType = 0
  GROUP BY period, sort_order, OrderID
),
pp_ship_agg AS (
  SELECT period, sort_order,
    SUM(ship_attempt) AS pp_ship_orders,
    SUM(ship_success) AS pp_ship_success_orders
  FROM pp_ship_orders
  GROUP BY period, sort_order
)

-- ===== FINAL OUTPUT =====
SELECT
  p.period                                                                          AS Period,
  -- Spreedly Auth
  a.auth_orders                                                                     AS SPL_Auth_Orders,
  ROUND(SAFE_DIVIDE(a.ao_success_orders,    a.auth_orders)    * 100, 2)             AS SPL_AO_Rate_Pct,
  ROUND(SAFE_DIVIDE(a.combined_pass_orders, a.auth_orders)    * 100, 2)             AS SPL_Combined_Auth_Rate_Pct,
  -- Spreedly Shipping
  c.shipping_orders                                                                 AS SPL_Shipping_Orders,
  ROUND(SAFE_DIVIDE(c.shipping_success_orders, c.shipping_orders) * 100, 2)         AS SPL_Shipping_Success_Pct,
  ROUND(SAFE_DIVIDE(c.fraud_blocked_orders,    c.shipping_orders) * 100, 2)         AS SPL_Shipping_Fraud_Fail_Pct,
  ROUND(SAFE_DIVIDE(c.payment_fail_orders,     c.shipping_orders) * 100, 2)         AS SPL_Shipping_Payment_Fail_Pct,
  -- PayPal Auth
  pa.pp_auth_orders                                                                 AS PP_Auth_Orders,
  ROUND(SAFE_DIVIDE(pa.pp_auth_success_orders, pa.pp_auth_orders) * 100, 2)         AS PP_Auth_Rate_Pct,
  -- PayPal Shipping
  ps.pp_ship_orders                                                                 AS PP_Shipping_Orders,
  ROUND(SAFE_DIVIDE(ps.pp_ship_success_orders, ps.pp_ship_orders) * 100, 2)         AS PP_Shipping_Success_Pct
FROM periods p
LEFT JOIN spl_auth_agg a  USING (period, sort_order)
LEFT JOIN spl_ship_agg c  USING (period, sort_order)
LEFT JOIN pp_auth_agg  pa USING (period, sort_order)
LEFT JOIN pp_ship_agg  ps USING (period, sort_order)
ORDER BY p.sort_order DESC;
