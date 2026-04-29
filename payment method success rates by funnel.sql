-- ============================================================
-- PAYMENT METHOD SUCCESS RATES BY FUNNEL
-- ============================================================
-- Output: 4 rows (periods) × 5 columns per funnel (4 funnels)
--         Period | Attempts | Overall% | CC% | Apple Pay% | PayPal%
-- Funnels: TRY Auth | TRY Shipping | BUY | SUB
-- Payment methods: Credit Card | Apple Pay | PayPal
--
-- Methodology:
--   TRY Auth/Shipping — aligned with Redash #1610 (TBYB Success Rate Timeline)
--     - QUALIFY MAX(TransactionType)=7 for TRY order identification
--     - PP AUTH_MODIFIED via window function; PP shipping includes CAPTURE_FOLLOW_UP
--     - CC fraud-blocked excluded from shipping denominator
--     - Apple Pay $0-amount auth attempts excluded (matching #1610)
--     - Date attribution: DATE(TransactionTime), matching #1610 (no TZ conversion)
--   BUY/SUB — aligned with Redash #1613 (BUY Success Rate Timeline)
--     - TransactionType=0 (Receipt), CAPTURE_FULL stage
--     - SUB = SitePart IN (10,12) or Spreedly Metadata_order_type='SUB'
--     - BUY = everything else (COALESCE fallback='BUY')
--     - CC fraud-blocked excluded from denominator
--     - NOTE: SUB rate = same-day billing success; remaining orders enter dunning
--       and may succeed on subsequent days. Use for anomaly detection, not final rates.
--
-- All funnels:
--   - Order-level dedup (MAX) per payment method for per-method rates
--   - Overall rate + total attempts use order-level dedup (each order counted once)
-- ============================================================

WITH params AS (
  SELECT DATE_SUB(CURRENT_DATE('Asia/Jerusalem'), INTERVAL 1 DAY) AS yesterday
),
periods AS (
  SELECT 'P1. Previous month' AS period,
         DATE_TRUNC(DATE_SUB(DATE_TRUNC(yesterday, MONTH), INTERVAL 1 DAY), MONTH) AS d_start,
         DATE_SUB(DATE_TRUNC(yesterday, MONTH), INTERVAL 1 DAY) AS d_end,
         1 AS sort_order
  FROM params UNION ALL
  SELECT 'P2. MTD (excl yesterday)', DATE_TRUNC(yesterday, MONTH), DATE_SUB(yesterday, INTERVAL 1 DAY), 2 FROM params UNION ALL
  SELECT 'P3. Last 7d',              DATE_SUB(yesterday, INTERVAL 7 DAY), DATE_SUB(yesterday, INTERVAL 1 DAY), 3 FROM params UNION ALL
  SELECT 'P4. Yesterday',            yesterday, yesterday, 4 FROM params
),

-- ========== TRY (#1610 methodology) ==========
try_raw AS (
  SELECT pt.OrderID, pt.TransactionTime, pt.TransactionType,
    CASE WHEN s.Succeeded='True' THEN true WHEN s.Succeeded='False' THEN false ELSE pt.IsSuccessful END AS succeeded,
    CASE WHEN IFNULL(LOWER(s.Message),'') LIKE '%fraud%' THEN true ELSE false END AS fraud_flag,
    COALESCE(s.Metadata_sub_transaction_type,
      CASE WHEN pt.TransactionType=7              THEN 'AUTH_ORIGINAL'
           WHEN pt.TransactionType=0 AND pt.Sum<10 THEN 'CAPTURE_SHIPPING'
           ELSE                                        'CAPTURE_FOLLOW_UP' END) AS sub_type,
    CASE WHEN LOWER(pt.EcType) LIKE '%paypal%'   THEN 'PayPal'
         WHEN LOWER(pt.EcType) LIKE '%applepay%' THEN 'Apple Pay'
         ELSE                                         'Credit Card' END AS pmt_method,
    CASE WHEN LOWER(pt.EcType) LIKE '%paypal%' THEN 'PayPal' ELSE 'Spreedly' END AS provider,
    pt.Sum AS amt
  FROM `cdc.PaymentTransactions_v` pt
  LEFT JOIN `spreedly.transaction_report_v` s ON pt.OrchestratorToken = s.token
  WHERE pt.TransactionType IN (0,7) AND pt.Sum > 0
  QUALIFY MAX(pt.TransactionType) OVER (PARTITION BY pt.OrderID) = 7
),
try_trans AS (
  SELECT *,
    -- DATE(TransactionTime) matches Redash #1610 (no timezone conversion)
    MIN(DATE(TransactionTime)) OVER (PARTITION BY OrderID) AS first_date,
    CASE
      WHEN provider='PayPal' AND TransactionType=7 AND sub_type='AUTH_ORIGINAL'
        AND MAX(CASE WHEN TransactionType=7 AND NOT succeeded THEN 1 ELSE 0 END)
            OVER (PARTITION BY OrderID ORDER BY TransactionTime
                  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) = 1
        AND amt < FIRST_VALUE(CASE WHEN TransactionType=7 THEN amt END IGNORE NULLS)
            OVER (PARTITION BY OrderID ORDER BY TransactionTime
                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
      THEN 'AUTH_MODIFIED' ELSE sub_type
    END AS sub_type_final,
    CASE
      WHEN provider='PayPal' AND TransactionType=0 AND amt>10
        AND TransactionTime=MAX(CASE WHEN TransactionType=0 AND amt>10
            THEN TransactionTime END) OVER (PARTITION BY OrderID)
      THEN true ELSE false
    END AS is_pp_followup
  FROM try_raw
),
try_tagged AS (
  SELECT t.*, p.period, p.sort_order
  FROM try_trans t
  JOIN periods p ON t.first_date BETWEEN p.d_start AND p.d_end
),

-- TRY AUTH: combined (AO or AM succeeded) / AO attempts
-- Apple Pay $0-amount auths excluded from attempt + success (matching #1610)
try_auth_order AS (
  SELECT period, sort_order, OrderID, pmt_method,
    MAX(CASE WHEN sub_type_final='AUTH_ORIGINAL'
             AND NOT (pmt_method='Apple Pay' AND amt=0) THEN 1 ELSE 0 END) AS ao_attempt,
    MAX(CASE WHEN sub_type_final IN ('AUTH_ORIGINAL','AUTH_MODIFIED')
             AND NOT (pmt_method='Apple Pay' AND amt=0)
             AND succeeded                              THEN 1 ELSE 0 END) AS auth_success
  FROM try_tagged
  WHERE TransactionType=7 AND sub_type_final IN ('AUTH_ORIGINAL','AUTH_MODIFIED')
  GROUP BY period, sort_order, OrderID, pmt_method
),
try_auth_pivot AS (
  SELECT period, sort_order,
    ROUND(SAFE_DIVIDE(COUNTIF(pmt_method='Credit Card' AND auth_success=1), COUNTIF(pmt_method='Credit Card' AND ao_attempt=1))*100, 1) AS CC,
    ROUND(SAFE_DIVIDE(COUNTIF(pmt_method='Apple Pay'   AND auth_success=1), COUNTIF(pmt_method='Apple Pay'   AND ao_attempt=1))*100, 1) AS AP,
    ROUND(SAFE_DIVIDE(COUNTIF(pmt_method='PayPal'      AND auth_success=1), COUNTIF(pmt_method='PayPal'      AND ao_attempt=1))*100, 1) AS PP
  FROM try_auth_order GROUP BY period, sort_order
),
try_auth_overall AS (
  SELECT period, sort_order,
    COUNTIF(ao_attempt=1)                                                                    AS total_attempts,
    ROUND(SAFE_DIVIDE(COUNTIF(ao_attempt=1 AND auth_success=1), COUNTIF(ao_attempt=1))*100, 1) AS overall_rate
  FROM (
    SELECT period, sort_order, OrderID,
      MAX(ao_attempt) AS ao_attempt, MAX(auth_success) AS auth_success
    FROM try_auth_order GROUP BY period, sort_order, OrderID
  )
  GROUP BY period, sort_order
),

-- TRY SHIPPING: success / attempts (CC fraud excluded from denom; PP includes CAPTURE_FOLLOW_UP)
try_ship_order AS (
  SELECT period, sort_order, OrderID, pmt_method,
    MAX(CASE WHEN pmt_method!='Credit Card' OR (succeeded OR NOT fraud_flag) THEN 1 ELSE 0 END) AS attempt,
    MAX(CASE WHEN succeeded THEN 1 ELSE 0 END) AS success
  FROM try_tagged
  WHERE (provider='Spreedly' AND sub_type_final='CAPTURE_SHIPPING')
     OR (provider='PayPal'   AND TransactionType=0 AND (sub_type_final='CAPTURE_SHIPPING' OR is_pp_followup))
  GROUP BY period, sort_order, OrderID, pmt_method
),
try_ship_pivot AS (
  SELECT period, sort_order,
    ROUND(SAFE_DIVIDE(COUNTIF(pmt_method='Credit Card' AND success=1), COUNTIF(pmt_method='Credit Card' AND attempt=1))*100, 1) AS CC,
    ROUND(SAFE_DIVIDE(COUNTIF(pmt_method='Apple Pay'   AND success=1), COUNTIF(pmt_method='Apple Pay'   AND attempt=1))*100, 1) AS AP,
    ROUND(SAFE_DIVIDE(COUNTIF(pmt_method='PayPal'      AND success=1), COUNTIF(pmt_method='PayPal'      AND attempt=1))*100, 1) AS PP
  FROM try_ship_order GROUP BY period, sort_order
),
try_ship_overall AS (
  SELECT period, sort_order,
    COUNTIF(attempt=1)                                                                    AS total_attempts,
    ROUND(SAFE_DIVIDE(COUNTIF(attempt=1 AND success=1), COUNTIF(attempt=1))*100, 1) AS overall_rate
  FROM (
    SELECT period, sort_order, OrderID,
      MAX(attempt) AS attempt, MAX(success) AS success
    FROM try_ship_order GROUP BY period, sort_order, OrderID
  )
  GROUP BY period, sort_order
),

-- ========== BUY + SUB (#1613 methodology) ==========
buy_sub_raw AS (
  SELECT pt.OrderID, pt.TransactionTime,
    CASE WHEN s.Succeeded='True' THEN true WHEN s.Succeeded='False' THEN false ELSE pt.IsSuccessful END AS succeeded,
    CASE WHEN IFNULL(LOWER(s.Message),'') LIKE '%fraud%' THEN true ELSE false END AS fraud_flag,
    COALESCE(s.Metadata_sub_transaction_type,'CAPTURE_FULL') AS sub_type,
    CASE WHEN LOWER(pt.EcType) LIKE '%paypal%'   THEN 'PayPal'
         WHEN LOWER(pt.EcType) LIKE '%applepay%' THEN 'Apple Pay'
         ELSE                                         'Credit Card' END AS pmt_method,
    CASE WHEN LOWER(pt.EcType) LIKE '%paypal%' THEN true ELSE false END AS is_paypal,
    pt.Sum AS amt,
    COALESCE(s.Metadata_order_type, o.order_type, 'BUY') AS order_type
  FROM `cdc.PaymentTransactions_v` pt
  LEFT JOIN `spreedly.transaction_report_v` s ON pt.OrchestratorToken = s.token
  LEFT JOIN (SELECT ID AS OrderID, 'SUB' AS order_type FROM `cdc.OrdersNew_v` WHERE SitePart IN (10,12)) o
    ON o.OrderID = pt.OrderID
  WHERE pt.TransactionType=0 AND pt.Sum>0
    AND DATE(pt.TransactionTime)
        BETWEEN (SELECT MIN(d_start) FROM periods) AND (SELECT MAX(d_end) FROM periods)
    AND COALESCE(s.Metadata_order_type, o.order_type, 'BUY') IN ('BUY','SUB')
),
buy_sub_trans AS (
  SELECT *,
    MIN(DATE(TransactionTime)) OVER (PARTITION BY OrderID) AS first_date,
    CASE
      WHEN NOT is_paypal AND sub_type='CAPTURE_FULL'
        AND MAX(CASE WHEN succeeded THEN amt END)
            OVER (PARTITION BY OrderID ORDER BY TransactionTime
                  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) IS NOT NULL
        AND amt > MAX(CASE WHEN succeeded THEN amt END)
            OVER (PARTITION BY OrderID ORDER BY TransactionTime
                  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
      THEN 'CAPTURE_POST_PURCHASE'
      WHEN is_paypal AND sub_type='CAPTURE_FULL'
        AND TIMESTAMP_DIFF(TIMESTAMP(TransactionTime),
            MIN(TIMESTAMP(TransactionTime)) OVER (PARTITION BY OrderID), MINUTE) > 20
      THEN 'CAPTURE_POST_PURCHASE'
      ELSE sub_type
    END AS capture_type
  FROM buy_sub_raw
),
buy_sub_tagged AS (
  SELECT t.*, p.period, p.sort_order
  FROM buy_sub_trans t
  JOIN periods p ON t.first_date BETWEEN p.d_start AND p.d_end
  WHERE DATE(TransactionTime) < CURRENT_DATE('Asia/Jerusalem')
),
buy_sub_order AS (
  SELECT period, sort_order, OrderID, pmt_method, order_type,
    MAX(CASE WHEN pmt_method!='Credit Card' OR (succeeded OR NOT fraud_flag) THEN 1 ELSE 0 END) AS attempt,
    MAX(CASE WHEN succeeded THEN 1 ELSE 0 END) AS success
  FROM buy_sub_tagged
  WHERE capture_type='CAPTURE_FULL'
  GROUP BY period, sort_order, OrderID, pmt_method, order_type
),
buy_pivot AS (
  SELECT period, sort_order,
    ROUND(SAFE_DIVIDE(COUNTIF(pmt_method='Credit Card' AND success=1), COUNTIF(pmt_method='Credit Card' AND attempt=1))*100, 1) AS CC,
    ROUND(SAFE_DIVIDE(COUNTIF(pmt_method='Apple Pay'   AND success=1), COUNTIF(pmt_method='Apple Pay'   AND attempt=1))*100, 1) AS AP,
    ROUND(SAFE_DIVIDE(COUNTIF(pmt_method='PayPal'      AND success=1), COUNTIF(pmt_method='PayPal'      AND attempt=1))*100, 1) AS PP
  FROM buy_sub_order WHERE order_type='BUY' GROUP BY period, sort_order
),
buy_overall AS (
  SELECT period, sort_order,
    COUNTIF(attempt=1)                                                                    AS total_attempts,
    ROUND(SAFE_DIVIDE(COUNTIF(attempt=1 AND success=1), COUNTIF(attempt=1))*100, 1) AS overall_rate
  FROM (
    SELECT period, sort_order, OrderID,
      MAX(attempt) AS attempt, MAX(success) AS success
    FROM buy_sub_order WHERE order_type='BUY' GROUP BY period, sort_order, OrderID
  )
  GROUP BY period, sort_order
),
sub_pivot AS (
  SELECT period, sort_order,
    ROUND(SAFE_DIVIDE(COUNTIF(pmt_method='Credit Card' AND success=1), COUNTIF(pmt_method='Credit Card' AND attempt=1))*100, 1) AS CC,
    ROUND(SAFE_DIVIDE(COUNTIF(pmt_method='Apple Pay'   AND success=1), COUNTIF(pmt_method='Apple Pay'   AND attempt=1))*100, 1) AS AP,
    ROUND(SAFE_DIVIDE(COUNTIF(pmt_method='PayPal'      AND success=1), COUNTIF(pmt_method='PayPal'      AND attempt=1))*100, 1) AS PP
  FROM buy_sub_order WHERE order_type='SUB' GROUP BY period, sort_order
),
sub_overall AS (
  SELECT period, sort_order,
    COUNTIF(attempt=1)                                                                    AS total_attempts,
    ROUND(SAFE_DIVIDE(COUNTIF(attempt=1 AND success=1), COUNTIF(attempt=1))*100, 1) AS overall_rate
  FROM (
    SELECT period, sort_order, OrderID,
      MAX(attempt) AS attempt, MAX(success) AS success
    FROM buy_sub_order WHERE order_type='SUB' GROUP BY period, sort_order, OrderID
  )
  GROUP BY period, sort_order
)

-- ========== FINAL OUTPUT ==========
SELECT
  p.period                                               AS Period,
  tao.total_attempts AS TryAuth_Total,  tao.overall_rate AS TryAuth_Overall,
  ta.CC  AS TryAuth_CC,  ta.AP  AS TryAuth_AP,  ta.PP  AS TryAuth_PP,
  tso.total_attempts AS TryShip_Total,  tso.overall_rate AS TryShip_Overall,
  ts.CC  AS TryShip_CC,  ts.AP  AS TryShip_AP,  ts.PP  AS TryShip_PP,
  bo.total_attempts  AS Buy_Total,      bo.overall_rate  AS Buy_Overall,
  b.CC   AS Buy_CC,       b.AP   AS Buy_AP,       b.PP   AS Buy_PP,
  so.total_attempts  AS Sub_Total,      so.overall_rate  AS Sub_Overall,
  s.CC   AS Sub_CC,       s.AP   AS Sub_AP,       s.PP   AS Sub_PP
FROM periods p
LEFT JOIN try_auth_pivot   ta  USING (period, sort_order)
LEFT JOIN try_auth_overall tao USING (period, sort_order)
LEFT JOIN try_ship_pivot   ts  USING (period, sort_order)
LEFT JOIN try_ship_overall tso USING (period, sort_order)
LEFT JOIN buy_pivot         b   USING (period, sort_order)
LEFT JOIN buy_overall       bo  USING (period, sort_order)
LEFT JOIN sub_pivot          s   USING (period, sort_order)
LEFT JOIN sub_overall        so  USING (period, sort_order)
ORDER BY p.sort_order DESC;
