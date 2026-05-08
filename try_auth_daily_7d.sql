-- ============================================================
-- TRY AUTH SUCCESS RATES - DAILY TREND (Last 7 Days)
-- ============================================================
-- Output: 7 rows (one per day) with success rates per payment method
-- Payment methods: Credit Card | Apple Pay | PayPal | Overall
-- Methodology: Aligned with Redash #1610 (TBYB Success Rate Timeline)
-- ============================================================

WITH params AS (
  SELECT DATE_SUB(CURRENT_DATE('Asia/Jerusalem'), INTERVAL 1 DAY) AS yesterday
),
date_range AS (
  SELECT day_date
  FROM params, UNNEST(GENERATE_DATE_ARRAY(
    DATE_SUB(yesterday, INTERVAL 6 DAY),
    yesterday
  )) AS day_date
),

try_raw AS (
  SELECT pt.OrderID, pt.TransactionTime, pt.TransactionType,
    CASE WHEN s.Succeeded='True' THEN true WHEN s.Succeeded='False' THEN false ELSE pt.IsSuccessful END AS succeeded,
    COALESCE(s.Metadata_sub_transaction_type,
      CASE WHEN pt.TransactionType=7 THEN 'AUTH_ORIGINAL'
           WHEN pt.TransactionType=0 AND pt.Sum<10 THEN 'CAPTURE_SHIPPING'
           ELSE 'CAPTURE_FOLLOW_UP' END) AS sub_type,
    CASE WHEN LOWER(pt.EcType) LIKE '%paypal%'   THEN 'PayPal'
         WHEN LOWER(pt.EcType) LIKE '%applepay%' THEN 'Apple Pay'
         ELSE 'Credit Card' END AS pmt_method,
    CASE WHEN LOWER(pt.EcType) LIKE '%paypal%' THEN 'PayPal' ELSE 'Spreedly' END AS provider,
    pt.Sum AS amt
  FROM `cdc.PaymentTransactions_v` pt
  LEFT JOIN `spreedly.transaction_report_v` s ON pt.OrchestratorToken = s.token
  WHERE pt.TransactionType IN (0,7) AND pt.Sum > 0
  QUALIFY MAX(pt.TransactionType) OVER (PARTITION BY pt.OrderID) = 7
),

try_trans AS (
  SELECT *,
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
    END AS sub_type_final
  FROM try_raw
),

try_tagged AS (
  SELECT t.*, d.day_date
  FROM try_trans t
  JOIN date_range d ON t.first_date = d.day_date
),

try_auth_order AS (
  SELECT day_date, OrderID, pmt_method,
    MAX(CASE WHEN sub_type_final='AUTH_ORIGINAL'
             AND NOT (pmt_method='Apple Pay' AND amt=0) THEN 1 ELSE 0 END) AS ao_attempt,
    MAX(CASE WHEN sub_type_final IN ('AUTH_ORIGINAL','AUTH_MODIFIED')
             AND NOT (pmt_method='Apple Pay' AND amt=0)
             AND succeeded THEN 1 ELSE 0 END) AS auth_success
  FROM try_tagged
  WHERE TransactionType=7 AND sub_type_final IN ('AUTH_ORIGINAL','AUTH_MODIFIED')
  GROUP BY day_date, OrderID, pmt_method
),

-- Per payment method rates
daily_by_pmt AS (
  SELECT day_date,
    pmt_method,
    COUNTIF(ao_attempt=1) AS attempts,
    COUNTIF(ao_attempt=1 AND auth_success=1) AS successes,
    ROUND(SAFE_DIVIDE(COUNTIF(auth_success=1), COUNTIF(ao_attempt=1))*100, 2) AS success_rate
  FROM try_auth_order
  GROUP BY day_date, pmt_method
),

-- Overall daily rates (order-level dedup)
daily_overall AS (
  SELECT day_date,
    'Overall' AS pmt_method,
    COUNTIF(ao_attempt=1) AS attempts,
    COUNTIF(ao_attempt=1 AND auth_success=1) AS successes,
    ROUND(SAFE_DIVIDE(COUNTIF(ao_attempt=1 AND auth_success=1), COUNTIF(ao_attempt=1))*100, 2) AS success_rate
  FROM (
    SELECT day_date, OrderID,
      MAX(ao_attempt) AS ao_attempt, MAX(auth_success) AS auth_success
    FROM try_auth_order
    GROUP BY day_date, OrderID
  )
  GROUP BY day_date
)

-- ===== FINAL PIVOTED OUTPUT =====
SELECT
  d.day_date AS Date,
  FORMAT_DATE('%a', d.day_date) AS Day,
  o.attempts AS Total_Attempts,
  o.success_rate AS Overall_Rate,
  cc.success_rate AS Credit_Card_Rate,
  cc.attempts AS CC_Attempts,
  ap.success_rate AS Apple_Pay_Rate,
  ap.attempts AS AP_Attempts,
  pp.success_rate AS PayPal_Rate,
  pp.attempts AS PP_Attempts
FROM date_range d
LEFT JOIN daily_overall o ON d.day_date = o.day_date
LEFT JOIN daily_by_pmt cc ON d.day_date = cc.day_date AND cc.pmt_method = 'Credit Card'
LEFT JOIN daily_by_pmt ap ON d.day_date = ap.day_date AND ap.pmt_method = 'Apple Pay'
LEFT JOIN daily_by_pmt pp ON d.day_date = pp.day_date AND pp.pmt_method = 'PayPal'
ORDER BY d.day_date ASC;
