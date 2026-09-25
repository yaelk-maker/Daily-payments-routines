-- ============================================================
-- PAYMENTS DAILY: success rates for BUY Paid, BUY Unpaid, SUB (US only)
-- ============================================================
-- Output: long format, one row per (period, segment, dim) with order counts
--   period  = 'Y'   yesterday (Israel date)
--             'B28' the 28 days before yesterday (the "normal" baseline)
--   segment = 'BuyPaid' | 'BuyUnpaid' | 'Sub'
--   dim     = 'Overall'                  n = orders attempted, k = orders completed
--             'CC' | 'AP' | 'PP' | 'AF'  (BUY) per payment method, n = orders that
--                                        tried the method, k = approved on it
--             'Forter'                   (BUY) n = orders, k = Forter-declined orders
--             'RN1' | 'RN2-3' | 'RN4+'   (SUB) by recurring order number
--   report_date = yesterday
-- The image computes every rate as k / n and scores yesterday against B28 with
-- an exact binomial test, so no rounding happens in SQL.
--
-- Scope
--   - US orders only (cdc.OrdersNew_v.Country = 'United States of America').
--   - TRY stopped selling on 23 Aug 2026. TRY orders (cdc.TbybOrders_v, or any
--     TRY auth TransactionType=7 in the window) are excluded; the remaining TRY
--     checkouts are old carts saved in customers' browsers and are immaterial.
--   - PrepaidConverted orders (TRY->BUY reroute) excluded from BUY.
--
-- BUY Paid / Unpaid: same rule as the Analysis report (reports.sales_analysis_*):
--   BuyPaid   = the order's UTMs classify as paid media, OR the customer is new
--               (no earlier paid order), whatever the UTMs say
--   BuyUnpaid = returning customer whose order did not come from paid media
--   Paid media = media_paid_type(source_naming(UtmSource, UtmMedium, UtmCampaign)),
--   the company definition behind FS_STATIC.MediaPaidType. New customer = the
--   user has no paid order (Status 1/3, or 11/12 as FS_STATIC treats them)
--   created before this one, so declined attempts are classified too.
--
-- Methodology (BUY/SUB), aligned with Redash #1613 (BUY Success Rate Timeline)
--   - TransactionType=0 (Receipt), CAPTURE_FULL stage, Sum > 0
--   - SUB = SitePart IN (10,12) or Spreedly Metadata_order_type='SUB'; else BUY
--   - Order dated by its first transaction in the window (DATE(TransactionTime),
--     Israel local time); one order counts once per period
--   - Completed = approved on any payment method (a customer declined on one
--     method who then pays with another counts as completed)
--   - Payment methods: Credit Card (real cards), Apple Pay, PayPal, AfterPay.
--     AfterPay does not route through Spreedly (no OrchestratorToken match), so
--     its outcome comes from cdc IsSuccessful and it has no Forter pre-auth flag.
--   - Forter-declined = order never succeeded and at least one attempt was
--     blocked by Forter's pre-auth check (Spreedly Message LIKE '%fraud%':
--     "gateway transaction not attempted due to failed pre authorization fraud
--     check."). SUB is merchant-initiated and not Forter-screened.
--   - SUB = attempt 1 of billing cycle 1 only (the regular monthly charge). A
--     cycle that fails all 6 attempts restarts next month at AttemptsAmount=1
--     with the SAME RecurringNumber; those restarts (~18% of attempt-1 orders,
--     2-5% approval) and retries 2-6 are excluded. Cycle 1 = the first
--     AttemptsAmount=1 order per (SubscriptionId, RecurringNumber), full history.
--     SubscriptionsRecurringOrders_v carries duplicate rows; it is deduplicated.
--   - SUB rate = same-day billing success; failed orders enter dunning.
-- ============================================================

WITH params AS (
  SELECT DATE_SUB(CURRENT_DATE('Asia/Jerusalem'), INTERVAL 1 DAY) AS yesterday
),
periods AS (
  SELECT 'Y'   AS period, yesterday AS d_start, yesterday AS d_end FROM params UNION ALL
  SELECT 'B28', DATE_SUB(yesterday, INTERVAL 28 DAY), DATE_SUB(yesterday, INTERVAL 1 DAY) FROM params
),
-- look back a week before the baseline so an order's first attempt is not cut off
window_start AS (SELECT DATE_SUB(yesterday, INTERVAL 35 DAY) AS d FROM params),

-- ========== order attributes ==========
first_paid AS (   -- each user's first paid order (FS_STATIC treats status 11/12 as paid)
  SELECT UserId,
         ARRAY_AGG(STRUCT(Id, OrderPaymentDate) ORDER BY OrderPaymentDate, Id LIMIT 1)[OFFSET(0)] AS fp
  FROM `cdc.OrdersNew_v`
  WHERE Status IN (1, 3, 11, 12) AND UserId IS NOT NULL AND OrderPaymentDate IS NOT NULL
  GROUP BY UserId
),
order_dim AS (
  SELECT o.Id AS OrderID,
         CASE WHEN o.SitePart IN (10,12) THEN 'SUB' END AS order_type,
         IFNULL(o.PrepaidConverted, FALSE) AS prepaid_conv,
         aas_equivalent.media_paid_type(
           aas_equivalent.source_naming(o.UtmSource, o.UtmMedium, o.UtmCampaign)) AS media,
         o.Country = 'United States of America' AS is_us,
         -- new customer: no paid order created before this one
         (f.fp IS NULL OR f.fp.Id = o.Id OR f.fp.OrderPaymentDate > o.OrderCreateDate) AS is_new
  FROM `cdc.OrdersNew_v` o
  LEFT JOIN first_paid f ON f.UserId = o.UserId
),

-- ========== BUY + SUB transactions (#1613 methodology) ==========
buy_sub_raw AS (
  SELECT pt.OrderID, pt.TransactionTime, pt.TransactionType,
    CASE WHEN s.Succeeded='True' THEN true WHEN s.Succeeded='False' THEN false ELSE pt.IsSuccessful END AS succeeded,
    CASE WHEN IFNULL(LOWER(s.Message),'') LIKE '%fraud%' THEN true ELSE false END AS fraud_flag,
    COALESCE(s.Metadata_sub_transaction_type,'CAPTURE_FULL') AS sub_type,
    CASE WHEN LOWER(pt.EcType) LIKE '%paypal%'   THEN 'PP'
         WHEN LOWER(pt.EcType) LIKE '%applepay%' THEN 'AP'
         WHEN LOWER(pt.EcType) LIKE '%afterpay%' THEN 'AF'
         ELSE                                         'CC' END AS pmt_method,
    CASE WHEN LOWER(pt.EcType) LIKE '%paypal%' THEN true ELSE false END AS is_paypal,
    pt.Sum AS amt,
    COALESCE(s.Metadata_order_type, d.order_type, 'BUY') AS order_type,
    d.prepaid_conv, d.media, d.is_us, d.is_new,
    tb.OrderId IS NOT NULL AS is_try,
    MAX(pt.TransactionType) OVER (PARTITION BY pt.OrderID) AS max_tt
  FROM `cdc.PaymentTransactions_v` pt
  LEFT JOIN `spreedly.transaction_report_v` s ON pt.OrchestratorToken = s.token
  LEFT JOIN order_dim d ON d.OrderID = pt.OrderID
  -- TRY order flag, same convention as FS_STATIC (COALESCE(tbyb.id,0) > 1)
  LEFT JOIN (SELECT DISTINCT OrderId FROM `cdc.TbybOrders_v` WHERE Id > 1) tb
    ON tb.OrderId = pt.OrderID
  WHERE pt.TransactionType IN (0,7) AND pt.Sum>0
    AND DATE(pt.TransactionTime) BETWEEN (SELECT d FROM window_start) AND (SELECT yesterday FROM params)
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
  WHERE TransactionType = 0 AND order_type IN ('BUY','SUB')
    AND max_tt = 0 AND NOT is_try AND is_us
),
buy_sub_order AS (   -- one row per order x payment method
  SELECT p.period, t.OrderID, t.pmt_method, t.order_type,
    MAX(CASE WHEN t.prepaid_conv THEN 1 ELSE 0 END) AS prepaid_conv,
    ANY_VALUE(t.media) AS media, LOGICAL_OR(t.is_new) AS is_new,
    MAX(CASE WHEN t.succeeded THEN 1 ELSE 0 END) AS success,
    MAX(CASE WHEN t.fraud_flag THEN 1 ELSE 0 END) AS forter_block
  FROM buy_sub_trans t
  JOIN periods p ON t.first_date BETWEEN p.d_start AND p.d_end
  WHERE t.capture_type = 'CAPTURE_FULL'
  GROUP BY p.period, t.OrderID, t.pmt_method, t.order_type
),
sub_recurring AS (   -- dedup: the view carries duplicate rows per RecurringOrderId
  SELECT RecurringOrderId, ANY_VALUE(SubscriptionId) AS SubscriptionId,
         MAX(AttemptsAmount) AS AttemptsAmount, MAX(RecurringNumber) AS RecurringNumber
  FROM `subscriptions.SubscriptionsRecurringOrders_v`
  GROUP BY RecurringOrderId
),
sub_first_attempt AS (   -- attempt 1 of billing cycle 1
  SELECT r.RecurringOrderId, r.RecurringNumber
  FROM sub_recurring r
  JOIN `cdc.OrdersNew_v` o ON o.Id = r.RecurringOrderId
  WHERE r.AttemptsAmount = 1
  QUALIFY ROW_NUMBER() OVER (PARTITION BY r.SubscriptionId, r.RecurringNumber
                             ORDER BY o.OrderCreateDate, r.RecurringOrderId) = 1
),
funnel_order AS (
  SELECT b.*, f.RecurringNumber,
    CASE
      WHEN b.order_type='BUY' AND b.prepaid_conv=0 AND (b.media='Paid' OR (b.media IS NOT NULL AND b.is_new))
        THEN 'BuyPaid'
      WHEN b.order_type='BUY' AND b.prepaid_conv=0 AND b.media='Non-Paid' AND NOT b.is_new
        THEN 'BuyUnpaid'
      WHEN b.order_type='SUB' AND f.RecurringOrderId IS NOT NULL
        THEN 'Sub'
    END AS segment
  FROM buy_sub_order b
  LEFT JOIN sub_first_attempt f ON b.OrderID = f.RecurringOrderId
),
order_level AS (
  SELECT period, segment, OrderID, ANY_VALUE(RecurringNumber) AS rn,
    MAX(success) AS success, MAX(forter_block) AS forter_block
  FROM funnel_order WHERE segment IS NOT NULL
  GROUP BY period, segment, OrderID
),
long AS (
  SELECT period, segment, 'Overall' AS dim, COUNT(*) AS n, COUNTIF(success=1) AS k
  FROM order_level GROUP BY period, segment
  UNION ALL
  SELECT period, segment, 'Forter', COUNT(*), COUNTIF(success=0 AND forter_block=1)
  FROM order_level WHERE segment IN ('BuyPaid','BuyUnpaid') GROUP BY period, segment
  UNION ALL
  SELECT period, segment, pmt_method, COUNT(*), COUNTIF(success=1)
  FROM funnel_order WHERE segment IN ('BuyPaid','BuyUnpaid') GROUP BY period, segment, pmt_method
  UNION ALL
  SELECT period, segment,
    CASE WHEN rn >= 4 THEN 'RN4+' WHEN rn >= 2 THEN 'RN2-3' ELSE 'RN1' END,
    COUNT(*), COUNTIF(success=1)
  FROM order_level WHERE segment = 'Sub' GROUP BY 1, 2, 3
)
SELECT (SELECT yesterday FROM params) AS report_date, period, segment, dim, n, k
FROM long
ORDER BY segment, dim, period DESC;
