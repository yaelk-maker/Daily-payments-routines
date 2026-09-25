-- ============================================================
-- PAYMENT METHOD SUCCESS RATES BY FUNNEL
-- ============================================================
-- Output: 4 rows (periods) × 4 funnels × (Total, Overall, CC, AP, PP, AF + per-method
--         attempt counts *_N, used by the image to skip scoring low-sample cells)
-- Funnels: BUY Paid | BUY Unpaid | SUB (first attempt) | SUB Blended (all attempts)
-- Payment methods: Credit Card (CC) | Apple Pay (AP) | PayPal (PP) | AfterPay (AF)
--   AfterPay (EcType 'AfterPay') does not route through Spreedly, so it has no
--   OrchestratorToken match; its outcome comes from cdc IsSuccessful and it has
--   no Forter pre-auth flag. Before 2026-09-26 it fell into Credit Card (~30% of
--   the CC column). SUB has no AfterPay orders.
--
-- 2026-09 redesign: MAËLYS stopped selling through TRY on 23 Aug 2026. The TRY
-- Auth / TRY Shipping / Prepaid-converted tables were retired (TRY fell from
-- ~600-2,300 orders/day to single digits; prepaid-converted to ~0-8/day) and
-- BUY is now split by acquisition source, matching the company KPIs:
--   BuyPaid_*   = BUY orders whose UTMs classify as paid media
--   BuyUnpaid_* = BUY orders that did not come from paid media
--   Sub_*       = subscription recurring charges, first attempt of billing cycle 1
--                 only (the regular monthly charge; see SUB note below)
--   SubAll_*    = SUB blended: every SUB order processed (first attempt + dunning
--                 retries). Each retry is its own recurring order ID charged on a
--                 single day, so each order counts once on its processing date
--   BUY only, per column <m> in (Overall, CC, AP, PP, AF):
--     <m>_Fraud     = Forter fraud declines, % of ALL orders (incl. Forter-blocked)
--     <m>_TotalSucc = overall success, % of ALL orders (incl. Forter-blocked)
--     <m>_AllN / AllOrders = order counts behind those two rates
--
-- Paid / Unpaid split — company definition, identical to
-- aas_equivalent.FS_STATIC.MediaPaidType / Orders_s.MediaPaidType / Live Report v2:
--   media_paid_type(source_naming(UtmSource, UtmMedium, UtmCampaign))
--   Unpaid ('Non-Paid') = Organic / Direct, CRM, Search (unpaid Google), Other
--   Paid                = everything else: Facebook, YouTube, Applovin, Snapchat,
--                         TikTok, Bing, PaidSearch, Affiliate (+ any new source)
--   Applied to the order's own UTMs (cdc.OrdersNew_v), so failed-payment orders
--   are classified too (FS_STATIC only holds paid orders).
--
-- Methodology (BUY/SUB) — aligned with Redash #1613 (BUY Success Rate Timeline)
--   - TransactionType=0 (Receipt), CAPTURE_FULL stage
--   - SUB = SitePart IN (10,12) or Spreedly Metadata_order_type='SUB'
--   - BUY = everything else (COALESCE fallback='BUY')
--   - TRY orders (cdc.TbybOrders_v, or any TRY auth TransactionType=7 in the
--     window) are excluded from BUY/SUB, so TRY shipping / post-trial charges
--     without Spreedly metadata (PayPal) cannot fall through the 'BUY' fallback.
--     Remaining TRY checkouts (old carts saved in customers' browsers) are
--     immaterial and not reported.
--   - PrepaidConverted orders (TRY->BUY reroute, ~20% approval) excluded from
--     BUY; the flow ended with TRY and is no longer reported
--   - Forter-declined orders excluded from the denominator of the card success
--     rates (Overall / CC / AP / PP). NOTE (2026-09): previously only orders whose
--     every attempt was Forter-blocked were excluded; now any order that never
--     succeeded and had a Forter block is excluded, so the three BUY Yesterday
--     rows reconcile: TotalSucc = card success x (1 - Fraud). Moves rates ~0.05pp.
--   - Forter split (BUY only; SUB is merchant-initiated, no Forter screening):
--       Fraud decline = order never succeeded and at least one attempt was blocked
--                       by Forter pre-auth (Spreedly Message LIKE '%fraud%':
--                       "gateway transaction not attempted due to failed pre
--                       authorization fraud check.")
--       Card success  (<m>)           = success / orders excl. Forter-declined
--       Overall success (<m>_TotalSucc) = success / all orders incl. Forter blocks
--   - SUB restricted to attempt 1 of billing cycle 1. When a cycle fails all 6
--     attempts the next month restarts at AttemptsAmount=1 with the SAME
--     RecurringNumber, so AttemptsAmount=1 alone also picks up cycle 2/3
--     restarts (~18% of attempt-1 orders, ~2-5% approval). Cycle 1 = the first
--     AttemptsAmount=1 order for a (SubscriptionId, RecurringNumber), over full
--     history. Restarts stay in SUB Blended. NOTE (2026-09-26): previously all
--     AttemptsAmount=1 orders; on 24 Sep that moved SUB from 58.9% to 75.2%.
--   - SubscriptionsRecurringOrders_v carries duplicate rows per RecurringOrderId
--     (~37% of ids); always dedup before use.
--   - NOTE: SUB rate = same-day billing success; remaining orders enter dunning
--     and may succeed on subsequent days. Use for anomaly detection, not final rates.
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

-- ========== BUY + SUB (#1613 methodology) ==========
buy_sub_raw AS (
  SELECT pt.OrderID, pt.TransactionTime, pt.TransactionType,
    CASE WHEN s.Succeeded='True' THEN true WHEN s.Succeeded='False' THEN false ELSE pt.IsSuccessful END AS succeeded,
    CASE WHEN IFNULL(LOWER(s.Message),'') LIKE '%fraud%' THEN true ELSE false END AS fraud_flag,
    COALESCE(s.Metadata_sub_transaction_type,'CAPTURE_FULL') AS sub_type,
    CASE WHEN LOWER(pt.EcType) LIKE '%paypal%'   THEN 'PayPal'
         WHEN LOWER(pt.EcType) LIKE '%applepay%' THEN 'Apple Pay'
         WHEN LOWER(pt.EcType) LIKE '%afterpay%' THEN 'AfterPay'
         ELSE                                         'Credit Card' END AS pmt_method,
    CASE WHEN LOWER(pt.EcType) LIKE '%paypal%' THEN true ELSE false END AS is_paypal,
    pt.Sum AS amt,
    COALESCE(s.Metadata_order_type, o.order_type, 'BUY') AS order_type,
    IFNULL(o.prepaid_conv, FALSE) AS prepaid_conv,
    o.media,
    tb.OrderId IS NOT NULL AS is_try,
    MAX(pt.TransactionType) OVER (PARTITION BY pt.OrderID) AS max_tt
  FROM `cdc.PaymentTransactions_v` pt
  LEFT JOIN `spreedly.transaction_report_v` s ON pt.OrchestratorToken = s.token
  LEFT JOIN (
    SELECT ID AS OrderID,
           CASE WHEN SitePart IN (10,12) THEN 'SUB' END AS order_type,
           PrepaidConverted AS prepaid_conv,
           aas_equivalent.media_paid_type(
             aas_equivalent.source_naming(UtmSource, UtmMedium, UtmCampaign)) AS media
    FROM `cdc.OrdersNew_v`
  ) o ON o.OrderID = pt.OrderID
  -- TRY order flag, same convention as FS_STATIC (COALESCE(tbyb.id,0) > 1)
  LEFT JOIN (SELECT DISTINCT OrderId FROM `cdc.TbybOrders_v` WHERE Id > 1) tb
    ON tb.OrderId = pt.OrderID
  WHERE pt.TransactionType IN (0,7) AND pt.Sum>0
    AND DATE(pt.TransactionTime)
        BETWEEN (SELECT MIN(d_start) FROM periods) AND (SELECT MAX(d_end) FROM periods)
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
    AND max_tt = 0 AND NOT is_try
),
buy_sub_tagged AS (
  SELECT t.*, p.period, p.sort_order
  FROM buy_sub_trans t
  JOIN periods p ON t.first_date BETWEEN p.d_start AND p.d_end
  WHERE DATE(TransactionTime) < CURRENT_DATE('Asia/Jerusalem')
),
buy_sub_order AS (
  SELECT period, sort_order, OrderID, pmt_method, order_type,
    MAX(CASE WHEN prepaid_conv THEN 1 ELSE 0 END) AS prepaid_conv,
    ANY_VALUE(media) AS media,
    MAX(CASE WHEN succeeded THEN 1 ELSE 0 END) AS success,
    MAX(CASE WHEN fraud_flag THEN 1 ELSE 0 END) AS forter_block,
    -- scored for card success unless the order was declined by Forter
    CASE WHEN MAX(CASE WHEN succeeded THEN 1 ELSE 0 END)=1
           OR MAX(CASE WHEN fraud_flag THEN 1 ELSE 0 END)=0 THEN 1 ELSE 0 END AS attempt
  FROM buy_sub_tagged
  WHERE capture_type='CAPTURE_FULL'
  GROUP BY period, sort_order, OrderID, pmt_method, order_type
),
-- SUB: attempt 1 of billing cycle 1 only (no dunning retries, no failed-month
--   restarts). Source: Redash #1573 / subscriptions.SubscriptionsRecurringOrders_v.
sub_recurring AS (   -- dedup: the view carries duplicate rows per RecurringOrderId
  SELECT RecurringOrderId, ANY_VALUE(SubscriptionId) AS SubscriptionId,
         MAX(AttemptsAmount) AS AttemptsAmount, MAX(RecurringNumber) AS RecurringNumber
  FROM `subscriptions.SubscriptionsRecurringOrders_v`
  GROUP BY RecurringOrderId
),
sub_first_attempt AS (
  SELECT r.RecurringOrderId
  FROM sub_recurring r
  JOIN `cdc.OrdersNew_v` o ON o.Id = r.RecurringOrderId
  WHERE r.AttemptsAmount = 1
  QUALIFY ROW_NUMBER() OVER (PARTITION BY r.SubscriptionId, r.RecurringNumber
                             ORDER BY o.OrderCreateDate, r.RecurringOrderId) = 1
),
-- Funnel label per order-method row; rows outside the funnels drop out.
-- SUB first-attempt rows appear twice: once as 'Sub', once in 'SubAll'
funnel_order AS (
  SELECT b.*,
    CASE
      WHEN order_type='BUY' AND prepaid_conv=0 AND media='Paid'     THEN 'BuyPaid'
      WHEN order_type='BUY' AND prepaid_conv=0 AND media='Non-Paid' THEN 'BuyUnpaid'
      WHEN order_type='SUB' AND f.RecurringOrderId IS NOT NULL      THEN 'Sub'
    END AS funnel
  FROM buy_sub_order b
  LEFT JOIN sub_first_attempt f ON b.OrderID = f.RecurringOrderId
  UNION ALL
  -- SUB blended: all SUB orders, attempts 1-6 of every cycle
  SELECT b.*, 'SubAll' AS funnel FROM buy_sub_order b WHERE order_type='SUB'
),
funnel_pivot AS (
  SELECT funnel, period, sort_order,
    ROUND(SAFE_DIVIDE(COUNTIF(pmt_method='Credit Card' AND success=1), COUNTIF(pmt_method='Credit Card' AND attempt=1))*100, 2) AS CC,
    ROUND(SAFE_DIVIDE(COUNTIF(pmt_method='Apple Pay'   AND success=1), COUNTIF(pmt_method='Apple Pay'   AND attempt=1))*100, 2) AS AP,
    ROUND(SAFE_DIVIDE(COUNTIF(pmt_method='PayPal'      AND success=1), COUNTIF(pmt_method='PayPal'      AND attempt=1))*100, 2) AS PP,
    ROUND(SAFE_DIVIDE(COUNTIF(pmt_method='AfterPay'    AND success=1), COUNTIF(pmt_method='AfterPay'    AND attempt=1))*100, 2) AS AF,
    COUNTIF(pmt_method='Credit Card' AND attempt=1) AS CC_N,
    COUNTIF(pmt_method='Apple Pay'   AND attempt=1) AS AP_N,
    COUNTIF(pmt_method='PayPal'      AND attempt=1) AS PP_N,
    COUNTIF(pmt_method='AfterPay'    AND attempt=1) AS AF_N,
    ROUND(SAFE_DIVIDE(COUNTIF(pmt_method='Credit Card' AND success=0 AND forter_block=1), COUNTIF(pmt_method='Credit Card'))*100, 2) AS CC_Fraud,
    ROUND(SAFE_DIVIDE(COUNTIF(pmt_method='Apple Pay'   AND success=0 AND forter_block=1), COUNTIF(pmt_method='Apple Pay'))*100, 2)   AS AP_Fraud,
    ROUND(SAFE_DIVIDE(COUNTIF(pmt_method='PayPal'      AND success=0 AND forter_block=1), COUNTIF(pmt_method='PayPal'))*100, 2)      AS PP_Fraud,
    ROUND(SAFE_DIVIDE(COUNTIF(pmt_method='AfterPay'    AND success=0 AND forter_block=1), COUNTIF(pmt_method='AfterPay'))*100, 2)    AS AF_Fraud,
    ROUND(SAFE_DIVIDE(COUNTIF(pmt_method='Credit Card' AND success=1), COUNTIF(pmt_method='Credit Card'))*100, 2) AS CC_TotalSucc,
    ROUND(SAFE_DIVIDE(COUNTIF(pmt_method='Apple Pay'   AND success=1), COUNTIF(pmt_method='Apple Pay'))*100, 2)   AS AP_TotalSucc,
    ROUND(SAFE_DIVIDE(COUNTIF(pmt_method='PayPal'      AND success=1), COUNTIF(pmt_method='PayPal'))*100, 2)      AS PP_TotalSucc,
    ROUND(SAFE_DIVIDE(COUNTIF(pmt_method='AfterPay'    AND success=1), COUNTIF(pmt_method='AfterPay'))*100, 2)    AS AF_TotalSucc,
    COUNTIF(pmt_method='Credit Card') AS CC_AllN,
    COUNTIF(pmt_method='Apple Pay')   AS AP_AllN,
    COUNTIF(pmt_method='PayPal')      AS PP_AllN,
    COUNTIF(pmt_method='AfterPay')    AS AF_AllN
  FROM funnel_order WHERE funnel IS NOT NULL GROUP BY funnel, period, sort_order
),
funnel_overall AS (
  SELECT funnel, period, sort_order,
    COUNTIF(attempt=1)                                                                    AS total_attempts,
    ROUND(SAFE_DIVIDE(COUNTIF(attempt=1 AND success=1), COUNTIF(attempt=1))*100, 2) AS overall_rate,
    COUNT(*)                                                                    AS all_orders,
    ROUND(SAFE_DIVIDE(COUNTIF(success=0 AND forter_block=1), COUNT(*))*100, 2) AS overall_fraud,
    ROUND(SAFE_DIVIDE(COUNTIF(success=1), COUNT(*))*100, 2)                    AS overall_total_succ
  FROM (
    -- order level: Forter-declined = never succeeded and at least one Forter block
    SELECT funnel, period, sort_order, OrderID,
      MAX(success) AS success, MAX(forter_block) AS forter_block,
      CASE WHEN MAX(success)=1 OR MAX(forter_block)=0 THEN 1 ELSE 0 END AS attempt
    FROM funnel_order WHERE funnel IS NOT NULL GROUP BY funnel, period, sort_order, OrderID
  )
  GROUP BY funnel, period, sort_order
),
funnel_stats AS (
  SELECT * FROM funnel_overall LEFT JOIN funnel_pivot USING (funnel, period, sort_order)
),
bp AS (SELECT * FROM funnel_stats WHERE funnel='BuyPaid'),
bu AS (SELECT * FROM funnel_stats WHERE funnel='BuyUnpaid'),
su AS (SELECT * FROM funnel_stats WHERE funnel='Sub'),
sa AS (SELECT * FROM funnel_stats WHERE funnel='SubAll')

-- ========== FINAL OUTPUT ==========
SELECT
  p.period                                               AS Period,
  bp.total_attempts AS BuyPaid_Total,   bp.overall_rate AS BuyPaid_Overall,
  bp.CC AS BuyPaid_CC,   bp.AP AS BuyPaid_AP,   bp.PP AS BuyPaid_PP, bp.AF AS BuyPaid_AF,
  bp.CC_N AS BuyPaid_CC_N,   bp.AP_N AS BuyPaid_AP_N,   bp.PP_N AS BuyPaid_PP_N, bp.AF_N AS BuyPaid_AF_N,
  bp.all_orders AS BuyPaid_AllOrders, bp.overall_fraud AS BuyPaid_Overall_Fraud, bp.overall_total_succ AS BuyPaid_Overall_TotalSucc,
  bp.CC_Fraud AS BuyPaid_CC_Fraud, bp.AP_Fraud AS BuyPaid_AP_Fraud, bp.PP_Fraud AS BuyPaid_PP_Fraud, bp.AF_Fraud AS BuyPaid_AF_Fraud,
  bp.CC_TotalSucc AS BuyPaid_CC_TotalSucc, bp.AP_TotalSucc AS BuyPaid_AP_TotalSucc, bp.PP_TotalSucc AS BuyPaid_PP_TotalSucc, bp.AF_TotalSucc AS BuyPaid_AF_TotalSucc,
  bp.CC_AllN AS BuyPaid_CC_AllN, bp.AP_AllN AS BuyPaid_AP_AllN, bp.PP_AllN AS BuyPaid_PP_AllN, bp.AF_AllN AS BuyPaid_AF_AllN,
  bu.total_attempts AS BuyUnpaid_Total, bu.overall_rate AS BuyUnpaid_Overall,
  bu.CC AS BuyUnpaid_CC, bu.AP AS BuyUnpaid_AP, bu.PP AS BuyUnpaid_PP, bu.AF AS BuyUnpaid_AF,
  bu.CC_N AS BuyUnpaid_CC_N, bu.AP_N AS BuyUnpaid_AP_N, bu.PP_N AS BuyUnpaid_PP_N, bu.AF_N AS BuyUnpaid_AF_N,
  bu.all_orders AS BuyUnpaid_AllOrders, bu.overall_fraud AS BuyUnpaid_Overall_Fraud, bu.overall_total_succ AS BuyUnpaid_Overall_TotalSucc,
  bu.CC_Fraud AS BuyUnpaid_CC_Fraud, bu.AP_Fraud AS BuyUnpaid_AP_Fraud, bu.PP_Fraud AS BuyUnpaid_PP_Fraud, bu.AF_Fraud AS BuyUnpaid_AF_Fraud,
  bu.CC_TotalSucc AS BuyUnpaid_CC_TotalSucc, bu.AP_TotalSucc AS BuyUnpaid_AP_TotalSucc, bu.PP_TotalSucc AS BuyUnpaid_PP_TotalSucc, bu.AF_TotalSucc AS BuyUnpaid_AF_TotalSucc,
  bu.CC_AllN AS BuyUnpaid_CC_AllN, bu.AP_AllN AS BuyUnpaid_AP_AllN, bu.PP_AllN AS BuyUnpaid_PP_AllN, bu.AF_AllN AS BuyUnpaid_AF_AllN,
  ROUND(SAFE_DIVIDE(bp.total_attempts, bp.total_attempts + bu.total_attempts)*100, 2) AS BuyPaid_Share,
  su.total_attempts AS Sub_Total,       su.overall_rate AS Sub_Overall,
  su.CC AS Sub_CC,       su.AP AS Sub_AP,       su.PP AS Sub_PP, su.AF AS Sub_AF,
  su.CC_N AS Sub_CC_N,       su.AP_N AS Sub_AP_N,       su.PP_N AS Sub_PP_N, su.AF_N AS Sub_AF_N,
  sa.total_attempts AS SubAll_Total,    sa.overall_rate AS SubAll_Overall,
  sa.CC AS SubAll_CC,    sa.AP AS SubAll_AP,    sa.PP AS SubAll_PP, sa.AF AS SubAll_AF,
  sa.CC_N AS SubAll_CC_N,    sa.AP_N AS SubAll_AP_N,    sa.PP_N AS SubAll_PP_N, sa.AF_N AS SubAll_AF_N
FROM periods p
LEFT JOIN bp USING (period, sort_order)
LEFT JOIN bu USING (period, sort_order)
LEFT JOIN su USING (period, sort_order)
LEFT JOIN sa USING (period, sort_order)
ORDER BY p.sort_order DESC;
