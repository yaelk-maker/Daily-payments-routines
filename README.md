# Daily-payments-routines

Daily monitoring of payment success rates for the live funnels (BUY Paid, BUY Unpaid, SUB first attempt, SUB blended) by payment method, posted as a single PNG to `#payments-daily-monitoring`.

## Files

| File | Purpose |
|---|---|
| `payment method success rates by funnel.sql` | **Routine query.** Returns 4 periods × 4 funnels (`BuyPaid`, `BuyUnpaid`, `Sub`, `SubAll`) × (`Total`, `Overall`, `CC`, `AP`, `PP`, plus per-method order counts `CC_N`/`AP_N`/`PP_N`), for BUY the Forter split per column (`<m>_Fraud`, `<m>_TotalSucc`, `<m>_AllN`, `AllOrders`) and `BuyPaid_Share`. |
| `run_daily_payments.py` | Reads the BQ result JSON, renders the daily PNG, commits + pushes it to the current branch, and posts the Slack `image` block. |
| `payments.png` | Latest rendered image; referenced by Slack via `raw.githubusercontent.com`. |
| `archive/try funnel daily monitoring.sql` | Retired TRY deep-dive (AO vs AM auth, fraud vs payment-fail shipping). Kept for reference only; TRY stopped selling on 23 Aug 2026. |

## What changed in September 2026

MAËLYS stopped selling through TRY on 23 Aug 2026. After that date TRY fell from ~600 to 2,300 orders/day to single or low double digits, and the prepaid-converted pool (the TRY→BUY reroute) fell to 0 to 8 orders/day. So the TRY Auth, TRY Shipping and Prepaid Converted tables no longer carried a usable signal. The routine now tracks the current key metrics:

| Funnel | Population |
|---|---|
| **BUY Paid** | BUY orders whose UTMs classify as paid media |
| **BUY Unpaid** | BUY orders that did not come from paid media (organic / direct, CRM, unpaid search, other) |
| **SUB** | Subscription recurring charges, first billing attempt only (unchanged) |
| **SUB Blended** | Every SUB order processed: first attempts plus dunning retries (attempts 2 to 6 of each cycle) |

## Methodology

**Paid vs Unpaid** uses the company definition: the same one behind `aas_equivalent.FS_STATIC.MediaPaidType`, `Orders_s.MediaPaidType` and the Live Report v2 `media_paid_type` filter:

```
aas_equivalent.media_paid_type(aas_equivalent.source_naming(UtmSource, UtmMedium, UtmCampaign))
```

| Class | `source_naming` channels |
|---|---|
| Non-Paid (Unpaid) | Organic / Direct, CRM, Search (Google with no paid markers), Other |
| Paid | Facebook, YouTube, Applovin, Snapchat, TikTok, Bing, PaidSearch, Affiliate, and any new channel (Unpaid is a whitelist) |

The functions are applied to the order's own UTMs in `cdc.OrdersNew_v`, not to `FS_STATIC`, because `FS_STATIC` only holds paid orders and a success rate needs the failed ones too. Attribution is the order's last-touch UTM, not the customer's first-touch source.

**BUY / SUB** follow Redash #1613 (BUY Success Rate Timeline):
- `TransactionType=0` / `CAPTURE_FULL`; `SUB = SitePart IN (10,12)` or Spreedly `Metadata_order_type='SUB'`; everything else is BUY
- CC fraud-blocked orders excluded from the success-rate denominator
- TRY orders are excluded from BUY and SUB: any order in `cdc.TbybOrders_v`, or with a TRY auth (`TransactionType=7`) in the window. Without this, TRY shipping and post-trial charges that have no Spreedly metadata (PayPal) fell through the `'BUY'` fallback. The TRY checkouts still coming in are customers completing old carts saved in their browser; they are immaterial and not reported.
- `OrdersNew_v.PrepaidConverted` orders are still excluded from BUY (the flow ended with TRY; ~20% approval would add noise)
- SUB is restricted to the first billing attempt (`subscriptions.SubscriptionsRecurringOrders_v.AttemptsAmount = 1`)
- SUB Blended takes every SUB order. Each retry is its own recurring order ID charged on a single day, so each order counts once on its processing date. The blended rate sits far below the first-attempt rate: on 24 Sep, 1,029 first attempts approved at 58.9% and 2,208 retries at ~3.9%, so 3,237 orders were approved at 21.4%. It moves with the retry mix as well as with payments health.

Common across all funnels:
- Source: `cdc.PaymentTransactions_v` `LEFT JOIN spreedly.transaction_report_v` on `OrchestratorToken`
- `sum > 0` filter (zero-amount transactions excluded)
- Period attribution: order's first `DATE(TransactionTime)` (matching Redash, no timezone conversion)
- Order-level dedup (`MAX` over flags); `Overall` and `Total` columns dedup orders once across payment methods
- Rates rounded to 2 decimal places; the image renders them at 1dp

**BUY Yesterday rows** (order level, BUY only). Each BUY table shows three Yesterday rows:

| Row | Definition |
|---|---|
| Yest. card success | Approved / orders that were **not** Forter-declined |
| Yest. fraud declines | Forter-declined orders / **all** orders. Forter-declined = the order never succeeded and at least one attempt was blocked by Forter's pre-auth fraud check (Spreedly `Message` contains `fraud`: "gateway transaction not attempted due to failed pre authorization fraud check.") |
| Yest. overall success | Approved / **all** orders, including Forter-declined. This is the metric in the Last 7d / MTD / Prev month rows. |

The three reconcile: overall success = card success × (1 − fraud declines). Forter blocks are almost all credit card: from 26 Aug to 24 Sep there was 1 Apple Pay block and 0 PayPal, so Apple Pay and PayPal normally show 0.0% fraud declines. SUB is merchant-initiated and not Forter-screened.

Change in Sep 2026: card success used to drop an order from the denominator only when **every** attempt on it was Forter-blocked. It now drops every Forter-declined order, so the three rows reconcile. On 24 Sep data this moved the historical BUY rates by +0.03 to +0.10pp (e.g. BUY Paid last 7d 96.37% → 96.43%).

**Note on SUB:** the rate reflects same-day billing success. Orders that fail same-day enter dunning and may succeed on subsequent days, so the SUB rate here is a leading indicator for anomaly detection, not a final renewal rate.

## Daily routine

Runs as the Claude Code Remote Routine **"Payments success rate monitoring"** (daily 04:30 UTC). The routine prompt references only the two file names, so it needs no change. Each run:

1. Execute `payment method success rates by funnel.sql` via the BigQuery MCP (project `maelys-data`) and capture the 4 rows as JSON, for example saved to `/tmp/bq_results.json`.
2. Run `python run_daily_payments.py /tmp/bq_results.json` (or `... -` for stdin). The script:
   - Renders `payments.png` (page title, color legend, BUY Paid / BUY Unpaid / SUB / SUB Blended tables with traffic-light highlighting on the Yesterday row, and a footer line).
   - Commits and pushes `payments.png` to the current branch.
   - Posts a Slack `image` block to `#payments-daily-monitoring` referencing `https://raw.githubusercontent.com/yaelk-maker/Daily-payments-routines/<branch>/payments.png?v=<ts>`.

### Required environment

| Variable | Purpose |
|---|---|
| `SLACK_BOT_TOKEN` | Slack bot token with `chat:write` |
| `SLACK_CHANNEL_PAYMENTS` | Channel ID for `#payments-daily-monitoring` |

### Why GitHub-hosted images

Slack `image` blocks require a publicly fetchable HTTPS URL. The Claude Code Remote Routine sandbox cannot reach `files.slack.com` (no `files.upload`), so the image is committed to the repo and Slack fetches it from `raw.githubusercontent.com`, which is publicly cached and reliable.

## Slack message format

A single PNG with:
- **Page title:** `Payment Success Rates - YYYY-MM-DD`
- **Legend:** `Delta vs Last 7d:` followed by colored swatches: `stable / up`, `-1 to -3pp`, `> -3pp drop`, `< 50 orders`
- **One table per funnel** (BUY Paid, BUY Unpaid, SUB, SUB Blended at the bottom) with rows `Yesterday | Last 7d | MTD | Prev month` and columns `Period | Overall | CC | Apple Pay | PayPal | Δ Overall vs 7d`
- **BUY tables** have three Yesterday rows (`Yest. card success`, `Yest. fraud declines`, `Yest. overall success`) above `Last 7d | MTD | Prev month`
- **Footer:** paid media share of BUY orders (yesterday vs last 7d)

Thresholds (Yesterday vs Last 7d):
- 🟥 cell: drop > 3pp
- 🟨 cell: drop 1 to 3pp
- 🟩 cell: stable / up
- ⬜ grey cell: fewer than 50 orders yesterday for that payment method, not scored (one decline in 50 is 2pp; splitting BUY in two leaves PayPal and Apple Pay at ~35 to 60 orders a day)
- Δ text: red when < −0.5pp, green otherwise
- Δ and colours are computed from the 1dp values shown, so they can be checked against the table

BUY tables: only the `Yest. overall success` row is coloured. Its Δ is yesterday's overall success minus the Last 7d row shown directly below it (both include Forter-declined orders). The card success and fraud decline rows are plain numbers. SUB tables are unchanged: all rows show the SUB success rate.
