# Daily-payments-routines

Daily monitoring of payment success rates for the three live funnels (BUY Paid, BUY Unpaid, SUB) by payment method, posted as a single PNG to `#payments-daily-monitoring`.

## Files

| File | Purpose |
|---|---|
| `payment method success rates by funnel.sql` | **Routine query.** Returns 4 periods × 3 funnels (`BuyPaid`, `BuyUnpaid`, `Sub`) × (`Total`, `Overall`, `CC`, `AP`, `PP`, plus per-method order counts `CC_N`/`AP_N`/`PP_N`), and `BuyPaid_Share` and `Try_Residual_Orders`. |
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
- CC fraud-blocked orders excluded from the denominator
- TRY orders are excluded from BUY and SUB: any order in `cdc.TbybOrders_v`, or with a TRY auth (`TransactionType=7`) in the window. Without this, TRY shipping and post-trial charges that have no Spreedly metadata (PayPal) fell through the `'BUY'` fallback.
- `OrdersNew_v.PrepaidConverted` orders are still excluded from BUY (the flow ended with TRY; ~20% approval would add noise)
- SUB is restricted to the first billing attempt (`subscriptions.SubscriptionsRecurringOrders_v.AttemptsAmount = 1`)

Common across all funnels:
- Source: `cdc.PaymentTransactions_v` `LEFT JOIN spreedly.transaction_report_v` on `OrchestratorToken`
- `sum > 0` filter (zero-amount transactions excluded)
- Period attribution: order's first `DATE(TransactionTime)` (matching Redash, no timezone conversion)
- Order-level dedup (`MAX` over flags); `Overall` and `Total` columns dedup orders once across payment methods
- Rates rounded to 2 decimal places; the image renders them at 1dp

**Note on SUB:** the rate reflects same-day billing success. Orders that fail same-day enter dunning and may succeed on subsequent days, so the SUB rate here is a leading indicator for anomaly detection, not a final renewal rate.

## Daily routine

Runs as the Claude Code Remote Routine **"Payments success rate monitoring"** (daily 04:30 UTC). The routine prompt references only the two file names, so it needs no change. Each run:

1. Execute `payment method success rates by funnel.sql` via the BigQuery MCP (project `maelys-data`) and capture the 4 rows as JSON, for example saved to `/tmp/bq_results.json`.
2. Run `python run_daily_payments.py /tmp/bq_results.json` (or `... -` for stdin). The script:
   - Renders `payments.png` (page title, color legend, three funnel tables with traffic-light highlighting on the Yesterday row, two footer lines).
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
- **One table per funnel** (BUY Paid, BUY Unpaid, SUB) with rows `Yesterday | Last 7d | MTD | Prev month` and columns `Period | Overall | CC | Apple Pay | PayPal | Δ Overall vs 7d`
- **Footer:** paid media share of BUY orders (yesterday vs last 7d), and the residual TRY checkout count (yesterday vs last 7d daily average)

Thresholds (Yesterday vs Last 7d):
- 🟥 cell: drop > 3pp
- 🟨 cell: drop 1 to 3pp
- 🟩 cell: stable / up
- ⬜ grey cell: fewer than 50 orders yesterday for that payment method, not scored (one decline in 50 is 2pp; splitting BUY in two leaves PayPal and Apple Pay at ~35 to 60 orders a day)
- Δ text: red when < −0.5pp, green otherwise

## Residual TRY checkouts

TRY checkouts have not gone to zero. In 1 to 24 Sep 2026 there were ~22 new TRY auth orders/day, all US, ~87% from paid media (mostly Applovin and Facebook), with ~58% auth approval. They are excluded from every table, and the footer shows the daily count so a jump is visible.
