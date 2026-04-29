# Daily-payments-routines

Daily monitoring of payment success rates across funnels (TRY, BUY, SUB) by payment method, posted as a single PNG to `#payments-daily-monitoring`.

## Files

| File | Purpose |
|---|---|
| `payment method success rates by funnel.sql` | **Routine query.** Returns 4 periods × 4 funnels (TRY Auth, TRY Shipping, BUY, SUB) × 5 columns (`Total`, `Overall`, `CC`, `AP`, `PP`). |
| `run_daily_payments.py` | Reads the BQ result JSON, renders the daily PNG, commits + pushes it to the current branch, and posts the Slack `image` block. |
| `payments.png` | Latest rendered image; referenced by Slack via `raw.githubusercontent.com`. |
| `try funnel daily monitoring.sql` | **Deep-dive query** (on demand). Detailed TRY-only breakdown: Spreedly vs PayPal, AO-vs-Combined Auth split, Fraud-vs-Payment-Fail Shipping split. |

## Methodology

Aligned with the canonical Redash queries:

| Funnel | Reference | Notes |
|---|---|---|
| TRY Auth, TRY Shipping | Redash #1610 (TBYB Success Rate Timeline) | `QUALIFY MAX(TransactionType)=7` for TRY identification; PP `AUTH_MODIFIED` via window function (failed AO + lower amount); Apple Pay `$0` auth attempts excluded from denominator; **Shipping = `CAPTURE_SHIPPING` only** (no `CAPTURE_FOLLOW_UP`/`FORCE_CAPTURE`); CC fraud-blocked excluded from shipping denominator. |
| BUY, SUB | Redash #1613 (BUY Success Rate Timeline) | `TransactionType=0` / `CAPTURE_FULL`; `SUB = SitePart IN (10,12)` or Spreedly `Metadata_order_type='SUB'`; CC fraud-blocked excluded. |

Common across all funnels:
- Source: `cdc.PaymentTransactions_v` `LEFT JOIN spreedly.transaction_report_v` on `OrchestratorToken`
- `sum > 0` filter (zero-amount transactions excluded)
- Period attribution: order's first `DATE(TransactionTime)` (matching Redash, no timezone conversion)
- Order-level dedup (`MAX` over flags); `Overall` and `Total` columns dedup orders once across payment methods
- Rates rounded to 2 decimal places; the image renders them at 1dp

**Note on SUB:** the rate reflects same-day billing success. Orders that fail same-day enter dunning and may succeed on subsequent days, so the SUB rate here is a leading indicator for anomaly detection — not a final renewal rate.

## Daily routine

Designed for **Claude Code Remote Routine**. Each run:

1. Execute `payment method success rates by funnel.sql` via the BigQuery MCP (project `maelys-data`) and capture the 4 rows as JSON — for example saved to `/tmp/bq_results.json`.
2. Run `python run_daily_payments.py /tmp/bq_results.json` (or `... -` for stdin). The script:
   - Renders `payments.png` with the styled layout (page title, color legend, four funnel tables with traffic-light highlighting on the Yesterday row).
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
- **Legend:** `Delta vs Last 7d:` followed by colored swatches — `stable / up`, `-1 to -3pp`, `> -3pp drop`
- **One table per funnel** (TRY Auth, TRY Shipping, BUY, SUB) with rows `Yesterday | Last 7d | MTD | Prev month` and columns `Period | Overall | CC | Apple Pay | PayPal | Δ Overall vs 7d`
- **Yesterday row** uses traffic-light cell backgrounds (per metric vs Last 7d) and a bold colored Δ value

Thresholds (Yesterday vs Last 7d):
- 🟥 cell: drop > 3pp
- 🟨 cell: drop 1–3pp
- 🟩 cell: stable / up
- Δ text: red when < −0.5pp, green otherwise

## Deep-dive (on demand)

When the daily image flags a TRY anomaly, run `try funnel daily monitoring.sql` for the AO-vs-AM split and the Fraud-vs-Payment-Fail breakdown — useful for diagnosing whether a TRY drop is driven by issuer declines, fraud rules, or PSP issues.
