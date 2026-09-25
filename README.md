# Daily-payments-routines

**Payments Daily**: a single PNG posted every morning to `#payments-daily-monitoring`. It shows yesterday's payment success for BUY Paid, BUY Unpaid and SUB (US only), each compared with its normal over the previous 28 days. It is built to stay quiet on normal days and name the problem on bad ones.

## Files

| File | Purpose |
|---|---|
| `payment method success rates by funnel.sql` | **Routine query.** Returns long-format order counts: one row per (`period`, `segment`, `dim`) with `n` and `k`, for yesterday (`Y`) and the previous 28 days (`B28`). No rates are rounded in SQL. |
| `run_daily_payments.py` | Reads the query result, scores yesterday against normal, renders `payments.png`, commits + pushes it, and posts the Slack `image` block. `--render-only [out.png]` renders without git or Slack. |
| `payments.png` | Latest rendered image; Slack loads it from `raw.githubusercontent.com`. |
| `archive/try funnel daily monitoring.sql` | Retired TRY deep-dive, kept for reference (TRY stopped selling on 23 Aug 2026). |

## What the image shows

| Section | Content |
|---|---|
| **Banner** | "All normal" in green, or one line per unusual cell (amber) or very unusual cell (red, listed first) |
| **Headline tiles** | BUY Paid, BUY Unpaid, SUB first attempt: yesterday's success rate, its normal, the change in pp, and `attempts · completed` |
| **BUY approval by payment method** | Paid and Unpaid × Card, Apple Pay, PayPal, AfterPay, plus Forter blocks. Each cell shows yesterday, its normal and yesterday's order count |
| **SUB first attempt by recurring order** | 1st recurring order, 2nd to 3rd, 4th+ |

Removed from the daily image by design: Last 7d / MTD / Prev month rows, SUB Blended (retries), and the per-processor view. Trend and retry recovery belong in a weekly review, and processor outages in an hourly alert.

## Definitions

| Term | Definition |
|---|---|
| Attempts | Orders with at least one charge attempt, each order counted once, dated by its first transaction (Israel date) |
| Completed | Attempted orders approved on any payment method. This is the number that compares with the sales reports' paid-order counts |
| Success rate | Completed ÷ attempts (Forter-declined orders included) |
| BUY Paid | The order's UTMs classify as paid media, **or** the customer is new (no paid order before this one). This is the Analysis report's rule, applied to declined attempts too |
| BUY Unpaid | A returning customer whose order did not come from paid media |
| Paid media | `aas_equivalent.media_paid_type(source_naming(UtmSource, UtmMedium, UtmCampaign))`, the company definition behind `FS_STATIC.MediaPaidType` |
| SUB first attempt | Attempt 1 of billing cycle 1: the regular monthly charge. Retries (attempts 2 to 6) and restarts after a failed month are excluded |
| Forter blocks | Orders that never succeeded and had at least one attempt blocked by Forter's pre-auth check (Spreedly message "gateway transaction not attempted due to failed pre authorization fraud check.") |
| Normal | The same metric over the 28 days before yesterday |
| Expected (SUB) | Σ yesterday's orders per recurring-order group × that group's 28-day rate, ÷ yesterday's orders. It adjusts for mix: 1st recurring orders approve at ~57%, 4th+ at ~82% |

## Scoring

A cell is coloured only when a move that bad would be unlikely by chance at yesterday's volume. It uses an exact one-sided binomial test against normal (expected for the SUB tile):

| Colour | Chance of noise | Equivalent |
|---|---|---|
| White | ≥ 2.3% | within normal range |
| Amber | < 2.3% | about 2 standard deviations |
| Red | < 0.13% | about 3 standard deviations |

Forter blocks are tested the other way: a rise is the warning. A method with no orders yesterday shows "no orders". Fixed pp thresholds were dropped because at 40 to 300 orders a day they coloured noise: about 2 to 3 red cells a day by chance under the old ±3pp rule.

## Methodology details

- **Source:** `cdc.PaymentTransactions_v` `LEFT JOIN spreedly.transaction_report_v` on `OrchestratorToken`; `TransactionType = 0`, `CAPTURE_FULL`, `Sum > 0`. Aligned with Redash #1613.
- **US only:** `cdc.OrdersNew_v.Country = 'United States of America'`. Non-US was 0.4% of BUY Paid, 3.9% of BUY Unpaid and 0.2% of SUB in Sep 2026.
- **Payment methods:** Card (real cards only), Apple Pay, PayPal, AfterPay.
  - AfterPay does not route through Spreedly, so it has no Spreedly record. Its outcome comes from `IsSuccessful`: 0 AfterPay orders were marked successful but unpaid in 28 days.
  - AfterPay has no Forter pre-auth flag.
  - Until 26 Sep 2026, AfterPay was counted inside Card (~30% of that column).
- **New customer:** the user has no paid order (`Status` 1/3, or 11/12 as `FS_STATIC` treats them) created before this order.
- **Excluded:**
  - TRY orders (`cdc.TbybOrders_v`, or any TRY auth in the window). The remaining TRY checkouts are old carts saved in customers' browsers.
  - PrepaidConverted orders.
- **SUB cycles:** a cycle that fails all 6 attempts restarts next month at `AttemptsAmount = 1` with the **same** `RecurringNumber`. Cycle 1 is therefore the first `AttemptsAmount = 1` order per (`SubscriptionId`, `RecurringNumber`), over full history.
  - `SubscriptionsRecurringOrders_v` has duplicate rows (~37% of ids) and is deduplicated.
  - SUB success is same-day billing success; failed orders enter dunning.

## Reconciliation with the Analysis report (24 Sep 2026, US)

| | Report Paid Media | Report Non-Paid |
|---|---|---|
| Completed here, same segment | 301 | 170 |
| Completed here as Paid, report Non-Paid (new-customer timing) | | 2 |
| Paid on 24 Sep after a decline on an earlier day (dated by first attempt here) | 2 | |
| Charged orders whose only item is a free gift (SitePart 12: SUB in payments data, BUY in the report) | | 15 |
| TRY order (excluded here) | | 1 |
| **Report total** | **303** | **188** |

Declined orders appear only here: 15 Paid, 5 Unpaid.

## Daily routine

Runs as the Claude Code Remote Routine **"Payments success rate monitoring"** (daily 04:30 UTC). The routine prompt references only the two file names. Each run:

1. Execute `payment method success rates by funnel.sql` via the BigQuery MCP (project `maelys-data`) and save the rows as JSON, e.g. `/tmp/bq_results.json`.
2. Run `python run_daily_payments.py /tmp/bq_results.json`. It renders `payments.png`, commits and pushes it, and posts a Slack `image` block referencing `https://raw.githubusercontent.com/yaelk-maker/Daily-payments-routines/<branch>/payments.png?v=<ts>`.

| Variable | Purpose |
|---|---|
| `SLACK_BOT_TOKEN` | Slack bot token with `chat:write` |
| `SLACK_CHANNEL_PAYMENTS` | Channel ID for `#payments-daily-monitoring` |

Slack `image` blocks need a public HTTPS URL, and the routine sandbox cannot reach `files.slack.com`. That is why the image is committed and served from `raw.githubusercontent.com`.

## Open items
- About 15 charged orders a day on SitePart 12 whose only item is a free gift: are they SUB or BUY? To confirm with the data team.
- `OrderDetails_v.FraudCheckStatus` (0/1/2) tracks payment outcome, not Forter's decision. Its meaning should be confirmed before any use.
