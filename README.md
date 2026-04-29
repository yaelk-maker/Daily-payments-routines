# Daily-payments-routines

Daily monitoring of the TRY payment funnel (Spreedly + PayPal), posted to `#payments-daily-monitoring`.

## Methodology

Aligned with **TBYB Success Rate Timeline** (Redash #1610).

| Choice | Detail |
|---|---|
| Source | `cdc.PaymentTransactions_v` LEFT JOIN `spreedly.transaction_report_v` on OrchestratorToken |
| TRY identification | `QUALIFY MAX(TransactionType) OVER (PARTITION BY OrderID) = 7` — order must have an auth |
| Zero-amount filter | `sum > 0` — excludes $0 transactions |
| Date attribution | Order's **first** transaction date (not per-transaction date) |
| Spreedly success | Prefers Spreedly's `Succeeded`/`Message`; falls back to `IsSuccessful` |
| PP AUTH_MODIFIED | Window-function detection: prior failed auth + lower amount |
| PP Shipping | CAPTURE_SHIPPING (Receipt < $10) + CAPTURE_FOLLOW_UP (last Receipt > $10 per order) |
| SPL Shipping denominator | CC fraud-blocked orders excluded (Apple Pay / PP: all attempts included) |

## How to run

1. Execute `try funnel daily monitoring.sql` in BigQuery (project: `maelys-data`).
2. Post results to `#payments-daily-monitoring` using the Slack message template below.

## Slack message template

```
**TRY Funnel Daily Monitoring — YYYY-MM-DD**

**AUTH — Spreedly**
| Period | Orders | AO Rate | Combined Rate |
|---|---|---|---|
| Yesterday | … | …% | …% |
| Last 7d | … | …% | …% |
| MTD | … | …% | …% |
| Prev month | … | …% | …% |

**AUTH — PayPal**
| Period | Orders | Auth Rate |
|---|---|---|
| Yesterday | … | …% |
| Last 7d | … | …% |
| MTD | … | …% |
| Prev month | … | …% |

**SHIPPING — Spreedly**
| Period | Orders | Success | Fraud Fail | Payment Fail |
|---|---|---|---|---|
| Yesterday | … | …% | …% | …% |
| Last 7d | … | …% | …% | …% |
| MTD | … | …% | …% | …% |
| Prev month | … | …% | …% | …% |

**SHIPPING — PayPal**
| Period | Orders | Success |
|---|---|---|
| Yesterday | … | …% |
| Last 7d | … | …% |
| MTD | … | …% |
| Prev month | … | …% |
```

Each table is kept narrow so it fits on a mobile screen without horizontal scrolling.
Note: SPL Shipping Fraud Fail will be ~0% — CC fraud-blocked orders are excluded from the denominator per #1610 methodology.
