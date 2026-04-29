# Daily-payments-routines

Daily monitoring of payment success rates across funnels (TRY, BUY, SUB) by payment method, posted to `#payments-daily-monitoring`.

## Files

| File | Purpose |
|---|---|
| `payment method success rates by funnel.sql` | **Routine query.** High-level success rates for TRY Auth, TRY Shipping, BUY, SUB by payment method (CC / Apple Pay / PayPal). Run daily and post to Slack. |
| `try funnel daily monitoring.sql` | **Deep-dive query.** Detailed TRY-only breakdown — Spreedly vs PayPal, with AO-vs-Combined Auth split and Fraud-vs-Payment-Fail Shipping split. Run on demand when investigating TRY anomalies; not part of the daily routine. |

## Methodology

Aligned with the canonical Redash queries:

| Funnel | Reference | Notes |
|---|---|---|
| TRY Auth, TRY Shipping | Redash #1610 (TBYB Success Rate Timeline) | QUALIFY MAX(TransactionType)=7 for TRY identification; PP AUTH_MODIFIED via window function; PP shipping includes CAPTURE_FOLLOW_UP; CC fraud-blocked excluded from shipping denominator |
| BUY, SUB | Redash #1613 (BUY Success Rate Timeline) | TransactionType=0 / CAPTURE_FULL; SUB = SitePart IN (10,12) or Spreedly Metadata_order_type='SUB'; CC fraud-blocked excluded |

Common across all funnels:
- Source: `cdc.PaymentTransactions_v` LEFT JOIN `spreedly.transaction_report_v` on OrchestratorToken
- `sum > 0` filter (zero-amount transactions excluded)
- Period attribution: order's first transaction date (Asia/Jerusalem)
- Order-level deduplication (MAX over flags)

**Note on SUB:** the rate reflects same-day billing success. Orders that fail same-day enter dunning and may succeed on subsequent days, so the SUB rate here is a leading indicator for anomaly detection — not a final renewal rate.

## How to run the routine

1. Execute `payment method success rates by funnel.sql` in BigQuery (project: `maelys-data`).
2. Post results to `#payments-daily-monitoring` using the Slack template below.

## Slack message template (routine)

Each funnel is its own narrow table so it fits on a mobile screen.

```
**Payment Success Rates — YYYY-MM-DD**

**TRY — Auth (Combined AO+AM)**
| Period | CC | Apple Pay | PayPal |
|---|---|---|---|
| Yesterday | …% | …% | …% |
| Last 7d | …% | …% | …% |
| MTD | …% | …% | …% |
| Prev month | …% | …% | …% |

**TRY — Shipping**
| Period | CC | Apple Pay | PayPal |
|---|---|---|---|
| Yesterday | …% | …% | …% |
| Last 7d | …% | …% | …% |
| MTD | …% | …% | …% |
| Prev month | …% | …% | …% |

**BUY**
| Period | CC | Apple Pay | PayPal |
|---|---|---|---|
| Yesterday | …% | …% | …% |
| Last 7d | …% | …% | …% |
| MTD | …% | …% | …% |
| Prev month | …% | …% | …% |

**SUB (same-day billing)**
| Period | CC | Apple Pay | PayPal |
|---|---|---|---|
| Yesterday | …% | …% | …% |
| Last 7d | …% | …% | …% |
| MTD | …% | …% | …% |
| Prev month | …% | …% | …% |
```

## Deep-dive (on demand)

When the routine flags a TRY anomaly, run `try funnel daily monitoring.sql` for the AO-vs-AM split and the Fraud-vs-Payment-Fail breakdown — useful for diagnosing whether a TRY drop is driven by issuer declines, fraud rules, or PSP issues.
