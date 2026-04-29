# Daily-payments-routines

Daily monitoring of the TRY payment funnel (Spreedly + PayPal), posted to `#payments-daily-monitoring`.

## Sources

| Provider | Table | Auth stage | Shipping stage |
|---|---|---|---|
| Spreedly | `maelys-data.spreedly.transactions_s` | AUTH_ORIGINAL + AUTH_MODIFIED | CAPTURE_SHIPPING |
| PayPal | `cdc.PaymentTransactions_v` (EcType `PayPal*`) | TransactionType=7 (Authorize) | TransactionType=0 (Receipt) < $10 |

PayPal TRY orders are identified via `cdc.OrdersNew_v` (OrderType=1, SitePart NOT IN (10,12)).

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
PayPal shipping has no fraud split (no fraud declines for PP).
