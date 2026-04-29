# Daily-payments-routines

Daily monitoring of the TRY payment funnel (Auth + Shipping), posted to `#payments-daily-monitoring`.

## How to run

1. Execute `try funnel daily monitoring.sql` in BigQuery (project: `maelys-data`).
2. Post results to `#payments-daily-monitoring` using the Slack message template below.

## Slack message template

```
**TRY Funnel Daily Monitoring — YYYY-MM-DD**

**AUTH**
| Period | Orders | AO Rate | Combined Rate |
|---|---|---|---|
| Yesterday | … | …% | …% |
| Last 7d | … | …% | …% |
| MTD | … | …% | …% |
| Prev month | … | …% | …% |

**SHIPPING**
| Period | Orders | Success | Fraud Fail | Payment Fail |
|---|---|---|---|---|
| Yesterday | … | …% | …% | …% |
| Last 7d | … | …% | …% | …% |
| MTD | … | …% | …% | …% |
| Prev month | … | …% | …% | …% |
```

The AUTH and SHIPPING tables are kept separate so each fits on a mobile screen without horizontal scrolling.
