# Routine: TRY funnel daily monitoring

You are running a scheduled daily check. Execute the steps below **in order** and
stop at the first hard failure (report what failed and which step). Do not skip
steps. Do not ask the user questions — this is an unattended run.

## Inputs (read from env, fail fast if missing)
- `SLACK_BOT_TOKEN` — Slack bot OAuth token (`xoxb-...`)
- `SLACK_CHANNEL_ID` — target channel (default `C0AVBTM4BG8`)
- `BQ_PROJECT` — BigQuery billing project (default `maelys-data`)

If any are missing, post a short failure message to `SLACK_CHANNEL_ID` (if the
token is available) and exit with a note in the end-of-turn summary.

## Step 1 — Run the query

Execute `queries/try_funnel_daily_monitoring.sql` against BigQuery and capture
the result as JSON. Use the `bq` CLI:

```bash
RUN_DATE=$(TZ=Asia/Jerusalem date +%F)
OUT="logs/${RUN_DATE}_try_funnel.json"
bq query \
  --project_id="${BQ_PROJECT:-maelys-data}" \
  --use_legacy_sql=false \
  --format=json \
  --max_rows=10 \
  < queries/try_funnel_daily_monitoring.sql \
  > "$OUT"
```

If `bq` returns non-zero, read the stderr, post it to Slack as a failure
message (see Step 3 formatter, `mode=error`), commit `$OUT` (even if empty) to
the log, and stop.

Expected shape: 4 rows, ordered `P4. Yesterday` → `P1. Previous month`, with
columns `Period, Auth_Orders, AO_Rate_Pct, Combined_Auth_Rate_Pct,
Shipping_Orders, Shipping_Success_Pct, Shipping_Fraud_Fail_Pct,
Shipping_Payment_Fail_Pct`.

## Step 2 — Build the Slack message

Use the inline Python below (no extra deps) to turn the JSON into a Slack
Block Kit payload. It also computes a delta vs. the 7-day baseline for the
headline metrics and flags regressions with a warning emoji when the gap is
≥ 2 percentage points.

```bash
python3 - "$OUT" "$RUN_DATE" > logs/"${RUN_DATE}_try_funnel.slack.json" <<'PY'
import json, sys, os
rows = json.load(open(sys.argv[1]))
run_date = sys.argv[2]

def row(period_prefix):
    for r in rows:
        if r["Period"].startswith(period_prefix):
            return r
    return None

def pct(x):
    return "—" if x in (None, "") else f"{float(x):.2f}%"

def num(x):
    return "—" if x in (None, "") else f"{int(float(x)):,}"

y  = row("P4")  # Yesterday
w  = row("P3")  # 7d
mtd = row("P2") # MTD
pm  = row("P1") # Previous month

def delta_flag(cur, base, good_is_high=True, threshold=2.0):
    if cur is None or base is None or cur == "" or base == "":
        return ""
    d = float(cur) - float(base)
    worse = (d < 0) if good_is_high else (d > 0)
    if abs(d) >= threshold and worse:
        return f" :warning: ({d:+.2f}pp)"
    return f" ({d:+.2f}pp)"

header = f":bar_chart: *TRY funnel — daily monitoring* · {run_date} (Asia/Jerusalem)"

def line(label, yval, wval, good_is_high=True):
    flag = delta_flag(yval, wval, good_is_high=good_is_high)
    return f"*{label}* · Yesterday: `{pct(yval)}`{flag} · 7d: `{pct(wval)}` · MTD: `{pct(mtd and mtd.get(label.replace(' ','_')))}`"

blocks = [
    {"type": "header", "text": {"type": "plain_text", "text": "TRY funnel — daily monitoring"}},
    {"type": "context", "elements": [{"type": "mrkdwn", "text": f"Run date: *{run_date}* · Timezone: Asia/Jerusalem · Source: `maelys-data.spreedly.transactions_s`"}]},
    {"type": "section", "fields": [
        {"type": "mrkdwn", "text": f"*Auth orders (Y)*\n{num(y and y['Auth_Orders'])}"},
        {"type": "mrkdwn", "text": f"*Shipping orders (Y)*\n{num(y and y['Shipping_Orders'])}"},
    ]},
    {"type": "section", "text": {"type": "mrkdwn", "text":
        f"*AO rate* · Y `{pct(y and y['AO_Rate_Pct'])}`{delta_flag(y and y['AO_Rate_Pct'], w and w['AO_Rate_Pct'])} · 7d `{pct(w and w['AO_Rate_Pct'])}` · MTD `{pct(mtd and mtd['AO_Rate_Pct'])}` · PrevM `{pct(pm and pm['AO_Rate_Pct'])}`"}},
    {"type": "section", "text": {"type": "mrkdwn", "text":
        f"*Combined auth rate* · Y `{pct(y and y['Combined_Auth_Rate_Pct'])}`{delta_flag(y and y['Combined_Auth_Rate_Pct'], w and w['Combined_Auth_Rate_Pct'])} · 7d `{pct(w and w['Combined_Auth_Rate_Pct'])}` · MTD `{pct(mtd and mtd['Combined_Auth_Rate_Pct'])}` · PrevM `{pct(pm and pm['Combined_Auth_Rate_Pct'])}`"}},
    {"type": "section", "text": {"type": "mrkdwn", "text":
        f"*Shipping success* · Y `{pct(y and y['Shipping_Success_Pct'])}`{delta_flag(y and y['Shipping_Success_Pct'], w and w['Shipping_Success_Pct'])} · 7d `{pct(w and w['Shipping_Success_Pct'])}` · MTD `{pct(mtd and mtd['Shipping_Success_Pct'])}` · PrevM `{pct(pm and pm['Shipping_Success_Pct'])}`"}},
    {"type": "section", "text": {"type": "mrkdwn", "text":
        f"*Shipping fraud-fail* · Y `{pct(y and y['Shipping_Fraud_Fail_Pct'])}`{delta_flag(y and y['Shipping_Fraud_Fail_Pct'], w and w['Shipping_Fraud_Fail_Pct'], good_is_high=False)} · 7d `{pct(w and w['Shipping_Fraud_Fail_Pct'])}`"}},
    {"type": "section", "text": {"type": "mrkdwn", "text":
        f"*Shipping payment-fail* · Y `{pct(y and y['Shipping_Payment_Fail_Pct'])}`{delta_flag(y and y['Shipping_Payment_Fail_Pct'], w and w['Shipping_Payment_Fail_Pct'], good_is_high=False)} · 7d `{pct(w and w['Shipping_Payment_Fail_Pct'])}`"}},
    {"type": "context", "elements": [{"type": "mrkdwn", "text": "Thresholds: `:warning:` flagged when a rate moves ≥ 2pp in the worse direction vs. the 7-day baseline."}]},
]

payload = {
    "channel": os.environ["SLACK_CHANNEL_ID"],
    "text": f"TRY funnel — daily monitoring · {run_date}",
    "blocks": blocks,
}
print(json.dumps(payload))
PY
```

## Step 3 — Post to Slack

```bash
curl -sS -X POST https://slack.com/api/chat.postMessage \
  -H "Authorization: Bearer ${SLACK_BOT_TOKEN}" \
  -H "Content-Type: application/json; charset=utf-8" \
  --data @logs/"${RUN_DATE}_try_funnel.slack.json" \
  | tee logs/"${RUN_DATE}_try_funnel.slack_response.json" \
  | python3 -c 'import sys,json; r=json.load(sys.stdin); sys.exit(0 if r.get("ok") else 1)'
```

If the response `ok` is false, the exit code above is non-zero. Read the error
field from `*.slack_response.json`, include it in your end-of-turn summary, and
still proceed to commit the logs so we have a trail.

## Step 4 — Commit the run log

Commit the three log files produced above. Use the exact message format:

```bash
git add logs/"${RUN_DATE}"_try_funnel.json \
        logs/"${RUN_DATE}"_try_funnel.slack.json \
        logs/"${RUN_DATE}"_try_funnel.slack_response.json
git commit -m "logs: try funnel monitoring ${RUN_DATE}"
git push -u origin claude/payments-monitoring-bigquery-slack-bxrHo
```

If there is nothing to commit (re-run on same day), skip the commit — do not
create an empty commit.

## End-of-turn summary

One or two sentences: run date, post success (yes/no), any `:warning:` flags
you noticed in the output, log file path. Nothing else.
