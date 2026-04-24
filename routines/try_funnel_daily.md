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
Shipping_Payment_Fail_Pct`. We use P4 / P3 / P2 only (Previous month is
dropped from the Slack summary — it's still in the log file).

## Step 2 — Build the Slack message

Use the inline Python below (no extra deps) to turn the JSON into a Slack
Block Kit payload: a compact body with three parts —

1. **Headline** with the run date.
2. **Highlights** section (rendered with bold mrkdwn, so it pops): one bullet
   per rate that moved ≥ 2pp in the worse direction vs. the 7-day baseline.
   If nothing crossed the threshold, a green-check confirmation line instead.
3. **Table** in a fixed-width code block (metrics as rows, `Y` / `7d` as
   columns, plus a `Δ` column). Rate values carry `%`; deltas carry `pp`.
   MTD is dropped from the Slack message to keep the table phone-friendly —
   it's still in the committed log JSON.

```bash
python3 - "$OUT" "$RUN_DATE" > logs/"${RUN_DATE}_try_funnel.slack.json" <<'PY'
import json, sys, os
rows = json.load(open(sys.argv[1]))
run_date = sys.argv[2]

def row(prefix):
    for r in rows:
        if r["Period"].startswith(prefix):
            return r
    return None

y, w = row("P4"), row("P3")

THRESHOLD_PP = 2.0
# (SQL column, short label for table, full label for highlights, good_is_high, is_rate)
METRICS = [
    ("Auth_Orders",              "Auth orders", "Auth orders",        True,  False),
    ("AO_Rate_Pct",              "AO rate",     "AO rate",            True,  True),
    ("Combined_Auth_Rate_Pct",   "Comb auth",   "Combined auth rate", True,  True),
    ("Shipping_Orders",          "Ship orders", "Shipping orders",    True,  False),
    ("Shipping_Success_Pct",     "Ship succ",   "Shipping success",   True,  True),
    ("Shipping_Fraud_Fail_Pct",  "Fraud %",     "Fraud %",            False, True),
    ("Shipping_Payment_Fail_Pct","Pay Fail %",  "Pay Fail %",         False, True),
]
PERIODS = [("Y", y), ("7d", w)]

def getf(r, k):
    if r is None: return None
    v = r.get(k)
    return None if v in (None, "") else float(v)

def fmt_val(v, is_rate):
    if v is None: return "—"
    return f"{v:.2f}%" if is_rate else f"{int(v):,}"

def fmt_delta(d):
    return "—" if d is None else f"{d:+.2f}pp"

def delta(cur, base):
    return None if (cur is None or base is None) else cur - base

def is_flag(cur, base, good_is_high, is_rate):
    if not is_rate: return False
    d = delta(cur, base)
    if d is None: return False
    worse = (d < 0) if good_is_high else (d > 0)
    return abs(d) >= THRESHOLD_PP and worse

# Build table + collect flags
headers = ["Metric"] + [name for name, _ in PERIODS] + ["Δ"]
table_rows = [headers]
flags = []  # list of (full_label, y_val, w_val, d, is_rate)

for key, short, full, good_is_high, is_rate in METRICS:
    cy, cw = getf(y, key), getf(w, key)
    flagged = is_flag(cy, cw, good_is_high, is_rate)
    if flagged:
        flags.append((full, cy, cw, delta(cy, cw)))

    cells = [short]
    for _, r in PERIODS:
        s = fmt_val(getf(r, key), is_rate)
        if r is y and flagged: s += "!"
        cells.append(s)
    if is_rate:
        s = fmt_delta(delta(cy, cw))
        if flagged: s += "!"
    else:
        s = "—"
    cells.append(s)
    table_rows.append(cells)

col_widths = [max(len(r[i]) for r in table_rows) for i in range(len(headers))]
def render_row(cells):
    out = [cells[0].ljust(col_widths[0])]
    out += [cells[i].rjust(col_widths[i]) for i in range(1, len(cells))]
    return "  ".join(out)
table = "\n".join(render_row(r) for r in table_rows)

# Highlights block (mrkdwn bold — renders outside the code block)
if flags:
    header = f":rotating_light: *{len(flags)} significant change(s) vs 7d baseline:*"
    bullets = "\n".join(
        f"• *{name}:* Yday `{fy:.2f}%` vs 7d `{fw:.2f}%` → *{d:+.2f}pp*"
        for (name, fy, fw, d) in flags
    )
    highlights = f"{header}\n{bullets}"
else:
    highlights = ":white_check_mark: No metrics crossed the 2pp threshold vs. 7d."

body = (
    f"*TRY funnel — daily monitoring* · {run_date} (Asia/Jerusalem)\n"
    f"{highlights}\n"
    f"```\n{table}\n```\n"
    f"_`!` marks a rate moving ≥ 2pp in the worse direction vs. the 7-day baseline. "
    f"Source: `maelys-data.spreedly.transactions_s`._"
)

payload = {
    "channel": os.environ["SLACK_CHANNEL_ID"],
    "text": f"TRY funnel — daily monitoring · {run_date}",
    "blocks": [
        {"type": "section", "text": {"type": "mrkdwn", "text": body}},
    ],
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
