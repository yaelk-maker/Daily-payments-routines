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
Block Kit payload: one fixed-width code block showing Yesterday / 7d / MTD
plus a `Δ Y vs 7d` row. Rates that move ≥ 2pp in the worse direction vs. the
7-day baseline are marked with a trailing `!` and called out above the table.

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

y, w, mtd = row("P4"), row("P3"), row("P2")

THRESHOLD_PP = 2.0
# metric key -> (header label, good_is_high)
METRICS = [
    ("Auth_Orders",              "Auth",   True),
    ("AO_Rate_Pct",              "AO%",    True),
    ("Combined_Auth_Rate_Pct",   "Comb%",  True),
    ("Shipping_Orders",          "Ship",   True),
    ("Shipping_Success_Pct",     "Succ%",  True),
    ("Shipping_Fraud_Fail_Pct",  "Fraud%", False),
    ("Shipping_Payment_Fail_Pct","Pay%",   False),
]
RATE_KEYS = {k for k,_,_ in METRICS if k.endswith("_Pct")}

def getf(r, k):
    if r is None: return None
    v = r.get(k)
    if v in (None, ""): return None
    return float(v)

def fmt(r, k):
    v = getf(r, k)
    if v is None: return "—"
    return f"{int(v):,}" if k.endswith("_Orders") else f"{v:.2f}"

def delta(cur, base):
    if cur is None or base is None: return None
    return cur - base

def is_flag(key, cur, base, good_is_high):
    d = delta(cur, base)
    if d is None: return False
    worse = (d < 0) if good_is_high else (d > 0)
    return abs(d) >= THRESHOLD_PP and worse

# Build table rows as lists of strings
WIDTHS = {"period": 12}
headers = ["Period"] + [label for _, label, _ in METRICS]
def build_row(label, r, flag_keys=None):
    flag_keys = flag_keys or set()
    cells = [label]
    for k, _, _ in METRICS:
        s = fmt(r, k)
        if k in flag_keys: s += "!"
        cells.append(s)
    return cells

# Flags on Yesterday row + delta row (same set)
flag_keys = set()
for k, _, good_is_high in METRICS:
    if k not in RATE_KEYS: continue
    if is_flag(k, getf(y, k), getf(w, k), good_is_high):
        flag_keys.add(k)

delta_cells = ["Δ Y vs 7d"]
for k, _, _ in METRICS:
    if k not in RATE_KEYS:
        delta_cells.append("—")
        continue
    d = delta(getf(y, k), getf(w, k))
    s = "—" if d is None else f"{d:+.2f}"
    if k in flag_keys: s += "!"
    delta_cells.append(s)

table_rows = [
    headers,
    build_row("Yesterday",   y, flag_keys),
    build_row("7d baseline", w),
    build_row("MTD",         mtd),
    delta_cells,
]

# Column widths: max of any cell, with sensible minima
col_widths = [max(len(r[i]) for r in table_rows) for i in range(len(headers))]
def render_row(cells):
    # first col left-aligned, rest right-aligned
    out = [cells[0].ljust(col_widths[0])]
    out += [cells[i].rjust(col_widths[i]) for i in range(1, len(cells))]
    return "  ".join(out)

table = "\n".join(render_row(r) for r in table_rows)

# Headline line above the block
flag_labels = {
    "AO_Rate_Pct": "AO",
    "Combined_Auth_Rate_Pct": "Combined auth",
    "Shipping_Success_Pct": "Shipping success",
    "Shipping_Fraud_Fail_Pct": "Shipping fraud-fail",
    "Shipping_Payment_Fail_Pct": "Shipping payment-fail",
}
if flag_keys:
    parts = []
    for k in flag_keys:
        d = delta(getf(y, k), getf(w, k))
        parts.append(f"{flag_labels[k]} {d:+.2f}pp")
    headline = f":rotating_light: *{len(flag_keys)} flag(s) vs 7d:* " + "; ".join(parts)
else:
    headline = ":white_check_mark: No metrics crossed the 2pp threshold vs. 7d."

body = (
    f"*TRY funnel — daily monitoring* · {run_date} (Asia/Jerusalem)\n"
    f"{headline}\n"
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
