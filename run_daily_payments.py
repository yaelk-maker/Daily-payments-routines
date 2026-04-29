#!/usr/bin/env python3
"""
Daily payments routine: runs the BigQuery query and posts results to Slack.

Usage:
    python run_daily_payments.py

Requirements:
    pip install google-cloud-bigquery
    SLACK_BOT_TOKEN and SLACK_CHANNEL_PAYMENTS env vars must be set.
    BigQuery auth via Application Default Credentials (gcloud auth application-default login)
    or GOOGLE_APPLICATION_CREDENTIALS env var pointing to a service account key.
"""

import json
import os
import urllib.request
from datetime import date, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from google.cloud import bigquery

PROJECT  = "maelys-data"
SQL_FILE = Path(__file__).parent / "payment method success rates by funnel.sql"
TZ_IL    = ZoneInfo("Asia/Jerusalem")

PERIOD_KEYS   = ["P4. Yesterday", "P3. Last 7d", "P2. MTD (excl yesterday)", "P1. Previous month"]
PERIOD_LABELS = {
    "P4. Yesterday":           "Yesterday  ",
    "P3. Last 7d":             "Last 7d    ",
    "P2. MTD (excl yesterday)":"MTD        ",
    "P1. Previous month":      "Prev month ",
}

FUNNELS = [
    ("TryAuth", "TRY — Auth (Combined AO+AM)"),
    ("TryShip", "TRY — Shipping"),
    ("Buy",     "BUY"),
    ("Sub",     "SUB (same-day billing)"),
]


def traffic_light(delta: float) -> str:
    if delta <= -3.0:
        return "🔴"
    elif delta <= -1.0:
        return "🟡"
    else:
        return "🟢"


def fmt_pct(val) -> str:
    return f"{val:.1f}%" if val is not None else " n/a "


def fmt_attempts(val) -> str:
    return f"{int(val):,}" if val is not None else "n/a"


def build_delta_line(y: dict, b: dict, prefix: str) -> str:
    """One-line delta summary: Δ vs 7d: Overall 🔴-2.5pp | CC 🟡-1.4pp | ..."""
    parts = []
    for col, label in [
        (f"{prefix}_Overall", "Overall"),
        (f"{prefix}_CC",      "CC"),
        (f"{prefix}_AP",      "AP"),
        (f"{prefix}_PP",      "PP"),
    ]:
        yv, bv = y.get(col), b.get(col)
        if yv is None or bv is None:
            continue
        delta = yv - bv
        sign  = "+" if delta >= 0 else ""
        parts.append(f"{label} {traffic_light(delta)}{sign}{delta:.1f}pp")
    return "Δ vs 7d:  " + "  |  ".join(parts)


def build_table(rows: dict, prefix: str) -> str:
    """Monospace pipe table for one funnel — goes inside a ``` code block."""
    header = f"{'Period':<12}| {'Attempts':>9} | {'Overall':>7} | {'CC':>6} | {'AP':>6} | {'PP':>6}"
    sep    = f"{'-'*12}|{'-'*11}|{'-'*9}|{'-'*8}|{'-'*8}|{'-'*8}"
    lines  = [header, sep]
    for key in PERIOD_KEYS:
        r = rows.get(key)
        if r is None:
            continue
        label = PERIOD_LABELS[key]
        att   = fmt_attempts(r.get(f"{prefix}_Total"))
        ov    = fmt_pct(r.get(f"{prefix}_Overall"))
        cc    = fmt_pct(r.get(f"{prefix}_CC"))
        ap    = fmt_pct(r.get(f"{prefix}_AP"))
        pp    = fmt_pct(r.get(f"{prefix}_PP"))
        lines.append(f"{label}| {att:>9} | {ov:>7} | {cc:>6} | {ap:>6} | {pp:>6}")
    return "\n".join(lines)


def build_blocks(rows: dict, report_date: str) -> list:
    yesterday = rows.get("P4. Yesterday", {})
    last7d    = rows.get("P3. Last 7d",   {})

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"Payment Success Rates — {report_date}"},
        }
    ]

    for prefix, title in FUNNELS:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*{title}*"},
        })
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": build_delta_line(yesterday, last7d, prefix)},
        })
        table = build_table(rows, prefix)
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"```{table}```"},
        })
        blocks.append({"type": "divider"})

    return blocks


def post_to_slack(blocks: list, fallback_text: str, channel: str, token: str):
    payload = json.dumps({
        "channel": channel,
        "text":    fallback_text,
        "blocks":  blocks,
    }).encode()
    req = urllib.request.Request(
        "https://slack.com/api/chat.postMessage",
        data=payload,
        headers={
            "Authorization":  f"Bearer {token}",
            "Content-Type":   "application/json; charset=utf-8",
        },
    )
    with urllib.request.urlopen(req) as resp:
        result = json.loads(resp.read())
    if not result.get("ok"):
        raise RuntimeError(f"Slack API error: {result.get('error')}")
    return result["ts"]


def main():
    token   = os.environ["SLACK_BOT_TOKEN"]
    channel = os.environ["SLACK_CHANNEL_PAYMENTS"]

    print("Running BigQuery query...")
    client  = bigquery.Client(project=PROJECT)
    sql     = SQL_FILE.read_text()
    rows_raw = list(client.query(sql).result())

    rows = {r["Period"]: dict(r) for r in rows_raw}
    if not rows:
        raise RuntimeError("Query returned no rows")

    report_date = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    blocks      = build_blocks(rows, report_date)
    fallback    = f"Payment Success Rates — {report_date}"

    print(f"Posting to Slack channel {channel}...")
    ts = post_to_slack(blocks, fallback, channel, token)
    print(f"Done. Message ts={ts}")


if __name__ == "__main__":
    main()
