#!/usr/bin/env python3
"""
Daily payments routine: BQ results → styled PNG → GitHub raw URL → Slack post.

Designed to run inside Claude Code Remote Routine. Two-step flow:

  1. The routine executes ``payment method success rates by funnel.sql`` via the
     BigQuery MCP and saves the rows as JSON.

  2. This script reads that JSON, generates ``payments.png`` matching the
     ``#payments-daily-monitoring`` template, commits + pushes the image to the
     current branch, and posts a Slack message with an ``image`` block pointing
     at ``raw.githubusercontent.com``.

Required environment variables:
  SLACK_BOT_TOKEN          — bot token with chat:write
  SLACK_CHANNEL_PAYMENTS   — channel ID for #payments-daily-monitoring

Usage:
  python run_daily_payments.py <bq_results.json>
  cat bq_results.json | python run_daily_payments.py -

Expected JSON shape — a list of period rows. Each row has a ``Period`` key
("P4. Yesterday", "P3. Last 7d", "P2. MTD (excl yesterday)", "P1. Previous month")
and the columns produced by the SQL:
  TryAuth_Total / TryAuth_Overall / TryAuth_CC / TryAuth_AP / TryAuth_PP
  TryShip_Total / TryShip_Overall / TryShip_CC / TryShip_AP / TryShip_PP
  Buy_Total     / Buy_Overall     / Buy_CC     / Buy_AP     / Buy_PP
  Sub_Total     / Sub_Overall     / Sub_CC     / Sub_AP     / Sub_PP
"""

import json
import os
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt

REPO_DIR          = Path(__file__).parent.resolve()
IMAGE_PATH        = REPO_DIR / "payments.png"
GITHUB_OWNER_REPO = "yaelk-maker/Daily-payments-routines"

PERIODS = ["Yesterday", "Last 7d", "MTD", "Prev month"]
PERIOD_FROM_KEY = {
    "P4. Yesterday":            "Yesterday",
    "P3. Last 7d":              "Last 7d",
    "P2. MTD (excl yesterday)": "MTD",
    "P1. Previous month":       "Prev month",
}
METRICS = [("Overall", "Overall"), ("CC", "CC"), ("AP", "Apple Pay"), ("PP", "PayPal")]
FUNNELS = [
    ("TryAuth", "TRY Auth"),
    ("TryShip", "TRY Shipping"),
    ("Buy",     "BUY"),
    ("Sub",     "SUB"),
]

# Pink/coral palette
HEADER_BG      = "#F8C8D0"
PERIOD_BG      = "#FBDDE0"
YEST_PERIOD_BG = "#F08A95"
YEST_PERIOD_TX = "#C5283D"
GREEN_BG, YELLOW_BG, RED_BG = "#A8E0A0", "#FFE99C", "#F5C6CB"
GREEN_TX, RED_TX            = "#1F7A1F", "#C5283D"


def bg_for(delta: float) -> str:
    if delta <= -3.0:
        return RED_BG
    if delta <= -1.0:
        return YELLOW_BG
    return GREEN_BG


def tx_for(delta: float) -> str:
    return RED_TX if delta < -0.5 else GREEN_TX


def render_funnel(ax, prefix: str, short_title: str, rows: dict) -> None:
    ax.axis("off")
    yest_total = rows["Yesterday"][f"{prefix}_Total"]
    title = f"{short_title} - {yest_total:,} attempts yesterday"
    ax.text(0.5, 1.05, title, ha="center", va="bottom",
            transform=ax.transAxes, fontsize=12, fontweight="bold", color="#000")
    ax.plot([0.18, 0.82], [1.02, 1.02], color="#000", linewidth=1.4,
            transform=ax.transAxes, clip_on=False)

    col_labels = ["Period"] + [m[1] for m in METRICS] + ["Δ Overall vs 7d"]
    cell_text, cell_colors = [], []
    overall_delta = rows["Yesterday"][f"{prefix}_Overall"] - rows["Last 7d"][f"{prefix}_Overall"]

    for period in PERIODS:
        r = rows[period]
        is_yest = (period == "Yesterday")
        row_vals = [period]
        row_colors = [YEST_PERIOD_BG if is_yest else PERIOD_BG]
        for code, _ in METRICS:
            v = r[f"{prefix}_{code}"]
            row_vals.append(f"{v:.1f}%")
            if is_yest:
                d = v - rows["Last 7d"][f"{prefix}_{code}"]
                row_colors.append(bg_for(d))
            else:
                row_colors.append("white")
        if is_yest:
            sign = "+" if overall_delta >= 0 else ""
            row_vals.append(f"{sign}{overall_delta:.1f}pp")
            row_colors.append(bg_for(overall_delta))
        else:
            row_vals.append("")
            row_colors.append("white")
        cell_text.append(row_vals)
        cell_colors.append(row_colors)

    tbl = ax.table(
        cellText=cell_text, colLabels=col_labels,
        cellColours=cell_colors,
        colColours=[HEADER_BG] * len(col_labels),
        cellLoc="center", colLoc="center",
        colWidths=[0.13, 0.13, 0.12, 0.15, 0.13, 0.20],
        bbox=[0.0, 0.0, 1.0, 1.0],
    )
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(10)

    for i in range(len(col_labels)):
        h = tbl[(0, i)]
        h.get_text().set_color("#000")
        h.get_text().set_fontweight("bold")

    yc = tbl[(1, 0)]
    yc.get_text().set_color(YEST_PERIOD_TX)
    yc.get_text().set_fontweight("bold")

    dc = tbl[(1, len(col_labels) - 1)]
    dc.get_text().set_color(tx_for(overall_delta))
    dc.get_text().set_fontweight("bold")

    for cell in tbl.get_celld().values():
        cell.set_edgecolor("#FFF")
        cell.set_linewidth(1.5)


def generate_image(rows: dict, report_date: str, out_path: Path) -> None:
    fig = plt.figure(figsize=(8.5, 9.5))
    fig.text(0.5, 0.985, f"Payment Success Rates - {report_date}",
             ha="center", va="top", fontsize=20, fontweight="bold", color="#000")

    y = 0.910
    fig.text(0.235, y, "Delta vs Last 7d:", ha="left", va="center", fontsize=10, color="#444")
    fig.add_artist(mpatches.Rectangle((0.395, y - 0.007), 0.020, 0.014,
                                      facecolor=GREEN_BG, edgecolor="none", transform=fig.transFigure))
    fig.text(0.420, y, "stable / up", ha="left", va="center", fontsize=10)
    fig.add_artist(mpatches.Rectangle((0.530, y - 0.007), 0.020, 0.014,
                                      facecolor=YELLOW_BG, edgecolor="none", transform=fig.transFigure))
    fig.text(0.555, y, "-1 to -3pp", ha="left", va="center", fontsize=10)
    fig.add_artist(mpatches.Rectangle((0.665, y - 0.007), 0.020, 0.014,
                                      facecolor=RED_BG, edgecolor="none", transform=fig.transFigure))
    fig.text(0.690, y, "> -3pp drop", ha="left", va="center", fontsize=10)

    gs = fig.add_gridspec(4, 1, top=0.81, bottom=0.02, hspace=0.45)
    for i, (prefix, short_title) in enumerate(FUNNELS):
        ax = fig.add_subplot(gs[i, 0])
        render_funnel(ax, prefix, short_title, rows)

    plt.savefig(out_path, dpi=170, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def parse_rows(raw_rows: list) -> dict:
    out = {}
    for r in raw_rows:
        period = PERIOD_FROM_KEY.get(r.get("Period"), r.get("Period"))
        out[period] = r
    missing = set(PERIODS) - set(out)
    if missing:
        raise SystemExit(f"BQ results missing periods: {sorted(missing)}")
    return out


def git_commit_push(message: str) -> str:
    branch = subprocess.check_output(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=REPO_DIR,
    ).decode().strip()
    subprocess.run(["git", "add", "payments.png"], cwd=REPO_DIR, check=True)
    subprocess.run(["git", "commit", "-m", message], cwd=REPO_DIR, check=True)
    for attempt, delay in enumerate([0, 2, 4, 8, 16]):
        if delay:
            time.sleep(delay)
        result = subprocess.run(["git", "push", "-u", "origin", branch], cwd=REPO_DIR)
        if result.returncode == 0:
            return branch
    raise RuntimeError("git push failed after retries")


def post_to_slack(image_url: str, report_date: str) -> str:
    token   = os.environ["SLACK_BOT_TOKEN"]
    channel = os.environ["SLACK_CHANNEL_PAYMENTS"]
    title   = f"Payment Success Rates - {report_date}"
    payload = json.dumps({
        "channel": channel,
        "text":    title,
        "blocks":  [{"type": "image", "image_url": image_url, "alt_text": title,
                     "title": {"type": "plain_text", "text": title}}],
    }).encode()
    req = urllib.request.Request(
        "https://slack.com/api/chat.postMessage", data=payload,
        headers={"Authorization":  f"Bearer {token}",
                 "Content-Type":   "application/json; charset=utf-8"},
    )
    with urllib.request.urlopen(req) as resp:
        result = json.loads(resp.read())
    if not result.get("ok"):
        raise RuntimeError(f"Slack error: {result.get('error')}")
    return result["ts"]


def yesterday_date_il() -> str:
    try:
        from zoneinfo import ZoneInfo
        return (datetime.now(ZoneInfo("Asia/Jerusalem")).date() - timedelta(days=1)).isoformat()
    except Exception:
        return (datetime.utcnow().date() - timedelta(days=1)).isoformat()


def main() -> None:
    if len(sys.argv) != 2:
        sys.exit("Usage: run_daily_payments.py <bq_results.json | ->")
    raw = sys.stdin.read() if sys.argv[1] == "-" else Path(sys.argv[1]).read_text()
    rows = parse_rows(json.loads(raw))

    report_date = yesterday_date_il()
    print(f"Generating image -> {IMAGE_PATH}")
    generate_image(rows, report_date, IMAGE_PATH)

    print("Committing and pushing image...")
    branch = git_commit_push(f"Daily payments report — {report_date}")

    img_url = (f"https://raw.githubusercontent.com/{GITHUB_OWNER_REPO}/{branch}"
               f"/payments.png?v={int(time.time())}")
    print(f"Posting to Slack: {img_url}")
    ts = post_to_slack(img_url, report_date)
    print(f"Done. Slack ts={ts}")


if __name__ == "__main__":
    main()
