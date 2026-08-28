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
  BuyReg_Total  / BuyReg_Overall  / BuyReg_CC  / BuyReg_AP  / BuyReg_PP
  Prepaid_Total / Prepaid_Rate    / Prepaid_Share / Buy_Blended_Overall
  Sub_Total     / Sub_Overall     / Sub_CC     / Sub_AP     / Sub_PP

BUY is split: BuyReg_* excludes PrepaidConverted orders (payments-health
signal, ~95-96% baseline); the prepaid-converted pool (TRY->BUY reroute,
~19-27% approval, volume surges every month start) gets its own table with an
INVERTED traffic light on its share of BUY — rising share is the warning.
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
    ("TryAuth", "TRY Auth",                      None),
    ("TryShip", "TRY Shipping",                  None),
    ("BuyReg",  "BUY — excl. prepaid-converted", None),
    ("PREPAID", "PREPAID CONVERTED",             None),   # rendered by render_prepaid
    ("Sub",     "SUB",                           "first attempt only"),
]

# MAËLYS brand palette (semantic traffic-light colors kept for status)
INK            = "#120D0E"
PAGE_BG        = "#FBFAF8"
HEADER_BG      = "#FFAFC4"
PERIOD_BG      = "#FFE0E9"
YEST_PERIOD_BG = "#DB6B8A"
YEST_PERIOD_TX = "#FBFAF8"
NEUTRAL_TX     = "#5A524D"
GREEN_BG, YELLOW_BG, RED_BG = "#A8E0A0", "#FFE99C", "#F5C6CB"
GREEN_TX, RED_TX            = "#1F7A1F", "#C5283D"


def bg_for(delta: float) -> str:
    if delta <= -3.0:
        return RED_BG
    if delta <= -1.0:
        return YELLOW_BG
    return GREEN_BG


def bg_for_share(delta: float) -> str:
    """Inverted thresholds for the prepaid pool: a RISING share is the warning."""
    if delta >= 5.0:
        return RED_BG
    if delta >= 2.0:
        return YELLOW_BG
    return GREEN_BG


def tx_for(delta: float, invert: bool = False) -> str:
    bad = delta > 0.5 if invert else delta < -0.5
    return RED_TX if bad else GREEN_TX


def _style_table(tbl, n_cols: int, yest_delta_tx: str) -> None:
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(10)
    for i in range(n_cols):
        h = tbl[(0, i)]
        h.get_text().set_color(INK)
        h.get_text().set_fontweight("bold")
    yc = tbl[(1, 0)]
    yc.get_text().set_color(YEST_PERIOD_TX)
    yc.get_text().set_fontweight("bold")
    dc = tbl[(1, n_cols - 1)]
    dc.get_text().set_color(yest_delta_tx)
    dc.get_text().set_fontweight("bold")
    for cell in tbl.get_celld().values():
        cell.set_edgecolor(PAGE_BG)
        cell.set_linewidth(1.5)


def _title(ax, text: str) -> None:
    ax.axis("off")
    ax.text(0.5, 1.05, text, ha="center", va="bottom",
            transform=ax.transAxes, fontsize=12, fontweight="bold", color=INK)
    ax.plot([0.18, 0.82], [1.02, 1.02], color=INK, linewidth=1.4,
            transform=ax.transAxes, clip_on=False)


def render_funnel(ax, prefix: str, short_title: str, rows: dict, note: str = None) -> None:
    yest_total = rows["Yesterday"][f"{prefix}_Total"]
    title = f"{short_title} - {yest_total:,} attempts yesterday"
    if note:
        title += f"  ({note})"
    _title(ax, title)

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
    _style_table(tbl, len(col_labels), tx_for(overall_delta))


def render_prepaid(ax, rows: dict) -> None:
    """Prepaid-converted pool (TRY→BUY reroute). Inverted traffic light: the
    warning signal is a RISING share of BUY, not a falling success rate."""
    _title(ax, f"PREPAID CONVERTED (TRY→BUY reroute) - "
               f"{rows['Yesterday']['Prepaid_Total']:,} orders yesterday")

    col_labels = ["Period", "Orders", "Share of BUY", "Success rate", "Δ Share vs 7d"]
    share_delta = rows["Yesterday"]["Prepaid_Share"] - rows["Last 7d"]["Prepaid_Share"]
    cell_text, cell_colors = [], []
    for period in PERIODS:
        r = rows[period]
        is_yest = (period == "Yesterday")
        row_vals = [period, f"{r['Prepaid_Total']:,}",
                    f"{r['Prepaid_Share']:.1f}%", f"{r['Prepaid_Rate']:.1f}%"]
        row_colors = [YEST_PERIOD_BG if is_yest else PERIOD_BG, "white",
                      bg_for_share(share_delta) if is_yest else "white", "white"]
        if is_yest:
            sign = "+" if share_delta >= 0 else ""
            row_vals.append(f"{sign}{share_delta:.1f}pp")
            row_colors.append(bg_for_share(share_delta))
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
        colWidths=[0.16, 0.15, 0.19, 0.19, 0.21],
        bbox=[0.0, 0.0, 1.0, 1.0],
    )
    _style_table(tbl, len(col_labels), tx_for(share_delta, invert=True))

    blended_y = rows["Yesterday"]["Buy_Blended_Overall"]
    blended_7 = rows["Last 7d"]["Buy_Blended_Overall"]
    ax.text(0.5, -0.13,
            f"Acquisition-quality signal, not payments health — volume surges every month-start. "
            f"Blended BUY overall (old view): {blended_y:.1f}% yesterday vs {blended_7:.1f}% last 7d.",
            ha="center", va="top", transform=ax.transAxes, fontsize=8,
            style="italic", color=NEUTRAL_TX)


def generate_image(rows: dict, report_date: str, out_path: Path) -> None:
    fig = plt.figure(figsize=(8.5, 11.8), facecolor=PAGE_BG)
    fig.text(0.5, 0.988, f"Payment Success Rates - {report_date}",
             ha="center", va="top", fontsize=20, fontweight="bold", color=INK)

    y = 0.935
    fig.text(0.20, y, "Delta vs Last 7d:", ha="left", va="center", fontsize=10, color=NEUTRAL_TX)
    for x, bg, label in [(0.36, GREEN_BG, "stable / up"), (0.50, YELLOW_BG, "-1 to -3pp"),
                         (0.64, RED_BG, "> -3pp drop")]:
        fig.add_artist(mpatches.Rectangle((x, y - 0.006), 0.018, 0.012,
                                          facecolor=bg, edgecolor="none", transform=fig.transFigure))
        fig.text(x + 0.024, y, label, ha="left", va="center", fontsize=10, color=INK)
    fig.text(0.20, y - 0.020,
             "Prepaid table inverted: rising share = warning (+2pp yellow, +5pp red)",
             ha="left", va="center", fontsize=8.5, style="italic", color=NEUTRAL_TX)

    gs = fig.add_gridspec(len(FUNNELS), 1, top=0.875, bottom=0.03, hspace=0.62)
    for i, (prefix, short_title, note) in enumerate(FUNNELS):
        ax = fig.add_subplot(gs[i, 0])
        ax.set_facecolor(PAGE_BG)
        if prefix == "PREPAID":
            render_prepaid(ax, rows)
        else:
            render_funnel(ax, prefix, short_title, rows, note=note)

    plt.savefig(out_path, dpi=170, bbox_inches="tight", facecolor=PAGE_BG)
    plt.close(fig)


PREPAID_KEYS = ("Prepaid_Total", "Prepaid_Rate", "Prepaid_Share")


def parse_rows(raw_rows: list) -> dict:
    out = {}
    for r in raw_rows:
        period = PERIOD_FROM_KEY.get(r.get("Period"), r.get("Period"))
        for key in PREPAID_KEYS:
            if r.get(key) is None:
                r[key] = 0
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
