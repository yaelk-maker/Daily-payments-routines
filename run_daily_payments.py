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
  BuyPaid_Total   / BuyPaid_Overall   / BuyPaid_CC   / BuyPaid_AP   / BuyPaid_PP
  BuyUnpaid_Total / BuyUnpaid_Overall / BuyUnpaid_CC / BuyUnpaid_AP / BuyUnpaid_PP
  Sub_Total       / Sub_Overall       / Sub_CC       / Sub_AP       / Sub_PP
  SubAll_Total    / SubAll_Overall    / SubAll_CC    / SubAll_AP    / SubAll_PP
  <funnel>_CC_N / <funnel>_AP_N / <funnel>_PP_N  (per-method order counts)
  BUY only: <funnel>_<m>_Fraud / <funnel>_<m>_TotalSucc / <funnel>_<m>_AllN for
  <m> in (Overall, CC, AP, PP), with <funnel>_AllOrders as the Overall count
  BuyPaid_Share

TRY was retired on 23 Aug 2026. BUY is split by acquisition source using the
company MediaPaidType definition (paid media vs everything else). Yesterday's
per-method cells are only traffic-lighted when they carry at least
MIN_SCORED_ORDERS orders; smaller cells are greyed out so a single decline
does not read as an incident.

BUY tables show three Yesterday rows: card success (Forter-declined orders
excluded), Forter fraud declines, and overall success (all orders). Only the
overall success row is traffic-lighted, against overall success over the last
7 days. Last 7d / MTD / Prev month show overall success too, so the Δ can be
read straight off the table.
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
BUY_FUNNELS = {"BuyPaid", "BuyUnpaid"}
FUNNELS = [
    ("BuyPaid",   "BUY Paid",   "paid media"),
    ("BuyUnpaid", "BUY Unpaid", "no paid media"),
    ("Sub",       "SUB",        "first attempt only"),
    ("SubAll",    "SUB Blended", "all attempts incl. retries"),
]
MIN_SCORED_ORDERS = 50   # yesterday's per-method cells below this are not scored

# MAËLYS brand palette (semantic traffic-light colors kept for status)
INK            = "#120D0E"
PAGE_BG        = "#FBFAF8"
HEADER_BG      = "#FFAFC4"
PERIOD_BG      = "#FFE0E9"
YEST_PERIOD_BG = "#DB6B8A"
YEST_PERIOD_TX = "#FBFAF8"
YEST_SUB_BG    = "#FFE0E9"   # the two extra BUY Yesterday rows
NEUTRAL_TX     = "#5A524D"
LOW_N_BG       = "#EBE5E2"
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


def _style_table(tbl, n_cols: int, yest_delta_tx: str, yest_rows: int = 1) -> None:
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(10)
    for i in range(n_cols):
        h = tbl[(0, i)]
        h.get_text().set_color(INK)
        h.get_text().set_fontweight("bold")
    for i in range(1, yest_rows + 1):
        yc = tbl[(i, 0)]
        yc.get_text().set_color(YEST_PERIOD_TX if i == yest_rows else INK)
        yc.get_text().set_fontweight("bold")
    dc = tbl[(yest_rows, n_cols - 1)]
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


def _pct(v) -> str:
    return "n/a" if v is None else f"{v:.1f}%"


def _score(v, base, n) -> str:
    """Traffic-light background for a Yesterday cell, grey when not scorable."""
    if v is None or base is None or (n or 0) < MIN_SCORED_ORDERS:
        return LOW_N_BG
    return bg_for(round(v, 1) - round(base, 1))


def _delta_cell(v, base):
    if v is None or base is None:
        return "", "white", 0.0
    # computed on the 1dp values shown, so the delta can be checked against the table
    d = round(round(v, 1) - round(base, 1), 1)
    return f"{'+' if d >= 0 else ''}{d:.1f}pp", bg_for(d), d


def render_funnel(ax, prefix: str, short_title: str, rows: dict, note: str = None) -> None:
    is_buy = prefix in BUY_FUNNELS
    y, l7 = rows["Yesterday"], rows["Last 7d"]
    yest_total = (y.get(f"{prefix}_AllOrders") if is_buy else y[f"{prefix}_Total"]) or 0
    title = f"{short_title} - {yest_total:,} orders yesterday"
    if note:
        title += f"  ({note})"
    _title(ax, title)

    col_labels = ["Period"] + [m[1] for m in METRICS] + ["Δ Overall vs 7d"]
    cell_text, cell_colors = [], []

    def n_for(r, code, all_orders):
        if code == "Overall":
            return r.get(f"{prefix}_AllOrders") if all_orders else r[f"{prefix}_Total"]
        return r.get(f"{prefix}_{code}_AllN" if all_orders else f"{prefix}_{code}_N")

    if is_buy:
        # Row 1: card success, row 2: Forter fraud declines (plain numbers)
        for label, suffix in (("Yest. card success", ""), ("Yest. fraud declines", "_Fraud")):
            cell_text.append([label] + [_pct(y.get(f"{prefix}_{c}{suffix}")) for c, _ in METRICS] + [""])
            cell_colors.append([YEST_SUB_BG] + ["white"] * (len(METRICS) + 1))
        # Row 3: overall success incl. Forter-blocked, scored vs the same metric last 7d
        vals, colors = ["Yest. overall success"], [YEST_PERIOD_BG]
        for code, _ in METRICS:
            v, base = y.get(f"{prefix}_{code}_TotalSucc"), l7.get(f"{prefix}_{code}_TotalSucc")
            vals.append(_pct(v))
            colors.append(_score(v, base, n_for(y, code, True)))
        d_txt, d_bg, delta = _delta_cell(y.get(f"{prefix}_Overall_TotalSucc"),
                                         l7.get(f"{prefix}_Overall_TotalSucc"))
        cell_text.append(vals + [d_txt])
        cell_colors.append(colors + [d_bg])
        yest_rows = 3
    else:
        vals, colors = ["Yesterday"], [YEST_PERIOD_BG]
        for code, _ in METRICS:
            v, base = y[f"{prefix}_{code}"], l7[f"{prefix}_{code}"]
            vals.append(_pct(v))
            colors.append(_score(v, base, n_for(y, code, False)))
        d_txt, d_bg, delta = _delta_cell(y[f"{prefix}_Overall"], l7[f"{prefix}_Overall"])
        cell_text.append(vals + [d_txt])
        cell_colors.append(colors + [d_bg])
        yest_rows = 1

    # BUY history rows show overall success, the metric the coloured row is scored on
    hist_suffix = "_TotalSucc" if is_buy else ""
    for period in PERIODS[1:]:
        r = rows[period]
        cell_text.append([period] + [_pct(r.get(f"{prefix}_{c}{hist_suffix}")) for c, _ in METRICS] + [""])
        cell_colors.append([PERIOD_BG] + ["white"] * (len(METRICS) + 1))

    tbl = ax.table(
        cellText=cell_text, colLabels=col_labels,
        cellColours=cell_colors,
        colColours=[HEADER_BG] * len(col_labels),
        cellLoc="center", colLoc="center",
        colWidths=[0.23, 0.12, 0.11, 0.13, 0.11, 0.18] if is_buy
                  else [0.13, 0.13, 0.12, 0.15, 0.13, 0.20],
        bbox=[0.0, 0.0, 1.0, 1.0],
    )
    _style_table(tbl, len(col_labels), tx_for(delta), yest_rows=yest_rows)
    if is_buy:
        for i in range(1, yest_rows + 1):
            tbl[(i, 0)].get_text().set_fontsize(9)


def _footer_lines(rows: dict) -> list:
    y, l7 = rows["Yesterday"], rows["Last 7d"]
    share_y, share_7 = y.get("BuyPaid_Share"), l7.get("BuyPaid_Share")
    lines = []
    if share_y is not None and share_7 is not None:
        lines.append(f"BUY mix: paid media {share_y:.1f}% of BUY orders yesterday "
                     f"vs {share_7:.1f}% last 7d.")
    return lines


def generate_image(rows: dict, report_date: str, out_path: Path) -> None:
    fig = plt.figure(figsize=(8.5, 13.0), facecolor=PAGE_BG)
    fig.text(0.5, 0.988, f"Payment Success Rates - {report_date}",
             ha="center", va="top", fontsize=20, fontweight="bold", color=INK)

    y = 0.948
    fig.text(0.13, y, "Delta vs Last 7d:", ha="left", va="center", fontsize=10, color=NEUTRAL_TX)
    for x, bg, label in [(0.29, GREEN_BG, "stable / up"), (0.43, YELLOW_BG, "-1 to -3pp"),
                         (0.57, RED_BG, "> -3pp drop"),
                         (0.71, LOW_N_BG, f"< {MIN_SCORED_ORDERS} orders")]:
        fig.add_artist(mpatches.Rectangle((x, y - 0.008), 0.018, 0.016,
                                          facecolor=bg, edgecolor="none", transform=fig.transFigure))
        fig.text(x + 0.024, y, label, ha="left", va="center", fontsize=10, color=INK)
    fig.text(0.13, y - 0.016,
             "BUY: Last 7d / MTD / Prev month show overall success (incl. Forter-declined orders); "
             "only that Yesterday row is scored.",
             ha="left", va="center", fontsize=8.5, style="italic", color=NEUTRAL_TX)

    heights = [7 if p in BUY_FUNNELS else 5 for p, _, _ in FUNNELS]
    gs = fig.add_gridspec(len(FUNNELS), 1, top=0.895, bottom=0.06, hspace=0.45,
                          height_ratios=heights)
    for i, (prefix, short_title, note) in enumerate(FUNNELS):
        ax = fig.add_subplot(gs[i, 0])
        ax.set_facecolor(PAGE_BG)
        render_funnel(ax, prefix, short_title, rows, note=note)

    for j, line in enumerate(_footer_lines(rows)):
        fig.text(0.5, 0.024 - j * 0.016, line, ha="center", va="center",
                 fontsize=8.5, style="italic", color=NEUTRAL_TX)

    plt.savefig(out_path, dpi=170, bbox_inches="tight", facecolor=PAGE_BG)
    plt.close(fig)


def _num(v):
    """BQ JSON may carry numbers as strings; NULL stays None."""
    if v is None or isinstance(v, (int, float)):
        return v
    try:
        f = float(v)
    except (TypeError, ValueError):
        return v
    return int(f) if f.is_integer() and "." not in str(v) else f


def parse_rows(raw_rows: list) -> dict:
    out = {}
    for r in raw_rows:
        r = {k: (v if k == "Period" else _num(v)) for k, v in r.items()}
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
