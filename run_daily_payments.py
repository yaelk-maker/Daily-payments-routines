#!/usr/bin/env python3
"""
Daily payments routine: BQ results → "Payments Daily" PNG → GitHub raw URL → Slack post.

Designed to run inside Claude Code Remote Routine. Two-step flow:

  1. The routine executes ``payment method success rates by funnel.sql`` via the
     BigQuery MCP and saves the rows as JSON.

  2. This script reads that JSON, renders ``payments.png``, commits + pushes the
     image to the current branch, and posts a Slack message with an ``image``
     block pointing at ``raw.githubusercontent.com``.

Required environment variables:
  SLACK_BOT_TOKEN          — bot token with chat:write
  SLACK_CHANNEL_PAYMENTS   — channel ID for #payments-daily-monitoring

Usage:
  python run_daily_payments.py <bq_results.json>
  cat bq_results.json | python run_daily_payments.py -
  python run_daily_payments.py <bq_results.json> --render-only [out.png]   (no git, no Slack)

Expected JSON shape — a list of rows with keys
  report_date, period ('Y' | 'B28'), segment ('BuyPaid' | 'BuyUnpaid' | 'Sub'),
  dim, n, k
where dim is 'Overall' (n attempted, k completed), 'CC' / 'AP' / 'PP' / 'AF'
(BUY, per payment method), 'Forter' (BUY, k = Forter-declined orders) or
'RN1' / 'RN2-3' / 'RN4+' (SUB, by recurring order number).

Scoring: yesterday is compared with the previous 28 days ("normal"). A cell is
coloured only when a move that bad is unlikely to be chance at yesterday's
volume, by an exact binomial test: amber below 2.3%, red below 0.13% (the
one-sided equivalents of 2 and 3 standard deviations). SUB is compared with an
expected rate that adjusts for yesterday's mix of recurring order numbers.
Every flagged cell is listed in the banner at the top.
"""

import json
import math
import os
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, Rectangle

REPO_DIR          = Path(__file__).parent.resolve()
IMAGE_PATH        = REPO_DIR / "payments.png"
GITHUB_OWNER_REPO = "yaelk-maker/Daily-payments-routines"

BUY_SEGMENTS = [("BuyPaid", "Paid"), ("BuyUnpaid", "Unpaid")]
METHODS      = [("CC", "Card"), ("AP", "Apple Pay"), ("PP", "PayPal"), ("AF", "AfterPay")]
SUB_BUCKETS  = [("RN1", "1st recurring order"), ("RN2-3", "2nd to 3rd"), ("RN4+", "4th+")]
AMBER_P, RED_P = 0.0228, 0.00135

# MAËLYS brand palette (semantic traffic-light colors kept for status)
INK, BG            = "#120D0E", "#FBFAF8"
PINK, PINK_L       = "#FFAFC4", "#FFE0E9"
NEU, NEU2, LINE    = "#5A524D", "#968E89", "#EBE5E2"
AMBER, RED, GREEN  = "#FFE99C", "#F5C6CB", "#A8E0A0"
AMBER_TX, RED_TX, GREEN_TX = "#8A6A00", "#C5283D", "#1F7A1F"
FILL = {"red": RED, "amber": AMBER, "ok": "white", "na": "white"}


# ---------- statistics ----------

def _log_pmf(i: int, n: int, p: float) -> float:
    return (math.lgamma(n + 1) - math.lgamma(i + 1) - math.lgamma(n - i + 1)
            + i * math.log(p) + (n - i) * math.log1p(-p))


def tail_p(k: int, n: int, p: float, upper: bool = False) -> float:
    """Exact one-sided binomial tail: P(X <= k), or P(X >= k) when upper."""
    if n <= 0 or p is None or p <= 0 or p >= 1:
        return 1.0
    rng = range(k, n + 1) if upper else range(0, k + 1)
    return min(1.0, sum(math.exp(_log_pmf(i, n, p)) for i in rng))


def status(pv: float) -> str:
    return "red" if pv < RED_P else "amber" if pv < AMBER_P else "ok"


def rate(nk):
    n, k = nk
    return k / n if n else None


# ---------- data ----------

def parse_rows(raw_rows: list):
    cells, report_date = {}, None
    for r in raw_rows:
        report_date = report_date or r.get("report_date")
        cells[(r["period"], r["segment"], r["dim"])] = (int(r["n"] or 0), int(r["k"] or 0))
    if not any(p == "Y" for p, _, _ in cells):
        raise SystemExit("BQ results have no rows for yesterday (period 'Y')")
    return cells, str(report_date) if report_date else yesterday_date_il()


def build_model(cells: dict) -> dict:
    def get(p, s, d):
        return cells.get((p, s, d), (0, 0))

    def score(y, base, upper=False):
        if not y[0]:
            return "na"
        if base is None:
            return "ok"
        return status(tail_p(y[1], y[0], base, upper=upper))

    m = {"tiles": [], "buy": {}, "sub": [], "flags": []}

    # headline tiles: BUY vs its 28-day rate, SUB vs the rate expected for yesterday's mix
    for seg, label in (("BuyPaid", "BUY Paid"), ("BuyUnpaid", "BUY Unpaid")):
        y = get("Y", seg, "Overall")
        base = rate(get("B28", seg, "Overall"))
        m["tiles"].append(dict(label=label, n=y[0], k=y[1], v=rate(y), base=base,
                               basis="normal", st=score(y, base)))
    y = get("Y", "Sub", "Overall")
    exp_k = sum(get("Y", "Sub", d)[0] * (rate(get("B28", "Sub", d)) or 0) for d, _ in SUB_BUCKETS)
    exp = exp_k / y[0] if y[0] else None
    m["tiles"].append(dict(label="SUB first attempt", n=y[0], k=y[1], v=rate(y), base=exp,
                           basis="expected", st=score(y, exp)))
    for t in m["tiles"]:
        if t["st"] in ("amber", "red"):
            m["flags"].append((t["st"], f"{t['label']}: {t['v']*100:.1f}% vs {t['base']*100:.1f}% "
                                            f"{t['basis']} ({t['n']:,} attempts)"))

    # BUY by payment method + Forter blocks
    for seg, label in BUY_SEGMENTS:
        row = []
        for code, name in METHODS:
            y = get("Y", seg, code)
            base = rate(get("B28", seg, code))
            st = score(y, base)
            row.append(dict(v=rate(y), base=base, n=y[0], st=st))
            if st in ("amber", "red"):
                m["flags"].append((st, f"BUY {label} {name.lower() if code == 'CC' else name} approval: "
                                       f"{rate(y)*100:.1f}% vs {base*100:.1f}% normal ({y[0]} orders)"))
        y = get("Y", seg, "Forter")
        base = rate(get("B28", seg, "Forter"))
        st = score(y, base, upper=True)
        row.append(dict(count=y[1], v=rate(y), base=base, n=y[0], st=st))
        if st in ("amber", "red"):
            m["flags"].append((st, f"Forter blocks on BUY {label}: {y[1]} orders ({rate(y)*100:.1f}%) "
                                   f"vs {base*100:.1f}% normal"))
        m["buy"][label] = row

    # SUB by recurring order number
    for code, name in SUB_BUCKETS:
        y = get("Y", "Sub", code)
        base = rate(get("B28", "Sub", code))
        st = score(y, base)
        m["sub"].append(dict(name=name, v=rate(y), base=base, n=y[0], st=st))
        if st in ("amber", "red"):
            m["flags"].append((st, f"SUB {name} approval: {rate(y)*100:.1f}% vs {base*100:.1f}% "
                                   f"normal ({y[0]} orders)"))
    return m


# ---------- drawing ----------

def _pct(v) -> str:
    return "n/a" if v is None else f"{v*100:.1f}%"


def generate_image(cells: dict, report_date: str, out_path: Path) -> None:
    m = build_model(cells)
    fig = plt.figure(figsize=(8.5, 10.6), facecolor=BG)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 125)
    ax.axis("off")

    def txt(x, y, s, **k):
        k.setdefault("color", INK)
        k.setdefault("fontsize", 10)
        k.setdefault("va", "center")
        ax.text(x, y, s, **k)

    def box(x, y, w, h, fc, ec=LINE, r=1.2):
        ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle=f"round,pad=0,rounding_size={r}",
                                    fc=fc, ec=ec, lw=1.0))

    def cell(x, y, w, h, value, sub, st):
        box(x, y, w, h, FILL[st], r=0.6)
        txt(x + w / 2, y + h * 0.62, value, ha="center", fontsize=11)
        txt(x + w / 2, y + h * 0.25, sub, ha="center", fontsize=8, color=NEU2)

    def header(y, xs, w, labels):
        for x, l in zip(xs, labels):
            box(x, y, w, 3.4, PINK, ec="none", r=0.6)
            txt(x + w / 2, y + 1.7, l, ha="center", fontsize=9.5, fontweight="bold")

    def section(y, title, note):
        txt(4, y, title, fontsize=12, fontweight="bold")
        txt(96, y, note, fontsize=8.5, color=NEU2, ha="right", style="italic")

    d = datetime.strptime(report_date, "%Y-%m-%d")
    txt(4, 120.5, "Payments Daily", fontsize=20, fontweight="bold")
    txt(96, 120.5, d.strftime("%a %d %b %Y") + "  ·  US", fontsize=11, color=NEU, ha="right")

    # banner: every flagged cell (red items first), or "All normal"
    flags = sorted(m["flags"], key=lambda f: f[0] != "red")
    any_red = any(sev == "red" for sev, _ in flags)
    bh = 4.2 + 3.4 * len(flags)
    box(4, 116 - bh, 92, bh, (RED if any_red else AMBER) if flags else GREEN, ec="none")
    head = f"{len(flags)} item{'s' if len(flags) > 1 else ''} to check" if flags else "All normal"
    txt(6.5, 116 - (2.6 if flags else bh / 2), head, fontsize=12, fontweight="bold",
        color=(RED_TX if any_red else AMBER_TX) if flags else GREEN_TX)
    for i, (sev, f) in enumerate(flags):
        txt(6.5, 116 - 6.2 - 3.4 * i, ("●  " if sev == "red" else "•  ") + f, fontsize=10)
    y0 = 116 - bh - 3

    # headline tiles
    tw, gap = 28.7, 3
    for i, t in enumerate(m["tiles"]):
        x = 4 + i * (tw + gap)
        box(x, y0 - 17, tw, 17, "white")
        if t["st"] in ("amber", "red"):
            ax.add_patch(Rectangle((x, y0 - 17), 1.1, 17,
                                   fc="#E0B400" if t["st"] == "amber" else RED_TX, ec="none"))
        txt(x + 3, y0 - 3.2, t["label"], fontsize=10, color=NEU, fontweight="bold")
        txt(x + 3, y0 - 8.6, _pct(t["v"]), fontsize=22, fontweight="bold")
        if t["v"] is not None and t["base"] is not None:
            dpp = round(t["v"] * 100, 1) - round(t["base"] * 100, 1)
            txt(x + 3, y0 - 13.2, f"{t['basis']} {t['base']*100:.1f}%   Δ {dpp:+.1f}pp",
                fontsize=9, color=NEU)
        txt(x + 3, y0 - 15.6, f"{t['n']:,} attempts  ·  {t['k']:,} completed",
            fontsize=8.5, color=NEU2)
    y0 -= 22

    # BUY by payment method
    section(y0, "BUY approval by payment method", "overall success, Forter blocks included")
    y0 -= 5.5
    lw_ = 12
    cw = (96 - 4 - lw_ - 0.6 * 5) / 5
    xs = [4 + lw_ + 0.6 + i * (cw + 0.6) for i in range(5)]
    header(y0, xs, cw, [name for _, name in METHODS] + ["Forter blocks"])
    y0 -= 0.6
    for label, row in m["buy"].items():
        y0 -= 6.2
        box(4, y0, lw_, 5.6, PINK_L, ec="none", r=0.6)
        txt(4 + lw_ / 2, y0 + 2.8, label, ha="center", fontsize=10.5, fontweight="bold")
        for x, c in zip(xs[:4], row[:4]):
            value = _pct(c["v"]) if c["n"] else "no orders"
            cell(x, y0, cw, 5.6, value, f"normal {_pct(c['base'])}  ·  {c['n']}", c["st"])
        f = row[4]
        cell(xs[4], y0, cw, 5.6, f"{f['count']}  ({_pct(f['v'])})",
             f"normal {_pct(f['base'])}", f["st"])
    y0 -= 3
    txt(4, y0, "Cells: approval on that method · normal = previous 28 days · orders yesterday. "
               "Tiles count each order once.", fontsize=8.5, color=NEU2, style="italic")
    y0 -= 6.5

    # SUB by recurring order number
    section(y0, "SUB first attempt by recurring order",
            "regular monthly charge, failed-month restarts excluded")
    y0 -= 5.5
    pw = 29.7
    xs = [4 + i * (pw + 1.45) for i in range(3)]
    header(y0, xs, pw, [s["name"] for s in m["sub"]])
    y0 -= 6.2
    for x, s in zip(xs, m["sub"]):
        cell(x, y0, pw, 5.6, _pct(s["v"]) if s["n"] else "no orders",
             f"normal {_pct(s['base'])}  ·  {s['n']} orders", s["st"])
    y0 -= 5

    # legend
    ax.plot([4, 96], [y0, y0], color=LINE, lw=1)
    y0 -= 3
    for i, (fc, lab) in enumerate([("white", "within normal range"),
                                   (AMBER, "unusual (< 2.3% chance)"),
                                   (RED, "very unusual (< 0.1% chance)")]):
        x = 4 + i * 30
        box(x, y0 - 1.1, 3, 2.2, fc, r=0.4)
        txt(x + 4.2, y0, lab, fontsize=9, color=NEU)
    txt(4, y0 - 4, "US only. BUY Paid = paid-media source or a new customer's first order "
                   "(Analysis report rule).", fontsize=8.5, color=NEU2, style="italic")
    txt(4, y0 - 7, "Normal = previous 28 days. Colour only when a move is unlikely to be chance "
                   "at yesterday's volume (exact binomial test).", fontsize=8.5, color=NEU2, style="italic")
    ax.set_ylim(y0 - 10, 125)

    plt.savefig(out_path, dpi=170, bbox_inches="tight", facecolor=BG)
    plt.close(fig)


# ---------- publish ----------

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
    title   = f"Payments Daily - {report_date}"
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
    args = sys.argv[1:]
    if not args:
        sys.exit("Usage: run_daily_payments.py <bq_results.json | -> [--render-only [out.png]]")
    raw = sys.stdin.read() if args[0] == "-" else Path(args[0]).read_text()
    cells, report_date = parse_rows(json.loads(raw))

    if "--render-only" in args:
        i = args.index("--render-only")
        out = Path(args[i + 1]) if len(args) > i + 1 else IMAGE_PATH
        generate_image(cells, report_date, out)
        print(f"Rendered {out}")
        return

    print(f"Generating image -> {IMAGE_PATH}")
    generate_image(cells, report_date, IMAGE_PATH)

    print("Committing and pushing image...")
    branch = git_commit_push(f"Daily payments report — {report_date}")

    img_url = (f"https://raw.githubusercontent.com/{GITHUB_OWNER_REPO}/{branch}"
               f"/payments.png?v={int(time.time())}")
    print(f"Posting to Slack: {img_url}")
    ts = post_to_slack(img_url, report_date)
    print(f"Done. Slack ts={ts}")


if __name__ == "__main__":
    main()
