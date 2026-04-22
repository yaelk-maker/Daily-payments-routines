# Daily payments routines

Daily payments monitoring routines that run on BigQuery and post results to
Slack. Each routine is a self-contained markdown prompt under `routines/` that
a scheduled Claude Code session executes end-to-end.

## Layout

```
queries/     # SQL, one file per check
routines/    # Prompt files Claude runs on schedule
logs/        # Per-run JSON output + Slack response (committed)
.env.example # Required env vars
```

## Routines

| Routine | Query | Cadence | Channel |
|---|---|---|---|
| `routines/try_funnel_daily.md` | `queries/try_funnel_daily_monitoring.sql` | Daily | `C0AVBTM4BG8` |

## Setup (one-time)

1. **Slack bot** — use the app from
   [`yaelk-maker/Slack-BOT`](https://github.com/yaelk-maker/Slack-BOT). Install
   it to the workspace, copy the Bot User OAuth token (`xoxb-...`), and invite
   the bot to channel `C0AVBTM4BG8`:
   ```
   /invite @<bot-name>
   ```
   Required OAuth scope: `chat:write`.

2. **BigQuery** — the remote session needs `bq` CLI authenticated to project
   `maelys-data` (or whichever you set as `BQ_PROJECT`). The SQL reads from
   `maelys-data.spreedly.transactions_s`.

3. **Secrets** — copy `.env.example` to `.env` for local testing, or set the
   same vars as session secrets in the scheduled routine:
   - `SLACK_BOT_TOKEN`
   - `SLACK_CHANNEL_ID` (default `C0AVBTM4BG8`)
   - `BQ_PROJECT` (default `maelys-data`)

## Scheduling via Claude Code remote routines

In Claude Code on the web, create a scheduled routine pointed at this repo on
branch `claude/payments-monitoring-bigquery-slack-bxrHo`. Use the contents of
the relevant `routines/*.md` file as the scheduled prompt — the prompt contains
all commands Claude needs to run the query, post to Slack, and commit the log.

Suggested schedule for the TRY funnel routine: **daily, 08:00 Asia/Jerusalem**
(after the previous day has fully closed in `transactions_s`).

## Manual run (for testing)

```bash
export SLACK_BOT_TOKEN=xoxb-...
export SLACK_CHANNEL_ID=C0AVBTM4BG8
export BQ_PROJECT=maelys-data
# then feed routines/try_funnel_daily.md to a Claude Code session, or
# execute the bash blocks in that file in order.
```

## Adding a new routine

1. Drop the SQL in `queries/<name>.sql`.
2. Copy `routines/try_funnel_daily.md` to `routines/<name>.md` and adjust:
   the query path, the formatter, the `RUN_DATE` filenames, and the commit
   message.
3. Add a row to the Routines table above.
4. Create a separate scheduled routine in Claude Code pointed at the new file.
