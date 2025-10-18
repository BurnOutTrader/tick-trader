# Daily Auto-Update of Historical Data

This document explains how the server refreshes historical data automatically once per UTC day, how it is configured, and how to verify it.

## Overview
- On server launch, providers discovered from environment variables are eagerly initialized (ProjectX today; Rithmic planned).
- For each initialized provider, the server runs a once-per-UTC-day job that:
  - Discovers instruments from the market data API
  - Iterates configured topics (e.g., Candles1s/1m/1h/1d)
  - Uses the DownloadManager to fetch and persist any missing data
- Work is sequential per provider: one instrument at a time; topics are processed in the order configured.
- Multiple providers may update in parallel during startup (each in its own background task).
- When a provider finishes successfully, a completion stamp is written to the database to avoid doing the same work again on the same UTC day.

## Configuration (.env)
- `AUTO_UPDATE_ENABLED=true`
  - Master switch. Set to `false` to disable auto-update.
- `AUTO_UPDATE_TOPICS=Candles1s,Candles1m,Candles1h,Candles1d`
  - Ordered, comma-separated list of topics to refresh per instrument.
  - Supported values: `Ticks, Quotes, MBP10, MBP1, Candles1s, Candles1m, Candles1h, Candles1d`.
- `AUTO_UPDATE_INSTRUMENT_FILTER=`
  - Optional substring filter to reduce the instrument set during testing (e.g., `MNQ`).
- `AUTO_UPDATE_PX_FIRM=topstep`
  - Preferred firm for ProjectX tenants. Also set `PX_<TENANT>_FIRM` (e.g., `PX_TOPSTEP_FIRM=topstep`) so the adapter scopes instrument discovery appropriately.

Example:
```
PX_TOPSTEP_FIRM=topstep
PX_TOPSTEP_APIKEY=your_api_key
PX_TOPSTEP_USERNAME=you@example.com

DATABASE_URL=127.0.0.1:5432:5432

AUTO_UPDATE_ENABLED=true
AUTO_UPDATE_TOPICS=Candles1s,Candles1m,Candles1h,Candles1d
AUTO_UPDATE_INSTRUMENT_FILTER=
AUTO_UPDATE_PX_FIRM=topstep
```

## How it works internally
- The `ProviderManager` eagerly ensures providers on startup when `AUTO_UPDATE_ENABLED` is true.
- On first initialization of a provider, a background task is spawned to run the daily update:
  1) Ensure DB schema exists (idempotent)
  2) Check the daily stamp for today in `provider_daily_updates` and skip if already present
  3) Discover instruments via `md.list_instruments(None)` (scoped by firm if applicable)
  4) For each instrument and topic, call `DownloadManager` to fetch and persist missing history
  5) If all tasks completed, write a stamp row for (provider_kind, today_utc)
- There are no artificial timeouts in the auto-update flow. If the provider hangs, the job will wait; stop the server if you need to interrupt.

## Database stamp
- Table: `provider_daily_updates(provider_kind TEXT, utc_date DATE, completed_at TIMESTAMPTZ, PRIMARY KEY(provider_kind, utc_date))`
- ProviderKind is stored as a stable short code (e.g., `projectx`, `rithmic`) via `tt_database::paths::provider_kind_to_db_string`.
- To force a re-run the same day, delete the row:
```
DELETE FROM provider_daily_updates WHERE provider_kind='projectx' AND utc_date=CURRENT_DATE;
```

## Monitoring
- Logs: search for `auto-update:`
  - `auto-update: already completed today`
  - `auto-update: started; waiting`
  - `auto-update: completed and stamped`
- Verify stamp:
```
SELECT * FROM provider_daily_updates ORDER BY utc_date DESC, provider_kind;
```

## Notes and edge cases
- The DownloadManager computes start times from the latest data in your DB, so first runs can be longer (backfill).
- Candles are filtered to match the requested resolution and to ensure half-open interval correctness, avoiding duplicates at the boundary.
- If some instrument/topic tasks fail, errors are logged and the run continues; the daily stamp is only set after the full pass completes.
