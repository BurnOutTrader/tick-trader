# Tick Trader Backtesting Guide

This page summarizes the backtesting subsystem: key parameters, realism models, lifecycle semantics, and how to run a quick demo. For a broader project overview, see the main README at the repository root.

- Project overview and architecture: ../README.md


## Quick start

- Example binary that exercises order types against historical bars:
  - cargo run -p tt-engine --bin tt-backtest_orders
- Minimal programmatic start (pseudocode using helpers already in the repo):
  - Create a DB pool (Postgres), ensure schema
  - Build BacktestConfig (step, date range)
  - Start backtest with your Strategy via start_backtest(db, cfg, strategy)

Relevant entry points in the codebase:
- crates/engine/src/backtest/orchestrator.rs (start_backtest, BacktestConfig)
- crates/engine/src/backtest/backtest_feeder.rs (BacktestFeederConfig and engine)
- crates/engine/bin/backtest_orders.rs (end-to-end demo exercising order types)


## Orchestrator and time

Backtests are driven by a deterministic, logical clock advanced by the orchestrator.
- BacktestConfig
  - step: chrono::Duration – advance logical time by this step; feeder emits data <= now each step.
  - feeder: BacktestFeederConfig – parameters for historical fetch and realism models.
  - start_date, end_date (optional): map to feeder.range_start/range_end (UTC) for clamping.
  - clock: optional shared BacktestClock (advanced deterministically alongside emissions).

Determinism
- All scheduling (ack, first-fill, cancel/replace effects) is relative to the logical watermark set by BacktestAdvanceTo.
- There are no Utc::now() fallbacks in the backtest path; timestamps come from the orchestrator or data.


## Historical feeder (BacktestFeederConfig)

Controls windowing and realism model factories. Defaults are sensible for CME-style futures.
- window: prefetch window size (e.g., 2 days) used to bulk load data chunks per key.
- lookahead: additional prefetch beyond the window to reduce refills.
- warmup: optional period prior to range_start to emit for consolidator warmup.
- range_start/range_end: optional UTC clamps for the backtest.
- make_latency: Fn() -> Box<dyn LatencyModel>
- make_fill: Fn() -> Box<dyn FillModel>
- make_slippage: Fn() -> Box<dyn SlippageModel>
- make_fee: Fn() -> Box<dyn FeeModel>
- calendar: Arc<dyn SessionCalendar>

The feeder:
- Caches latest marks (tick/quote/candle) per symbol and latest MBP10 book snapshot.
- Emits data in timestamp order up to the orchestrator’s watermark.
- Simulates order lifecycles using the configured models and emits OrdersBatch updates.
- Emits position/account snapshots on fills; the runtime also synthesizes periodic snapshots (<= 1 Hz) only when content changes.


## Realism models (defaults and behavior)

LatencyModel (default: PxLatency)
- submit_to_ack(): delay from submission to acknowledgment.
- ack_to_first_fill(): delay from ack to first possible fill.
- cancel_rtt()/replace_rtt(): round-trip time for cancel/replace effects. Orders remain fillable until the effect time.

FillModel (default: CmeFillModel with FillConfig)
- Uses simulated time and an optional BookLevels snapshot for matching.
- Supports market policies: BookWalk, AtTouch, AtMid.
- Supports limit policies: TouchOrCross, AtOrBetter, NextTickOnly.
- Stop and StopLimit logic triggers off last price and/or book; trailing stop ratchets; buy-side bug fixed.
- Price-limit guard: no fills if SessionCalendar::is_limit_locked at candidate prices.
- Maker attribution: if an order previously rested (0 fills), the next fills are tagged maker=true.
- Partial fills: produces multiple fills over time; leaves keep working until done or canceled.

SlippageModel (default: NoSlippage)
- No price adjustment. You can swap in custom models later (e.g., spread-aware, gap-on-trigger).

FeeModel (default: PxFlatFee)
- Returns negative Money amounts for fees (positive for rebates if implemented). Applied per fill.
- Per-account fee snapshots are accumulated and emitted.

SessionCalendar (default: HoursCalendar)
- is_open/is_halt gates fills; next_open_after defers work when closed; optional limit-lock checks.


## Order lifecycle semantics

- Submit -> Ack: scheduled by submit_to_ack().
- Ack -> First fill: scheduled by ack_to_first_fill().
- Matching uses current MBP10 book (if present) and last mark to price.
- Partials: if book depth can’t satisfy full qty, partial fills emit OrderState::PartiallyFilled and the remainder keeps working.
- Cancel/Replace: effects occur at cancel_rtt()/replace_rtt() after request; orders remain live and fillable until the effect timestamp.
- No forced fills: if matching produces zero fills, the order remains working (resting) and is retried on subsequent ticks.


## Snapshots and portfolio

- Positions: maintained per-account/instrument; PositionsBatch emitted on fill ticks (and periodically by runtime when content changes, throttled to ≤ 1/s).
- Accounts: AccountDeltaBatch snapshots reflect fee cash flows and timing; realized PnL from trades is computed by the portfolio from executions/positions and last prices.


## Running the demo

- Ensure Postgres is available and the schema is initialized (ensure_schema is called in the demo).
- Build and run:
  - cargo run -p tt-engine --bin tt-backtest_orders
- What it does:
  - Subscribes to 1m candles for MNQ.Z25 (ProjectX/Topstep), walks bars, and places a series of orders (Market, Limit, Stop, StopLimit, JoinBid/JoinAsk, TrailingStop) driven by a bar counter.
  - Asserts acks for all orders; expects fills for market/crossing orders; prints bars, order updates, and positions.


## Configuration tips

- Step size: choose a step consistent with your data frequency (e.g., 250 ms–1 s for 1s/1m bars).
- Policies: adjust FillConfig to switch market/limit policies.
- Fees/Slippage: plug different model factories via BacktestFeederConfig to run scenario analyses.
- Calendar: use your venue’s session calendar for correct open/halting and price limits.


## Known modeling limitations (v1)

- No queue priority modeling at touch; no order book queue position.
- Default slippage is none; consider custom models for gap/impact.
- TIF (IOC/FOK) and bracket orders are not yet simulated end-to-end.
- Maker rebates are not modeled by default, though maker attribution exists for future fee tiers.


## Links

- Main project README: ../README.md
- Engine Orchestrator: crates/engine/src/backtest/orchestrator.rs
- Feeder and realism models: crates/engine/src/backtest/backtest_feeder.rs and crates/engine/src/backtest/realism_models
- Demo: crates/engine/bin/backtest_orders.rs
