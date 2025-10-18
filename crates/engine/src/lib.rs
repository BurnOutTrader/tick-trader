pub mod backtest;
pub(crate) mod client;
pub(crate) mod engine;
pub(crate) mod helpers;
pub mod models;
pub mod runtime;
pub mod statics;
pub mod stats;
pub mod traits;

// === Top-level re-exports for strategy authors ===
// Keep original module paths working (tt_engine::statics::*) while also exposing
// convenient top-level modules and types (e.g., tt_engine::clock::time_now).

// Core runtime and trait
pub use crate::runtime::EngineRuntime;
pub use crate::traits::Strategy;

// Common models
pub use crate::models::DataTopic;

// Backtest orchestrator helpers
pub use crate::backtest::orchestrator::{start_backtest, start_backtest_with_dates, BacktestConfig};

// Statics helpers as top-level modules
pub use crate::statics::clock::{time_now, time_ns};
pub use crate::statics::consolidators::{add_hybrid_tick_or_candle, add_consolidator, remove_consolidator, remove_hybrid_tick_or_candle};
pub use crate::statics::order_modify::{replace_order, cancel_order};
pub use crate::statics::order_placement::{place_order};
pub use crate::statics::portfolio::{open_positions, position_open_pnl, balance, can_trade, booked_pnl, open_pnl, open_orders, open_orders_for_instrument, open_order_count, is_long, is_short, is_flat, qty, long_qty, short_qty};
pub use crate::statics::subscriptions::{subscribe, unsubscribe};
