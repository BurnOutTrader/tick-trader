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
pub use crate::statics::bus;
pub use crate::statics::clock;
pub use crate::statics::consolidators;
pub use crate::statics::order_modify;
pub use crate::statics::order_placement;
pub use crate::statics::portfolio;
pub use crate::statics::subscriptions;
