use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use strum_macros::Display;

#[derive(
    Archive,
    RkyvDeserialize,
    RkyvSerialize,
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Display,
    Serialize,
    Deserialize,
)]
pub enum OrderType {
    /// A market order to buy or sell at the best available price in the current market.
    Market = 1,
    /// A limit order to buy or sell at a specific price or better.
    Limit = 2,
    /// A stop market order to buy or sell once the price reaches the specified stop/trigger price. When the stop price is reached, the order effectively becomes a market order.
    StopMarket = 3,
    /// A stop limit order to buy or sell which combines the features of a stop order and a limit order. Once the stop/trigger price is reached, a stop-limit order effectively becomes a limit order.
    StopLimit = 4,
    /// A market-to-limit order is a market order that is to be executed as a limit order at the current best market price after reaching the market.
    MarketToLimit = 5,
    /// A market-if-touched order effectively becomes a market order when the specified trigger price is reached.
    MarketIfTouched = 6,
    /// A limit-if-touched order effectively becomes a limit order when the specified trigger price is reached.
    LimitIfTouched = 7,
    /// A trailing stop market order sets the stop/trigger price at a fixed "trailing offset" amount from the market.
    TrailingStopMarket = 8,
    /// A trailing stop limit order combines the features of a trailing stop order with those of a limit order.
    TrailingStopLimit = 9,
    Unknown,
}

#[derive(
    Archive,
    RkyvDeserialize,
    RkyvSerialize,
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Display,
    Serialize,
    Deserialize,
)]
pub enum OrderEventType {
    Initialized,
    Denied,
    Released,
    Submitted,
    Accepted,
    Rejected,
    Canceled,
    Expired,
    Triggered,
    PendingUpdate,
    PendingCancel,
    ModifyRejected,
    CancelRejected,
    Updated,
    PartiallyFilled,
    Filled,
}
