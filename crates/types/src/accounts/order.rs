use super::events::{ClientOrderId, ProviderOrderId, Side};
use crate::rkyv_types::DecimalDef;
use crate::securities::symbols::Instrument;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use rust_decimal::Decimal;

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderState {
    New,
    Acknowledged,
    PartiallyFilled,
    Filled,
    Canceled,
    Rejected,
}

impl OrderState {
    // Higher value means higher precedence in tie-breaks (same seq)
    pub fn precedence(self) -> u8 {
        match self {
            OrderState::Rejected => 6,
            OrderState::Canceled => 5,
            OrderState::Filled => 4,
            OrderState::PartiallyFilled => 3,
            OrderState::Acknowledged => 2,
            OrderState::New => 1,
        }
    }
}

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone)]
pub struct Order {
    pub provider_order_id: Option<ProviderOrderId>,
    pub client_order_id: Option<ClientOrderId>,
    pub instrument: Instrument,
    pub side: Side,
    pub version: u64,
    pub last_provider_seq: Option<u64>,
    pub state: OrderState,
    pub qty: i64,
    pub leaves: i64,
    pub cum_qty: i64,
    #[rkyv(with = DecimalDef)]
    pub avg_fill_px: Decimal,
}

impl Order {
    pub fn new(instrument: Instrument, side: Side, qty: i64) -> Self {
        Self {
            provider_order_id: None,
            client_order_id: None,
            instrument,
            side,
            version: 0,
            last_provider_seq: None,
            state: OrderState::New,
            qty,
            leaves: qty,
            cum_qty: 0,
            avg_fill_px: Decimal::ZERO,
        }
    }

    pub fn can_apply(&self, incoming_seq: Option<u64>, incoming_state: OrderState) -> bool {
        match (self.last_provider_seq, incoming_seq) {
            (Some(prev), Some(inc)) => {
                if inc > prev {
                    return true;
                }
                if inc < prev {
                    return false;
                }
                // equal seq: precedence
                incoming_state.precedence() >= self.state.precedence()
            }
            (None, Some(_)) => true,
            (_, None) => true, // if no seq, accept (assume arrival order)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::securities::symbols::Instrument;

    #[test]
    fn order_state_precedence_ordering() {
        assert!(OrderState::Rejected.precedence() > OrderState::Canceled.precedence());
        assert!(OrderState::Canceled.precedence() > OrderState::Filled.precedence());
        assert!(OrderState::Filled.precedence() > OrderState::PartiallyFilled.precedence());
        assert!(OrderState::PartiallyFilled.precedence() > OrderState::Acknowledged.precedence());
        assert!(OrderState::Acknowledged.precedence() > OrderState::New.precedence());
    }

    #[test]
    fn can_apply_seq_greater_allows() {
        let inst = Instrument::try_from("ESZ5").unwrap();
        let mut o = Order::new(inst, Side::Buy, 5);
        o.last_provider_seq = Some(10);
        // incoming seq higher
        assert!(o.can_apply(Some(11), OrderState::Acknowledged));
        // incoming seq lower blocked
        assert!(!o.can_apply(Some(9), OrderState::Filled));
    }

    #[test]
    fn can_apply_equal_seq_uses_precedence() {
        let inst = Instrument::try_from("ESZ5").unwrap();
        let mut o = Order::new(inst, Side::Buy, 5);
        o.state = OrderState::Acknowledged;
        o.last_provider_seq = Some(42);
        // same seq, higher precedence (Filled) should be accepted
        assert!(o.can_apply(Some(42), OrderState::Filled));
        // same seq, lower precedence (New) should be rejected
        assert!(!o.can_apply(Some(42), OrderState::New));
    }

    #[test]
    fn can_apply_without_seq_defaults_to_accept() {
        let inst = Instrument::try_from("ESZ5").unwrap();
        let o = Order::new(inst, Side::Buy, 1);
        assert!(o.can_apply(None, OrderState::Acknowledged));
    }
}
