use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use strum_macros::Display;

#[derive(Debug, Clone, Display, Copy, PartialEq, Eq, Archive, RkyvDeserialize, RkyvSerialize)]
#[archive(check_bytes)]
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

    pub fn is_eol(&self) -> bool {
        match self {
            OrderState::Canceled => true,
            OrderState::Filled => true,
            OrderState::PartiallyFilled => false,
            OrderState::Acknowledged => false,
            OrderState::New => false,
            OrderState::Rejected => true,
        }
    }
}
