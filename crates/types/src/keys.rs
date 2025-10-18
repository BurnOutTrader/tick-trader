use crate::accounts::account::AccountName;
use crate::providers::ProviderKind;
use crate::securities::symbols::Instrument;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::RwLock;

use crate::data::models::Resolution;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[archive(check_bytes)]
pub struct TopicId(pub u8);

#[derive(
    Archive,
    RkyvDeserialize,
    RkyvSerialize,
    Debug,
    PartialEq,
    Clone,
    Copy,
    Eq,
    Hash,
    Ord,
    PartialOrd,
)]
#[archive(check_bytes)]
pub enum Topic {
    Ticks = 1,
    Quotes = 2,
    MBP10 = 3,
    Candles1s = 4,
    MBP1 = 14,
    Candles1m = 5,
    Candles1h = 6,
    Candles1d = 7,
    AccountEvt = 8,
    Positions = 9,
    Orders = 10,
    Fills = 11,
    // Umbrella topics for subscription convenience
    MarketData = 12,
    Other = 13,
}

impl Display for Topic {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ticks => write!(f, "Ticks"),
            Self::Quotes => write!(f, "Quotes"),
            Self::MBP10 => write!(f, "MBP10"),
            Self::Candles1s => write!(f, "Candles1s"),
            Self::Candles1m => write!(f, "Candles1m"),
            Self::Candles1h => write!(f, "Candles1h"),
            Self::Candles1d => write!(f, "Candles1d"),
            Self::AccountEvt => write!(f, "AccountEvent"),
            Self::Positions => write!(f, "Positions"),
            Self::Orders => write!(f, "Orders"),
            Self::Fills => write!(f, "Fills"),
            Self::MarketData => write!(f, "MarketData"),
            Self::Other => write!(f, "Other"),
            Self::MBP1 => write!(f, "MBP1"),
        }
    }
}

impl Topic {
    pub fn id(self) -> TopicId {
        use crate::keys::Topic::{
            AccountEvt, Candles1d, Candles1h, Candles1m, Candles1s, Fills, MBP1, MBP10, MarketData,
            Orders, Other, Positions, Quotes, Ticks,
        };
        let id = match self {
            Ticks => 1,
            Quotes => 2,
            MBP10 => 3,
            Candles1s => 4,
            Candles1m => 5,
            Candles1h => 6,
            Candles1d => 7,
            AccountEvt => 8,
            Positions => 9,
            Orders => 10,
            Fills => 11,
            MarketData => 12,
            Other => 13,
            MBP1 => 14,
        };
        TopicId(id)
    }

    pub fn is_market_data(&self) -> bool {
        matches!(
            self,
            Topic::Ticks
                | Topic::Quotes
                | Topic::MBP10
                | Topic::Candles1s
                | Topic::Candles1m
                | Topic::Candles1h
                | Topic::Candles1d
        )
    }
    pub fn to_resolution(&self) -> Option<Resolution> {
        match self {
            Self::Candles1s => Some(Resolution::Seconds(1)),
            Self::Candles1m => Some(Resolution::Minutes(1)),
            Self::Candles1h => Some(Resolution::Hours(1)),
            Self::Candles1d => Some(Resolution::Daily),
            _ => None,
        }
    }

    pub fn from_resolution(resolution: Resolution) -> Option<Topic> {
        match resolution {
            Resolution::Seconds(1) => Some(Topic::Candles1s),
            Resolution::Minutes(1) => Some(Topic::Candles1m),
            Resolution::Hours(1) => Some(Topic::Candles1h),
            Resolution::Daily => Some(Topic::Candles1d),
            _ => None,
        }
    }
}

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[archive(check_bytes)]
pub struct KeyId(pub u32);

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[archive(check_bytes)]
pub struct ProviderId(pub u16);

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[archive(check_bytes)]
pub struct BrokerId(pub u16);

static INTERN: Lazy<RwLock<Interner>> = Lazy::new(|| RwLock::new(Interner::default()));

#[derive(Default)]
struct Interner {
    map: HashMap<String, u32>,
    rev: Vec<String>,
}

impl Interner {
    fn intern(&mut self, s: &str) -> KeyId {
        if let Some(&id) = self.map.get(s) {
            return KeyId(id);
        }
        let id = self.rev.len() as u32 + 1; // 0 reserved
        self.map.insert(s.to_string(), id);
        self.rev.push(s.to_string());
        KeyId(id)
    }

    fn resolve(&self, id: KeyId) -> Option<&str> {
        let i = id.0 as usize;
        if i == 0 {
            return None;
        }
        self.rev.get(i - 1).map(|s| s.as_str())
    }
}

pub fn intern_key(s: &str) -> KeyId {
    let mut g = INTERN.write().unwrap();
    g.intern(s)
}

pub fn resolve_key(id: KeyId) -> Option<String> {
    let g = INTERN.read().unwrap();
    g.resolve(id).map(|s| s.to_string())
}

#[derive(
    Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd,
)]
#[archive(check_bytes)]
pub struct SymbolKey {
    pub instrument: Instrument, // UPPER
    pub provider: ProviderKind, // provider kind (may include tenant/affiliation)
}
impl SymbolKey {
    pub fn new(instrument: Instrument, provider: ProviderKind) -> Self {
        Self {
            instrument,
            provider,
        }
    }
}

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq, Eq, Hash)]
#[archive(check_bytes)]
pub struct AccountKey {
    pub provider: ProviderKind,    // provider kind
    pub account_name: AccountName, // as-is
}

impl AccountKey {
    pub fn new(provider: ProviderKind, account_name: AccountName) -> Self {
        Self {
            provider,
            account_name,
        }
    }
}

pub fn stream_id(topic: TopicId, key: KeyId) -> u64 {
    ((topic.0 as u64) << 32) | key.0 as u64
}
