use crate::accounts::account::AccountName;
use crate::providers::ProviderKind;
use crate::securities::symbols::Instrument;
use once_cell::sync::Lazy;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::RwLock;

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq, Copy, Eq, Hash)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct TopicId(pub u8);

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq, Copy, Eq, Hash)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub enum Topic {
    Ticks = 1,
    Quotes = 2,
    Depth = 3,
    Candles1s = 4,
    Candles1m = 5,
    Candles1h = 6,
    Candles1d = 7,
    AccountEvt = 8,
    Positions = 9,
    Orders = 10,
    Fills = 11,
}

impl Display for Topic {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ticks => write!(f, "Ticks"),
            Self::Quotes => write!(f, "Quotes"),
            Self::Depth => write!(f, "Depth"),
            Self::Candles1s => write!(f, "Candles1s"),
            Self::Candles1m => write!(f, "Candles1m"),
            Self::Candles1h => write!(f, "Candles1h"),
            Self::Candles1d => write!(f, "Candles1d"),
            Self::AccountEvt => write!(f, "AccountEvent"),
            Self::Positions => write!(f, "Positions"),
            Self::Orders => write!(f, "Orders"),
            Self::Fills => write!(f, "Fills"),
        }
    }
}

impl Topic {
    pub fn id(self) -> TopicId {
        use crate::keys::Topic::{
            AccountEvt, Candles1d, Candles1h, Candles1m, Candles1s, Depth, Fills, Orders, Positions, Quotes,
            Ticks,
        };
        let id = match self {
            Ticks => 1,
            Quotes => 2,
            Depth => 3,
            Candles1s => 4,
            Candles1m => 5,
            Candles1h => 6,
            Candles1d => 7,
            AccountEvt => 8,
            Positions => 9,
            Orders => 10,
            Fills => 11,
        };
        TopicId(id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct KeyId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ProviderId(pub u16);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct SymbolKey {
    pub instrument: Instrument, // UPPER
    pub provider: ProviderKind, // provider kind (may include tenant/affiliation)
}

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct AccountKey {
    pub provider: ProviderKind,    // provider kind
    pub account_name: AccountName, // as-is
}

pub fn stream_id(topic: TopicId, key: KeyId) -> u64 {
    ((topic.0 as u64) << 32) | key.0 as u64
}
