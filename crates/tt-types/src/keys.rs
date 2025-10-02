use std::collections::HashMap;
use std::sync::RwLock;
use once_cell::sync::Lazy;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq, Copy, Eq, Hash)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct TopicId(pub u8);

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq, Copy, Eq, Hash)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub enum Topic{
    Ticks = 1,
    Quotes = 2,
    Depth = 3,
    Bars1s = 4,
    Bars1m = 5,
    Bars1h = 6,
    Bars1d = 7,
    AccountEvt = 8,
    Positions = 9,
    Orders = 10,
    Fills = 11,
}

impl Topic {
    pub fn id(self) -> TopicId {
        use crate::keys::Topic::{AccountEvt, Bars1d, Bars1h, Bars1m, Bars1s, Depth, Fills, Orders, Positions, Quotes, Ticks};
        let id = match self {
            Ticks => 1,
            Quotes => 2,
            Depth => 3,
            Bars1s => 4,
            Bars1m => 5,
            Bars1h => 6,
            Bars1d => 7,
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

fn escape(input: &str, special: char) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '\\' => {
                out.push('\\');
                out.push('\\');
            }
            c if c == special => {
                out.push('\\');
                out.push(c);
            }
            c => out.push(c),
        }
    }
    out
}

fn unescape(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut it = input.chars();
    while let Some(c) = it.next() {
        if c == '\\' {
            if let Some(n) = it.next() {
                out.push(n);
            } else {
                out.push('\\');
            }
        } else {
            out.push(c);
        }
    }
    out
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SymbolKey {
    pub instrument: String,     // UPPER
    pub provider: String,       // lower
    pub broker: Option<String>, // lower
}

impl SymbolKey {
    pub fn parse(s: &str) -> anyhow::Result<Self> {
        // split by '.' with escapes
        let mut parts = Vec::new();
        let mut cur = String::new();
        let mut esc = false;
        for ch in s.chars() {
            if esc {
                cur.push(ch);
                esc = false;
                continue;
            }
            match ch {
                '\\' => esc = true,
                '.' => {
                    parts.push(cur.clone());
                    cur.clear();
                }
                c => cur.push(c),
            }
        }
        parts.push(cur);
        if parts.len() < 2 {
            anyhow::bail!("invalid symbol key format");
        }
        let instrument = unescape(&parts[0]).to_uppercase();
        let provider = unescape(&parts[1]).to_lowercase();
        // Support multiple affiliation segments after provider (e.g., rithmic.topstep)
        let broker = if parts.len() > 2 {
            let tail: Vec<String> = parts[2..]
                .iter()
                .map(|p| unescape(p).to_lowercase())
                .collect();
            Some(tail.join("."))
        } else {
            None
        };
        Ok(SymbolKey {
            instrument,
            provider,
            broker,
        })
    }

    pub fn to_string_wire(&self) -> String {
        let mut s = String::new();
        s.push_str(&escape(&self.instrument.to_uppercase(), '.'));
        s.push('.');
        s.push_str(&escape(&self.provider.to_lowercase(), '.'));
        if let Some(b) = &self.broker {
            // Emit broker chain segments individually to preserve multi-level providers
            for seg in b.split('.') {
                s.push('.');
                s.push_str(&escape(&seg.to_lowercase(), '.'));
            }
        }
        s
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AccountKey {
    pub provider: String,       // lower
    pub broker: Option<String>, // lower
    pub account_id: String,     // as-is
    pub env: Option<String>,
}

impl AccountKey {
    pub fn parse(s: &str) -> anyhow::Result<Self> {
        if !s.starts_with("acct:") {
            anyhow::bail!("missing acct: prefix");
        }
        let rest = &s[5..];
        // split by ':' handling escapes
        let mut parts = Vec::new();
        let mut cur = String::new();
        let mut esc = false;
        for ch in rest.chars() {
            if esc {
                cur.push(ch);
                esc = false;
                continue;
            }
            match ch {
                '\\' => esc = true,
                ':' => {
                    parts.push(cur.clone());
                    cur.clear();
                }
                c => cur.push(c),
            }
        }
        parts.push(cur);
        if parts.len() < 2 {
            anyhow::bail!("invalid account key format");
        }
        // Layout rules:
        // - len==2: provider, account_id
        // - len==3: provider, account_id, env
        // - len>=4: provider, broker chain..., account_id, env
        let provider = unescape(&parts[0]).to_lowercase();
        let (broker_chain_range, account_idx, env): (std::ops::Range<usize>, usize, Option<String>) = match parts.len() {
            2 => (1..1, 1, None),
            3 => (1..1, 1, Some(unescape(&parts[2]))),
            _ => {
                let env = Some(unescape(&parts[parts.len() - 1]));
                let acc_idx = parts.len() - 2;
                (1..acc_idx, acc_idx, env)
            }
        };
        let account_id = unescape(&parts[account_idx]);
        // Broker chain is any segments between provider and account_id for len>=4
        let broker = if broker_chain_range.start < broker_chain_range.end {
            let tail: Vec<String> = parts[broker_chain_range]
                .iter()
                .map(|p| unescape(p).to_lowercase())
                .collect();
            Some(tail.join("."))
        } else {
            None
        };
        Ok(AccountKey {
            provider,
            broker,
            account_id,
            env,
        })
    }

    pub fn to_string_wire(&self) -> String {
        let mut s = String::from("acct:");
        s.push_str(&escape(&self.provider.to_lowercase(), ':'));
        if let Some(b) = &self.broker {
            for seg in b.split('.') {
                s.push(':');
                s.push_str(&escape(&seg.to_lowercase(), ':'));
            }
        }
        s.push(':');
        // account id as-is with escaping ':' and '\\'
        s.push_str(&escape(&self.account_id, ':'));
        if let Some(env) = &self.env {
            s.push(':');
            s.push_str(&escape(env, ':'));
        }
        s
    }
}

pub fn stream_id(topic: TopicId, key: KeyId) -> u64 {
    ((topic.0 as u64) << 32) | key.0 as u64
}

#[cfg(test)]
mod tests {
    use crate::keys::{AccountKey, SymbolKey};

    #[test]
    fn symbol_round_trip() {
        let s = "es.futu.hk";
        let k = SymbolKey::parse(s).unwrap();
        assert_eq!(k.instrument, "ES");
        assert_eq!(k.provider, "futu");
        assert_eq!(k.broker.as_deref(), Some("hk"));
        let s2 = k.to_string_wire();
        assert_eq!(s2, "ES.futu.hk");
    }

    #[test]
    fn account_round_trip() {
        let s = "acct:ib:DU12345:paper";
        let k = AccountKey::parse(s).unwrap();
        assert_eq!(k.provider, "ib");
        assert_eq!(k.broker, None);
        assert_eq!(k.account_id, "DU12345");
        assert_eq!(k.env.as_deref(), Some("paper"));
        let s2 = k.to_string_wire();
        assert_eq!(s2, s);
    }

    #[test]
    fn escapes() {
        let s = "MNQ\\.Z.CME.rithmic.topstep"; // instrument has a dot escaped, rithmic with broker specified
        let k = SymbolKey::parse(s).unwrap();
        assert_eq!(k.instrument, "MNQ.Z");
        // provider lowercased, broker lowercased; dot in instrument stays escaped on wire
        assert_eq!(k.provider, "cme");
        assert_eq!(k.broker.as_deref(), Some("rithmic.topstep"));
        assert_eq!(k.to_string_wire(), "MNQ\\.Z.cme.rithmic.topstep");

        let a = "acct:ibkr:DU\\:123:live";
        let k2 = AccountKey::parse(a).unwrap();
        assert_eq!(k2.account_id, "DU:123");
        assert_eq!(k2.to_string_wire(), a);

        let a2 = "acct:rithmic:top:step:DU12345:paper"; // multi-level broker chain for accounts
        let k3 = AccountKey::parse(a2).unwrap();
        assert_eq!(k3.provider, "rithmic");
        assert_eq!(k3.broker.as_deref(), Some("top.step"));
        assert_eq!(k3.account_id, "DU12345");
        assert_eq!(k3.env.as_deref(), Some("paper"));
        assert_eq!(k3.to_string_wire(), "acct:rithmic:top:step:DU12345:paper");
    }
}