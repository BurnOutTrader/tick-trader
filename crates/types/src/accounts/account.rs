use rust_decimal::Decimal;
use std::fmt::Display;
use std::str::FromStr;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Archive, RkyvDeserialize, RkyvSerialize)]
pub struct AccountName(String);
impl FromStr for AccountName {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.to_string()))
    }
}
impl AccountName {
    pub fn new(name: String) -> Self {
        AccountName(name)
    }
}

impl Display for AccountName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for AccountName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Archive, RkyvDeserialize, RkyvSerialize)]
pub struct AccountSnapShot {
    pub name: AccountName,
    pub id: i64,
    #[rkyv(with = crate::rkyv_types::DecimalDef)]
    pub balance: Decimal,
    pub can_trade: bool,
}
