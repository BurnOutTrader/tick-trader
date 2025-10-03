use std::str::FromStr;
use rust_decimal::Decimal;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

#[derive(Archive, RkyvDeserialize, RkyvSerialize)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AccountId(i64);
impl AccountId {
    pub fn new(id: i64) -> Self {
        Self(id)
    }
    pub fn to_string(&self) -> String {
        self.0.to_string()
    }
    pub fn to_i64(&self) -> i64 {
        self.0
    }
    pub fn is_valid(&self) -> bool {}
}
impl FromStr for AccountId {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(i64::from_str(s).map_err(|e| format!("Invalid AccountId: {}", e))?))
    }
}

#[derive(Archive, RkyvDeserialize, RkyvSerialize)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

#[derive(Archive, RkyvDeserialize, RkyvSerialize)]
#[derive(Debug, Clone, PartialEq)]
pub struct AccountSnapShot {
    pub name: AccountName,
    pub id: AccountId,
    #[rkyv(with = crate::rkyv_types::DecimalDef)]
    pub balance: Decimal,
    pub can_trade: bool,
}