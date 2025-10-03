use crate::reality_models::inbuilt::defaults::{
    DefaultFeeModel, DefaultFill, DefaultFuturesBP, DefaultSettle, DefaultSlippage, DefaultVol,
};
use crate::reality_models::traits::{
    BuyingPowerModel, FeeModel, FillModel, SettlementModel, SlippageModel, VolatilityModel,
};
use crate::securities::symbols::{Exchange, SecurityType};
use std::sync::Arc;

#[derive(Clone)]
pub struct ModelSet {
    pub fee: Arc<dyn FeeModel>,
    pub slip: Arc<dyn SlippageModel>,
    pub fill: Arc<dyn FillModel>,
    pub bp: Arc<dyn BuyingPowerModel>,
    pub vol: Arc<dyn VolatilityModel>,
    pub settle: Arc<dyn SettlementModel>,
}

impl Default for ModelSet {
    fn default() -> Self {
        Self {
            fee: Arc::new(DefaultFeeModel),
            slip: Arc::new(DefaultSlippage),
            fill: Arc::new(DefaultFill),
            bp: Arc::new(DefaultFuturesBP),
            vol: Arc::new(DefaultVol),
            settle: Arc::new(DefaultSettle),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ModelKey {
    pub security_type: SecurityType,
    pub exchange: Option<Exchange>,   // e.g., Some(Exchange::CME)
    pub product_root: Option<String>, // e.g., Some("MNQ") for futures root
}

#[derive(Clone, Default)]
pub struct ModelCatalog {
    // Most-specific rule wins; we’ll resolve with a fallback ladder.
    by_key: std::collections::HashMap<ModelKey, ModelSet>,
    by_kind: std::collections::HashMap<SecurityType, ModelSet>,
    default_set: ModelSet,
}

impl ModelCatalog {
    pub fn with_default(default: ModelSet) -> Self {
        Self {
            default_set: default,
            ..Default::default()
        }
    }

    pub fn set_for_kind(&mut self, security_type: SecurityType, set: ModelSet) {
        self.by_kind.insert(security_type, set);
    }
    pub fn set_for_key(&mut self, key: ModelKey, set: ModelSet) {
        self.by_key.insert(key, set);
    }

    /// Resolution order (most → least specific):
    /// 1) (kind, exchange, product_root)
    /// 2) (kind, exchange, None)
    /// 3) (kind, None, None)
    /// 4) default_set
    pub fn select(
        &self,
        security_type: SecurityType,
        exchange: Option<Exchange>,
        product_root: Option<&str>,
    ) -> &ModelSet {
        if let Some(root) = product_root {
            let key = ModelKey {
                security_type,
                exchange,
                product_root: Some(root.to_string()),
            };
            if let Some(ms) = self.by_key.get(&key) {
                return ms;
            }
        }
        if let Some(ms) = self.by_key.get(&ModelKey {
            security_type,
            exchange,
            product_root: None,
        }) {
            return ms;
        }
        if let Some(ms) = self.by_kind.get(&security_type) {
            return ms;
        }
        &self.default_set
    }
}
