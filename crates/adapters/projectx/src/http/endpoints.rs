use crate::common::consts::{DEMO_DOMAIN, DEMO_STREAM};

#[derive(Clone, Debug)]
pub struct PxEndpoints {
    pub api_base: String,
    pub user_hub: String,
    pub market_hub: String,
}

impl PxEndpoints {
    /// Build endpoints given a firm name (e.g. "topstepx").
    /// [from api docs](https://gateway.docs.projectx.com/docs/getting-started/authenticate/authenticate-api-key)
    pub fn from_firm(firm: &str) -> Self {
        let f = firm.trim().to_lowercase();
        if firm == "demo" {
            return PxEndpoints {
                api_base: DEMO_DOMAIN.to_string(),
                user_hub: format!("{DEMO_STREAM}/hubs/user"),
                market_hub: format!("{DEMO_STREAM}/hubs/market"),
            };
        }
        let api_base = format!("https://api.{f}.com",);
        let hub_base = format!("https://rtc.{f}.com",);
        PxEndpoints {
            api_base: api_base.clone(),
            user_hub: format!("{hub_base}/hubs/user"),
            market_hub: format!("{hub_base}/hubs/market"),
        }
    }
}
