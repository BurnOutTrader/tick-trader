use std::fmt::Display;
use std::hash::Hash;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use strum_macros::{Display};
use zeroize::{Zeroize, ZeroizeOnDrop};

#[derive(Debug, Clone, PartialEq, Eq, Copy, Hash, Archive, RkyvDeserialize, RkyvSerialize)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub enum ProjectXTenant {
    Topstep,
    AlphaFutures,
    Demo
}

impl Display for ProjectXTenant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProjectXTenant::Topstep => write!(f, "topstep"),
            ProjectXTenant::AlphaFutures => write!(f, "alphafutures"),
            ProjectXTenant::Demo => write!(f, "demo"),
        }
    }
}

impl ProjectXTenant {
    pub fn from_env_string(s: &str) -> Self {
        let binding = s.to_lowercase();
        let s = binding.as_str();
        match s {
            "topstep" => ProjectXTenant::Topstep,
            "alphafutures" => ProjectXTenant::AlphaFutures,
            "demo" => ProjectXTenant::Demo,
            _ => panic!("invalid ProjectX tenant: {}", s),
        }
    }
    pub fn to_id_segment(&self) -> String {
        match self {
            ProjectXTenant::Topstep => "topstep".to_string(),
            ProjectXTenant::AlphaFutures => "alphafutures".to_string(),
            ProjectXTenant::Demo => "demo".to_string(),
        }
    }

    /// Base HTTPS URL for ProjectX APIs for this tenant.
    /// For Custom, returns the string verbatim assuming it's already a URL; if not, caller may build it.
    pub fn to_url_string(&self) -> String {
        match self {
            ProjectXTenant::Topstep => "https://api.projectx.topstep.com".to_string(),
            ProjectXTenant::AlphaFutures => "https://api.projectx.alphafutures.com".to_string(),
            ProjectXTenant::Demo => "https://api.projectx.demo.com".to_string(),
        }
    }

    pub fn to_platform_name(&self) -> String {
        match self {
            ProjectXTenant::Topstep => "topstepx".to_string(),
            ProjectXTenant::AlphaFutures => "alphaticks".to_string(),
            ProjectXTenant::Demo => "demo".to_string(),
        }
    }
}

impl RithmicSystem {
    pub fn to_id_segment(&self) -> String {
        match self {
            RithmicSystem::TopstepTrader => "topstep".to_string(),
            RithmicSystem::Apex => "apex".to_string(),
            _ => unimplemented!()
        }
    }
}


#[derive(Deserialize)]
pub struct RithmicCredentials {
    pub(crate) user: String,
    pub(crate) server_name: RithmicServer,
    pub(crate) system_name: RithmicSystem,
    pub(crate) app_name: String,
    pub(crate) app_version: String,
    pub(crate) password: String,
    pub(crate) fcm_id: Option<String>,
    pub(crate) ib_id: Option<String>,
    pub(crate) user_type: Option<i32>,
}

impl RithmicCredentials {
    pub fn new(
        user: String,
        server_name: RithmicServer,
        system_name: RithmicSystem,
        app_name: String,
        app_version: String,
        password: String,
        fcm_id: Option<String>,
        ib_id: Option<String>,
        user_type: Option<i32>,
    ) -> Self {
        Self {
            user,
            server_name,
            system_name,
            app_name,
            app_version,
            password,
            fcm_id,
            ib_id,
            user_type,
        }
    }
}


#[derive(
    Serialize,
    Deserialize,
    Clone,
    Eq,
    PartialEq,
    Debug,
    Hash,
    PartialOrd,
    Ord,
    Display,
    Copy,
    Archive, RkyvDeserialize, RkyvSerialize,
)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub enum RithmicSystem {
    #[strum(serialize = "Rithmic 04 Colo")]
    Rithmic04Colo,
    #[strum(serialize = "Rithmic 01")]
    Rithmic01,
    #[strum(serialize = "Rithmic Paper Trading")]
    RithmicPaperTrading,
    #[strum(serialize = "TopstepTrader")]
    TopstepTrader,
    #[strum(serialize = "SpeedUp")]
    SpeedUp,
    #[strum(serialize = "TradeFundrr")]
    TradeFundrr,
    #[strum(serialize = "UProfitTrader")]
    UProfitTrader,
    #[strum(serialize = "Apex")]
    Apex,
    #[strum(serialize = "MES Capital")]
    MESCapital,
    #[strum(serialize = "The Trading Pit")]
    TheTradingPit,
    #[strum(serialize = "Funded Futures Network")]
    FundedFuturesNetwork,
    #[strum(serialize = "Bulenox")]
    Bulenox,
    #[strum(serialize = "PropShopTrader")]
    PropShopTrader,
    #[strum(serialize = "4PropTrader")]
    FourPropTrader,
    #[strum(serialize = "FastTrackTrading")]
    FastTrackTrading,
    #[strum(serialize = "Test")]
    Test,
}

impl RithmicSystem {
    pub fn from_env_string(env_str: &str) -> Option<Self> {
        let binding = env_str.to_lowercase();
        let s = binding.as_str();
        match s {
            "RITHMIC_04_COLO" => Some(RithmicSystem::Rithmic04Colo),
            "RITHMIC_01" => Some(RithmicSystem::Rithmic01),
            "RITHMIC_PAPER_TRADING" => Some(RithmicSystem::RithmicPaperTrading),
            "TOPSTEPTRADER" => Some(RithmicSystem::TopstepTrader),
            "SPEEDUP" => Some(RithmicSystem::SpeedUp),
            "TRADEFUNDRR" => Some(RithmicSystem::TradeFundrr),
            "UPROFITTRADER" => Some(RithmicSystem::UProfitTrader),
            "APEX" => Some(RithmicSystem::Apex),
            "MESCAPITAL" => Some(RithmicSystem::MESCapital),
            "THETRADINGPIT" => Some(RithmicSystem::TheTradingPit),
            "FUNDENDFUTURESNETWORK" => Some(RithmicSystem::FundedFuturesNetwork),
            "BULENOX" => Some(RithmicSystem::Bulenox),
            "PROPSHOPTRADER" => Some(RithmicSystem::PropShopTrader),
            "4PROPTRADER" => Some(RithmicSystem::FourPropTrader),
            "FASTTRACKTRADING" => Some(RithmicSystem::FastTrackTrading),
            "TEST" => Some(RithmicSystem::Test),
            _ => None,
        }
    }
}

#[derive(
    Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Hash, PartialOrd, Ord, Display, Archive, RkyvDeserialize, RkyvSerialize,
)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub enum RithmicServer {
    Chicago,
    Sydney,
    SaoPaolo,
    Colo75,
    Frankfurt,
    HongKong,
    Ireland,
    Mumbai,
    Seoul,
    CapeTown,
    Tokyo,
    Singapore,
    Test,
}

#[derive(Debug, Clone, PartialEq, Eq,Copy, Hash, Archive, RkyvDeserialize, RkyvSerialize)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub enum ProviderKind {
    ProjectX(ProjectXTenant),
    Rithmic(RithmicSystem),
}

impl ProviderKind {
    /// For providers that use HTTP base URLs (ProjectX), return a URL; others may return an empty string.
    pub fn to_url_string(&self) -> Option<String> {
        match self {
            ProviderKind::ProjectX(t) => Some(t.to_url_string()),
            ProviderKind::Rithmic(_) => unimplemented!(),
        }
    }
}
