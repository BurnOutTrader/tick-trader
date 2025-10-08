use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use std::fmt::Display;
use std::hash::Hash;
use strum_macros::Display;

#[derive(Clone, Debug, PartialEq, Eq, Copy, Hash, Ord, PartialOrd, Archive, RkyvDeserialize, RkyvSerialize)]
#[archive(check_bytes)]
pub enum ProjectXTenant {
    Topstep,
    AlphaFutures,
    Demo,
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
            _ => unimplemented!(),
        }
    }
}

#[derive(RkyvDeserialize, RkyvSerialize, Archive)]
#[archive(check_bytes)]
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
    Archive,
    RkyvDeserialize,
    RkyvSerialize,
    Clone,
    Eq,
    PartialEq,
    Hash,
    PartialOrd,
    Ord,
    Display,
    Copy,
    Debug,
)]
#[archive(check_bytes)]
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
            "rithmic_04_colo" => Some(RithmicSystem::Rithmic04Colo),
            "rithmic_01" => Some(RithmicSystem::Rithmic01),
            "rithmic_paper_trading" => Some(RithmicSystem::RithmicPaperTrading),
            "topsteptrader" => Some(RithmicSystem::TopstepTrader),
            "speedup" => Some(RithmicSystem::SpeedUp),
            "tradefundrr" => Some(RithmicSystem::TradeFundrr),
            "uprofittrader" => Some(RithmicSystem::UProfitTrader),
            "apex" => Some(RithmicSystem::Apex),
            "mescapital" => Some(RithmicSystem::MESCapital),
            "thetradingpit" => Some(RithmicSystem::TheTradingPit),
            "fundendfuturesnetwork" => Some(RithmicSystem::FundedFuturesNetwork),
            "bulenox" => Some(RithmicSystem::Bulenox),
            "propshoptrader" => Some(RithmicSystem::PropShopTrader),
            "4proptrader" => Some(RithmicSystem::FourPropTrader),
            "fasttracktrading" => Some(RithmicSystem::FastTrackTrading),
            "test" => Some(RithmicSystem::Test),
            _ => None,
        }
    }
}

#[derive(
    Archive, RkyvDeserialize, RkyvSerialize, Clone, Eq, PartialEq, Hash, PartialOrd, Ord, Display,
)]
#[archive(check_bytes)]
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

#[derive(Clone, Debug,  Eq, Ord, PartialOrd, PartialEq, Copy, Hash, Archive, RkyvDeserialize, RkyvSerialize)]
#[archive(check_bytes)]
pub enum ProviderKind {
    ProjectX(ProjectXTenant),
    Rithmic(RithmicSystem),
}
