use std::{num::NonZeroU32, sync::LazyLock};
use ustr::Ustr;

pub const PROJECT_X: &str = "ProjectX";

pub static DEMO_DOMAIN: &str = "https://gateway-api-demo.s2f.projectx.com";
pub static DEMO_STREAM: &str = "https://gateway-rtc-demo.s2f.projectx.com";

// Global rate-limit key namespace for ProjectX HTTP client
pub const PX_GLOBAL_RATE_KEY: &str = "px:global";

pub static PX_REST_QUOTA: LazyLock<Quota> =
    LazyLock::new(|| Quota::per_minute(NonZeroU32::new(200).unwrap()));

pub static PX_BARS_QUOTA: LazyLock<Quota> = LazyLock::new(|| {
    Quota::with_period(std::time::Duration::from_secs(30))
        .unwrap()
        .allow_burst(NonZeroU32::new(50).unwrap())
});
