use std::fmt::Debug;
use ustr::Ustr;
use tt_types::providers::ProjectXTenant;

/// ProjectX credentials used to authenticate HTTP requests
///
/// Fields:
/// - firm: the ProjectX firm identifier
/// - user_name: the ProjectX login user name, redacted in Debug
/// - api_key: the ProjectX API key, stored as Ustr and redacted in Debug.
#[derive(Clone)]
pub struct PxCredential {
    pub firm: ProjectXTenant,
    pub user_name: String,
    pub api_key: Ustr,
}

impl PxCredential {
    /// Create a new credential from explicit values
    pub fn new(firm: ProjectXTenant, user_name: String, api_key: String) -> Self {
        Self {
            firm,
            user_name,
            api_key: api_key.into(),
        }
    }

    /// Load credentials from the process environment
    ///
    /// Expected variables: PX_FIRM, PX_USERNAME, PX_API_KEY.
    pub fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            firm: ProjectXTenant::from(std::env::var("PX_FIRM")?.as_str()),
            user_name: std::env::var("PX_USERNAME")?,
            api_key: std::env::var("PX_API_KEY")?.into(),
        })
    }
}

impl Debug for PxCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(Credential))
            .field("firm", &self.firm)
            .field("user_name", &"<redacted>")
            .field("api_key", &"<redacted>")
            .finish()
    }
}
