use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Archive, RkyvDeserialize, RkyvSerialize)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub enum ProjectXTenant {
    Topstep,
    AlphaFutures,
    /// Custom provider affinity; treated as literal provider id or URL segment
    Custom(String),
}

impl ProjectXTenant {
    pub fn to_id_segment(&self) -> String {
        match self {
            ProjectXTenant::Topstep => "topstep".to_string(),
            ProjectXTenant::AlphaFutures => "alphafutures".to_string(),
            ProjectXTenant::Custom(s) => s.to_lowercase(),
        }
    }

    /// Base HTTPS URL for ProjectX APIs for this tenant.
    /// For Custom, returns the string verbatim assuming it's already a URL; if not, caller may build it.
    pub fn to_url_string(&self) -> String {
        match self {
            ProjectXTenant::Topstep => "https://api.projectx.topstep.com".to_string(),
            ProjectXTenant::AlphaFutures => "https://api.projectx.alphafutures.com".to_string(),
            ProjectXTenant::Custom(s) => s.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Archive, RkyvDeserialize, RkyvSerialize)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub enum RithmicAffiliation {
    Topstep,
    Apex,
    Generic(String),
}

impl RithmicAffiliation {
    pub fn to_id_segment(&self) -> String {
        match self {
            RithmicAffiliation::Topstep => "topstep".to_string(),
            RithmicAffiliation::Apex => "apex".to_string(),
            RithmicAffiliation::Generic(s) => s.to_lowercase(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Archive, RkyvDeserialize, RkyvSerialize)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub enum ProviderKind {
    ProjectX(ProjectXTenant),
    Rithmic(RithmicAffiliation),
}

impl ProviderKind {
    /// Provider id string used across bus/keys, e.g., "projectx.topstep" or "rithmic.apex"
    pub fn to_id_string(&self) -> String {
        match self {
            ProviderKind::ProjectX(t) => format!("projectx.{}", t.to_id_segment()),
            ProviderKind::Rithmic(a) => format!("rithmic.{}", a.to_id_segment()),
        }
    }

    /// For providers that use HTTP base URLs (ProjectX), return a URL; others may return an empty string.
    pub fn to_url_string(&self) -> Option<String> {
        match self {
            ProviderKind::ProjectX(t) => Some(t.to_url_string()),
            ProviderKind::Rithmic(_) => None,
        }
    }
}
