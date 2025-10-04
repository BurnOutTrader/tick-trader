use std::fmt::Display;
use std::hash::Hash;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

#[derive(Debug, Clone, PartialEq, Eq, Copy, Hash, Archive, RkyvDeserialize, RkyvSerialize)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub enum ProjectXTenant {
    Topstep,
    AlphaFutures,
    Demo
}

impl From<&str> for ProjectXTenant {
    fn from(s: &str) -> Self {
        match s {
            "topstep" => ProjectXTenant::Topstep,
            "alphafutures" => ProjectXTenant::AlphaFutures,
            "demo" => ProjectXTenant::Demo,
            _ => panic!("invalid ProjectX tenant: {}", s),
        }
    }
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy, Archive, RkyvDeserialize, RkyvSerialize)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub enum RithmicAffiliation {
    Topstep,
    Apex,
}

impl RithmicAffiliation {
    pub fn to_id_segment(&self) -> String {
        match self {
            RithmicAffiliation::Topstep => "topstep".to_string(),
            RithmicAffiliation::Apex => "apex".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq,Copy, Hash, Archive, RkyvDeserialize, RkyvSerialize)]
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
    
    pub fn to_u64(&self) -> u64 {
        match self {
            ProviderKind::ProjectX(b) => {
                match b {
                    ProjectXTenant::Topstep => 11,
                    ProjectXTenant::AlphaFutures => 12,
                    ProjectXTenant::Demo => 13,
                }
            }
            ProviderKind::Rithmic(b) => {
                match b {
                    RithmicAffiliation::Topstep => 54,
                    RithmicAffiliation::Apex => 55,
                }
            }
        }
    }
    
    pub fn from_u64(id: u64) -> Option<ProviderKind> {
        match id {
            11 => Some(ProviderKind::ProjectX(ProjectXTenant::Topstep)),
            12 => Some(ProviderKind::ProjectX(ProjectXTenant::AlphaFutures)),
            13 => Some(ProviderKind::ProjectX(ProjectXTenant::Demo)),
            54 => Some(ProviderKind::Rithmic(RithmicAffiliation::Topstep)),
            55 => Some(ProviderKind::Rithmic(RithmicAffiliation::Apex)),
            _ => None,
        }
    }
}
