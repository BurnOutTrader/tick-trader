use tt_types::providers::ProviderKind;

/// Map ProviderKind to a stable short code string used in the database.
/// This should be considered part of the storage contract; changing it would
/// require a migration.
pub fn provider_kind_to_db_string(provider: ProviderKind) -> String {
    //dont change, we dont want the tenants here only the ProviderKind eg ProjectX or Rithmic not their wrapped enums for specific firms
    match provider {
        ProviderKind::ProjectX(_) => "projectx".to_string(),
        ProviderKind::Rithmic(_) => "rithmic".to_string(),
    }
}
