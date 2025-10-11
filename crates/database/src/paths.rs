use tt_types::providers::ProviderKind;

/// Map ProviderKind to a stable short code string used in the database.
/// This should be considered part of the storage contract; changing it would
/// require a migration.
pub fn provider_kind_to_db_string(provider: ProviderKind) -> String {
    match provider {
        ProviderKind::ProjectX(_) => "projectx".to_string(),
        ProviderKind::Rithmic(_) => "rithmic".to_string(),
    }
}
