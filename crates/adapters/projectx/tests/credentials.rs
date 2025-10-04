use projectx::http::credentials::PxCredential;
use tt_types::providers::ProjectXTenant;

const FIRM: ProjectXTenant =  ProjectXTenant::Topstep;
const USER_NAME: &str = "example_user";
const API_KEY: &str = "example_api_key_123";

#[test]
fn test_new_constructs_fields() {
    let cred = PxCredential::new(FIRM, USER_NAME.to_string(), API_KEY.to_string());
    assert_eq!(cred.firm, FIRM);
    assert_eq!(cred.user_name, USER_NAME);
    // api_key is a Ustr; compare by as_str()
    assert_eq!(cred.api_key.as_str(), API_KEY);
}

#[test]
fn test_debug_redacts_secret() {
    let cred = PxCredential::new(FIRM, USER_NAME.to_string(), API_KEY.to_string());
    let dbg_out = format!("{:?}", cred);

    // Ensure redaction markers are present
    assert!(dbg_out.contains("user_name: \"<redacted>\""));
    assert!(dbg_out.contains("api_key: \"<redacted>\""));

    // Ensure raw values are not leaked
    assert!(!dbg_out.contains(USER_NAME));
    assert!(!dbg_out.contains(API_KEY));

    // Ensure raw bytes of secret are not leaked either
    let api_key_bytes_dbg = format!("{:?}", API_KEY.as_bytes());
    assert!(
        !dbg_out.contains(&api_key_bytes_dbg),
        "Debug output must not contain raw secret bytes"
    );
}
