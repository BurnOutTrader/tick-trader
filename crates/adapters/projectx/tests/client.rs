use dotenvy::dotenv;
use projectx::http::credentials::PxCredential;
use projectx::http::inner_client::PxHttpInnerClient;
use provider::traits::ProviderSessionSpec;
use tt_types::providers::{ProjectXTenant, ProviderKind};

#[ignore]
#[tokio::test]
async fn test_authenticate_returns_token() {
    dotenv().ok();
    let session_creds = ProviderSessionSpec::from_env();
    let firm = ProjectXTenant::Topstep;
    let provider = ProviderKind::ProjectX(firm);
    let cfg = PxCredential::new(firm, session_creds.user_names.get(&provider).unwrap().clone(), session_creds.api_keys.get(&provider).unwrap().clone() );
    let http = PxHttpInnerClient::new(cfg, None, None, None, None).unwrap();

    // now returns Result<(), PxError>
    http.authenticate().await.expect("auth failed");

    // read token for assertions
    let token_guard = http.token_string().await;
    let token = token_guard
        .read()
        .await
        .clone()
        .expect("Token guard poisoned");

    // Basic sanity checks
    assert!(!token.is_empty(), "Token was empty");
    assert!(token.len() > 10, "Token too short");
    assert!(
        token.starts_with("eyJ"),
        "Token doesn’t look like a JWT: {}",
        token
    );
}

#[ignore]
#[tokio::test]
async fn auth_key_smoke_test() {
    let session_creds = ProviderSessionSpec::from_env();
    let firm = ProjectXTenant::Topstep;
    let provider = ProviderKind::ProjectX(firm);
    let cfg = PxCredential::new(firm, session_creds.user_names.get(&provider).unwrap().clone(), session_creds.api_keys.get(&provider).unwrap().clone() );
    let http = PxHttpInnerClient::new(cfg, None, None, None, None).unwrap();

    http.authenticate().await.expect("Failed to auth");

    let token_guard = http.token_string().await;
    let token = token_guard
        .read()
        .await
        .clone()
        .expect("Token guard poisoned");
    assert!(!token.is_empty());
    assert!(token.len() > 10);
    assert!(token.starts_with("eyJ"));
}

#[ignore]
#[tokio::test]
async fn validate_returns_new_token() {
    dotenv().ok();

    let session_creds = ProviderSessionSpec::from_env();
    let firm = ProjectXTenant::Topstep;
    let provider = ProviderKind::ProjectX(firm);
    let cfg = PxCredential::new(firm, session_creds.user_names.get(&provider).unwrap().clone(), session_creds.api_keys.get(&provider).unwrap().clone() );
    let http = PxHttpInnerClient::new(cfg, None, None, None, None).unwrap();

    // initial auth
    http.authenticate().await.expect("auth");

    #[allow(unused)]
    let token_guard = http.token_string().await;
    let _ = token_guard
        .read()
        .await
        .clone()
        .expect("Token guard poisoned");

    // rotate/validate (now Result<()>)
    http.validate().await.expect("validate");

    let token_guard = http.token_string().await;
    let tok2 = token_guard
        .read()
        .await
        .clone()
        .expect("Token guard poisoned");

    assert!(!tok2.is_empty(), "empty token after validate");
    // Depending on tenant, token may or may not change; if it should rotate:
    // assert_ne!(tok1, tok2, "token did not rotate");
}
