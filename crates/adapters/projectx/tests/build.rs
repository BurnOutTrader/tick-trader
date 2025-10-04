use projectx::http::client::PxHttpInnerClient;
use projectx::http::credentials::PxCredential;

#[test]
fn builds_client_with_dummy_config() {
    // This test should not perform any network I/O. It only ensures that the
    // ProjectX HTTP client can be constructed without panicking given a
    // minimally valid config structure.
    let cfg = PxCredential {
        firm: "example_firm".to_string(),
        user_name: "example_user".to_string(),
        api_key: "example_key".into(),
    };

    let _client = PxHttpInnerClient::new_default(cfg).unwrap();
    // If construction succeeds without panic, the test passes.
}
