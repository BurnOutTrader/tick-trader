use projectx::http::models::AccountSearchReq;

#[test]
fn account_search_req_serializes_camel_case() {
    let req = AccountSearchReq {
        only_active_accounts: true,
    };
    let json = serde_json::to_string(&req).expect("serialize");
    assert!(json.contains("onlyActiveAccounts"));
    assert!(json.contains("true"));
}
