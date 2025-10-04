use projectx::http::models::CloseContractReq;

#[test]
fn close_contract_req_serializes_camel_case() {
    let req = CloseContractReq {
        account_id: 536,
        contract_id: "CON.F.US.GMET.J25".to_string(),
    };
    let json = serde_json::to_string(&req).expect("serialize");
    assert!(json.contains("\"accountId\""));
    assert!(json.contains("\"contractId\""));
}
