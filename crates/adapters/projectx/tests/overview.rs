use projectx::http::inner_client::PxHttpInnerClient;

#[test]
fn rate_limit_overview_contains_expected_text() {
    let text = PxHttpInnerClient::rate_limit_overview();
    assert!(text.contains("Overview"));
    assert!(text.contains("rate limiting system"));
    assert!(text.contains("POST /api/History/retrieveBars"));
    assert!(text.contains("50 requests / 30 seconds"));
    assert!(text.contains("200 requests / 60 seconds"));
    assert!(text.contains("HTTP 429"));
}
