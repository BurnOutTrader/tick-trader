pub mod common;
#[cfg(feature = "adapter-extra")]
pub mod data;
#[cfg(feature = "adapter-extra")]
pub mod execution;
pub mod http;
#[cfg(feature = "adapter-extra")]
pub mod websocket;

// Consolidated client aliases
#[cfg(feature = "adapter-extra")]
pub type PxRestClient = http::client::PxHttpClient;
#[cfg(feature = "adapter-extra")]
pub type PxStreamingClient = websocket::PxWebSocketClient;
