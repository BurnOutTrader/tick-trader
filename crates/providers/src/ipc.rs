//! Out-of-process ProviderWorker scaffolding.
//! This module defines a minimal command protocol and a placeholder worker that would
//! communicate with a separate process over UDS/TCP in a future pass.

use anyhow::Result;
use async_trait::async_trait;
use tt_types::keys::{SymbolKey, Topic};

#[derive(Debug, Clone)]
pub enum ProviderCmd {
    SubscribeMd { topic: Topic, key: SymbolKey },
    UnsubscribeMd { topic: Topic, key: SymbolKey },
}

#[async_trait]
pub trait ProviderProcess: Send + Sync {
    async fn send(&self, cmd: ProviderCmd) -> Result<()>;
}

/// Minimal placeholder that just stubs the API; real implementation will
/// marshal commands to a child process and manage its lifecycle.
pub struct OutOfProcessWorker {
    proc: Box<dyn ProviderProcess>,
}

impl OutOfProcessWorker {
    pub fn new(proc: Box<dyn ProviderProcess>) -> Self { Self { proc } }
}

#[async_trait]
impl super::worker::ProviderWorker for OutOfProcessWorker {
    async fn subscribe_md(&self, topic: Topic, key: &SymbolKey) -> Result<()> {
        self.proc.send(ProviderCmd::SubscribeMd { topic, key: key.clone() }).await
    }
    async fn unsubscribe_md(&self, topic: Topic, key: &SymbolKey) -> Result<()> {
        self.proc.send(ProviderCmd::UnsubscribeMd { topic, key: key.clone() }).await
    }
}
