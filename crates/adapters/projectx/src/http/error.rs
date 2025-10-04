use thiserror::Error;

#[derive(Debug, Error)]
pub enum PxError {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("authentication failed: {0}")]
    Auth(String),
    #[error("unexpected http status: {status}, body: {body}")]
    UnexpectedStatus { status: u16, body: String },
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
