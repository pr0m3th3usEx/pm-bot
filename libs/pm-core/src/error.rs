use thiserror::Error;

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("unknown variant: {0}")]
    UnknownVariant(String),
    #[error("illegal state transition: {0}")]
    IllegalTransition(String),
    #[error("store error: {0}")]
    Store(String),
    #[error("adapter error: {0}")]
    Adapter(String),
}

pub type Result<T> = std::result::Result<T, CoreError>;
