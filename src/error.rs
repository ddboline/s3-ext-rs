use rusoto_core::{request::TlsError, HttpDispatchError, RusotoError};
use rusoto_s3::{
    CompleteMultipartUploadError, CreateBucketError, CreateMultipartUploadError, GetObjectError,
    ListObjectsV2Error, PutObjectError, UploadPartError,
};
use std::io::Error as IoError;
use thiserror::Error;

pub type S4Result<T> = Result<T, S4Error>;

/// Errors returned by S4 extensions to Rusoto
#[derive(Debug, Error)]
pub enum S4Error {
    /// Unknown error
    #[error("Unknown error {0}")]
    Other(&'static str),

    /// I/O Error
    #[error("I/O Error {0}")]
    IoError(#[from] IoError),

    /// Rusoto CompleteMultipartUploadError
    #[error("Rusoto CompleteMultipartUploadError {0}")]
    CompleteMultipartUploadError(#[from] RusotoError<CompleteMultipartUploadError>),

    /// Rusoto CreateMultipartUploadError
    #[error("Rusoto CreateMultipartUploadError {0}")]
    CreateMultipartUploadError(#[from] RusotoError<CreateMultipartUploadError>),

    /// Rusoto GetObjectError
    #[error("Rusoto GetObjectError {0}")]
    GetObjectError(#[from] RusotoError<GetObjectError>),

    /// Rusoto HttpDispatchError
    #[error("Rusoto HttpDispatchError {0}")]
    HttpDispatchError(#[from] RusotoError<HttpDispatchError>),

    /// Rusoto ListObjectV2Error
    #[error("Rusoto ListObjectV2Error {0}")]
    ListObjectV2Error(#[from] RusotoError<ListObjectsV2Error>),

    /// Rusoto PutObjectError
    #[error("Rusoto PutObjectError {0}")]
    PutObjectError(#[from] RusotoError<PutObjectError>),

    /// Rusoto UploadPartError
    #[error("Rusoto UploadPartError {0}")]
    UploadPartError(#[from] RusotoError<UploadPartError>),

    /// Rusoto CreateBucketError
    #[error("Rusoto CreateBucketError {0}")]
    CreateBucketError(#[from] RusotoError<CreateBucketError>),

    /// Rusoto request TlsError
    #[error("Rusoto TlsError {0}")]
    TlsError(#[from] TlsError),
}
