use rusoto_core::{HttpDispatchError, RusotoError};
use rusoto_s3::{
    CompleteMultipartUploadError, CreateMultipartUploadError, GetObjectError, ListObjectsV2Error,
    PutObjectError, UploadPartError,
};
use std::io::Error as IoError;

pub type S4Result<T> = Result<T, S4Error>;

/// Errors returned by S4 extensions to Rusoto
#[derive(Debug, Error)]
pub enum S4Error {
    /// Unknown error
    #[error(no_from, non_std)]
    Other(&'static str),

    /// I/O Error
    IoError(IoError),

    /// Rusoto CompleteMultipartUploadError
    CompleteMultipartUploadError(RusotoError<CompleteMultipartUploadError>),

    /// Rusoto CreateMultipartUploadError
    CreateMultipartUploadError(RusotoError<CreateMultipartUploadError>),

    /// Rusoto GetObjectError
    GetObjectError(RusotoError<GetObjectError>),

    /// Rusoto HttpDispatchError
    HttpDispatchError(RusotoError<HttpDispatchError>),

    /// Rusoto ListObjectV2Error
    ListObjectV2Error(RusotoError<ListObjectsV2Error>),

    /// Rusoto PutObjectError
    PutObjectError(RusotoError<PutObjectError>),

    /// Rusoto UploadPartError
    UploadPartError(RusotoError<UploadPartError>),
}
