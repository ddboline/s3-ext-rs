//! Simpler Simple Storage Service: high-level API extensions for Rusoto's
//! `S3Client`
//!
//! # TLS Support
//!
//! For TLS support [`rustls`](https://crates.io/crates/rustls) is used by default.
//!
//! Alternatively, [`native-tls`](https://crates.io/crates/native-tls) can be used by
//! disabling the default features and enabling the `native-tls` feature.
//!
//! For instance, like this:
//!
//! ```toml
//! [dependencies]
//! s3_ext = { version = "…", default_features = false }
//!
//! [features]
//! default = ["s3_ext/native-tls"]
//! ```

#![allow(clippy::default_trait_access)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::redundant_closure)]
#![allow(clippy::redundant_closure_for_method_calls)]
#![allow(clippy::should_implement_trait)]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::type_repetition_in_bounds)]

pub mod iter;
use crate::iter::{GetObjectStream, ObjectStream};
pub mod error;
use crate::error::{S3ExtError, S3ExtResult};
mod upload;

use async_trait::async_trait;
use log::debug;
use rusoto_core::{
    request::{HttpClient, TlsError},
    Region,
};
use rusoto_credential::StaticProvider;
use rusoto_s3::{
    CompleteMultipartUploadOutput, GetObjectOutput, GetObjectRequest, PutObjectOutput,
    PutObjectRequest, S3Client, StreamingBody, S3,
};
use std::{convert::AsRef, path::Path};
use tokio::{
    fs::{File, OpenOptions},
    io,
};

/// Create client using given static access/secret keys
pub fn new_s3client_with_credentials(
    region: Region,
    access_key: impl Into<String>,
    secret_key: impl Into<String>,
) -> Result<S3Client, TlsError> {
    Ok(S3Client::new_with(
        HttpClient::new()?,
        StaticProvider::new_minimal(access_key.into(), secret_key.into()),
        region,
    ))
}

#[async_trait]
pub trait S3Ext {
    /// Get object and write it to file `target`
    async fn download_to_file<F>(
        &self,
        source: GetObjectRequest,
        target: F,
    ) -> S3ExtResult<GetObjectOutput>
    where
        F: AsRef<Path> + Send + Sync;

    /// Upload content of file to S3
    ///
    /// # Caveats
    ///
    /// The current implementation is incomplete. For now, the following
    /// limitation applies:
    ///
    /// * The full content of `source` is copied into memory.
    async fn upload_from_file<F>(
        &self,
        source: F,
        target: PutObjectRequest,
    ) -> S3ExtResult<PutObjectOutput>
    where
        F: AsRef<Path> + Send + Sync;

    /// Upload content of file to S3 using multi-part upload
    ///
    /// # Caveats
    ///
    /// The current implementation is incomplete. For now, the following
    /// limitation applies:
    ///
    /// * The full content of a part is copied into memory.
    async fn upload_from_file_multipart<F>(
        &self,
        source: F,
        target: PutObjectRequest,
        part_size: usize,
    ) -> S3ExtResult<CompleteMultipartUploadOutput>
    where
        F: AsRef<Path> + Send + Sync;

    /// Get object and write it to `target`
    async fn download<W>(
        &self,
        source: GetObjectRequest,
        target: &mut W,
    ) -> S3ExtResult<GetObjectOutput>
    where
        W: io::AsyncWrite + Unpin + Send;

    /// Read `source` and upload it to S3
    ///
    /// # Caveats
    ///
    /// The current implementation is incomplete. For now, the following
    /// limitation applies:
    ///
    /// * The full content of `source` is copied into memory.
    async fn upload<R>(
        &self,
        source: &mut R,
        target: PutObjectRequest,
    ) -> S3ExtResult<PutObjectOutput>
    where
        R: io::AsyncRead + Unpin + Send;

    /// Read `source` and upload it to S3 using multi-part upload
    ///
    /// # Caveats
    ///
    /// The current implementation is incomplete. For now, the following
    /// limitation applies:
    ///
    /// * The full content of a part is copied into memory.
    async fn upload_multipart<R>(
        &self,
        source: &mut R,
        target: PutObjectRequest,
        part_size: usize,
    ) -> S3ExtResult<CompleteMultipartUploadOutput>
    where
        R: io::AsyncRead + Unpin + Send;

    /// Stream over all objects
    /// Access to an iterator-like object `ObjectIter` can be obtained by
    /// calling into_iter()
    ///
    /// Objects are lexicographically sorted by their key.
    fn stream_objects(&self, bucket: impl Into<String>) -> ObjectStream;

    /// Stream over objects with given `prefix`
    ///
    /// Objects are lexicographically sorted by their key.
    fn stream_objects_with_prefix(&self, bucket: impl Into<String>, prefix: impl Into<String>) -> ObjectStream;

    /// Stream over all objects; fetching objects as needed
    ///
    /// Objects are lexicographically sorted by their key.
    fn stream_get_objects(&self, bucket: impl Into<String>) -> GetObjectStream;

    /// Stream over objects with given `prefix`; fetching objects as needed
    ///
    /// Objects are lexicographically sorted by their key.
    fn stream_get_objects_with_prefix(&self, bucket: impl Into<String>, prefix: impl Into<String>) -> GetObjectStream;
}

#[async_trait]
impl S3Ext for S3Client {
    async fn download_to_file<F>(
        &self,
        source: GetObjectRequest,
        target: F,
    ) -> Result<GetObjectOutput, S3ExtError>
    where
        F: AsRef<Path> + Send + Sync,
    {
        debug!("downloading to file {:?}", target.as_ref());
        let mut resp = self.get_object(source).await?;
        let body = resp.body.take().expect("no body");
        let mut target = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(target)
            .await?;
        copy(body, &mut target).await?;
        Ok(resp)
    }

    #[inline]
    async fn upload_from_file<F>(
        &self,
        source: F,
        target: PutObjectRequest,
    ) -> S3ExtResult<PutObjectOutput>
    where
        F: AsRef<Path> + Send + Sync,
    {
        debug!("uploading file {:?}", source.as_ref());
        let mut source = File::open(source).await?;
        upload::upload(self, &mut source, target).await
    }

    #[inline]
    async fn upload_from_file_multipart<F>(
        &self,
        source: F,
        target: PutObjectRequest,
        part_size: usize,
    ) -> S3ExtResult<CompleteMultipartUploadOutput>
    where
        F: AsRef<Path> + Send + Sync,
    {
        debug!("uploading file {:?}", source.as_ref());
        let mut source = File::open(source).await?;
        upload::upload_multipart(self, &mut source, target, part_size).await
    }

    async fn download<W>(
        &self,
        source: GetObjectRequest,
        mut target: &mut W,
    ) -> S3ExtResult<GetObjectOutput>
    where
        W: io::AsyncWrite + Unpin + Send,
    {
        let mut resp = self.get_object(source).await?;
        let body = resp.body.take().expect("no body");
        copy(body, &mut target).await?;
        Ok(resp)
    }

    #[inline]
    async fn upload<R>(
        &self,
        source: &mut R,
        target: PutObjectRequest,
    ) -> S3ExtResult<PutObjectOutput>
    where
        R: io::AsyncRead + Unpin + Send,
    {
        upload::upload(self, source, target).await
    }

    #[inline]
    async fn upload_multipart<R>(
        &self,
        mut source: &mut R,
        target: PutObjectRequest,
        part_size: usize,
    ) -> S3ExtResult<CompleteMultipartUploadOutput>
    where
        R: io::AsyncRead + Unpin + Send,
    {
        upload::upload_multipart(self, &mut source, target, part_size).await
    }

    #[inline]
    fn stream_objects(&self, bucket: impl Into<String>) -> ObjectStream {
        ObjectStream::new(self, bucket, None as Option<&str>)
    }

    #[inline]
    fn stream_objects_with_prefix(&self, bucket: impl Into<String>, prefix: impl Into<String>) -> ObjectStream {
        ObjectStream::new(self, bucket, Some(prefix))
    }

    #[inline]
    fn stream_get_objects(&self, bucket: impl Into<String>) -> GetObjectStream {
        GetObjectStream::new(self, bucket, None as Option<&str>)
    }

    #[inline]
    fn stream_get_objects_with_prefix(&self, bucket: impl Into<String>, prefix: impl Into<String>) -> GetObjectStream {
        GetObjectStream::new(self, bucket, Some(prefix))
    }
}

async fn copy<W>(src: StreamingBody, dest: &mut W) -> S3ExtResult<()>
where
    W: io::AsyncWrite + Unpin + Send,
{
    io::copy(&mut src.into_async_read(), dest).await?;
    Ok(())
}
