#![allow(dead_code)]

use core::{
    pin::Pin,
    task::{Context, Poll},
};
use rand::{distributions::Alphanumeric, Rng};
use rusoto_core::Region;
use rusoto_s3::{CreateBucketRequest, GetObjectRequest, PutObjectRequest, S3Client, S3};
use s3_ext::new_s3client_with_credentials;
use std::env;
use tokio::io::{self, AsyncRead, AsyncReadExt};

pub async fn create_test_bucket() -> (S3Client, String) {
    let endpoint = env::var("S3_ENDPOINT").unwrap_or_else(|_| "http://localhost:9000".to_string());
    let client = new_s3client_with_credentials(
        Region::Custom {
            name: "eu-west-1".to_owned(),
            endpoint,
        },
        "ANTN35UAENTS5UIAEATD".to_owned(),
        "TtnuieannGt2rGuie2t8Tt7urarg5nauedRndrur".to_owned(),
    )
    .unwrap();
    let bucket: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(63)
        .map(|x| x as char)
        .collect();
    let bucket = bucket.to_lowercase();

    client
        .create_bucket(CreateBucketRequest {
            bucket: bucket.clone(),
            ..Default::default()
        })
        .await
        .unwrap();

    (client, bucket)
}

pub async fn put_object(client: &S3Client, bucket: &str, key: &str, data: Vec<u8>) {
    client
        .put_object(PutObjectRequest {
            bucket: bucket.to_string(),
            key: key.to_string(),
            body: Some(data.into()),
            ..Default::default()
        })
        .await
        .unwrap();
}

pub async fn get_body(client: &S3Client, bucket: &str, key: &str) -> Vec<u8> {
    let object = client
        .get_object(GetObjectRequest {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            ..Default::default()
        })
        .await
        .unwrap();
    let mut body = Vec::new();
    object
        .body
        .unwrap()
        .into_async_read()
        .read_to_end(&mut body)
        .await
        .unwrap();
    body
}

pub fn init_logger() {
    let _ = env_logger::Builder::from_default_env()
        .filter(Some("s3_ext"), log::LevelFilter::Debug)
        .try_init();
}

pub struct ReaderWithError {
    pub abort_after: usize,
}

impl AsyncRead for ReaderWithError {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        if buf.len() > self.abort_after {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::Other,
                "explicit, unconditional error",
            )));
        }
        for i in buf.iter_mut() {
            *i = 0;
        }
        self.abort_after -= buf.len();
        Poll::Ready(Ok(buf.len()))
    }
}
