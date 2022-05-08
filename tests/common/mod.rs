#![allow(dead_code)]

use core::{
    pin::Pin,
    task::{Context, Poll},
};
use rand::{distributions::Alphanumeric, Rng};
use rusoto_core::Region;
use rusoto_s3::{
    CreateBucketRequest, DeleteBucketRequest, DeleteObjectRequest, GetObjectRequest,
    PutObjectRequest, S3Client, S3,
};
use s3_ext::new_s3client_with_credentials;
use std::env;
use sts_profile_auth::get_client_sts;
use tokio::io::{self, AsyncRead, AsyncReadExt, ReadBuf};

pub fn get_client() -> S3Client {
    if let Ok(endpoint) = env::var("S3_ENDPOINT") {
        new_s3client_with_credentials(
            Region::Custom {
                name: "eu-west-1".to_owned(),
                endpoint,
            },
            "ANTN35UAENTS5UIAEATD".to_owned(),
            "TtnuieannGt2rGuie2t8Tt7urarg5nauedRndrur".to_owned(),
        )
        .unwrap()
    } else {
        get_client_sts!(S3Client, Region::UsEast1).unwrap()
    }
}

pub async fn create_test_bucket(client: &S3Client) -> String {
    let bucket: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(56)
        .map(|x| x as char)
        .collect();
    let bucket = bucket.to_lowercase();
    let bucket = format!("s3-ext-{bucket}");
    println!("{bucket} {}", bucket.len());

    client
        .create_bucket(CreateBucketRequest {
            bucket: bucket.clone(),
            ..Default::default()
        })
        .await
        .unwrap();

    bucket
}

pub async fn delete_test_bucket(client: &S3Client, bucket: &str, keys: &[&str]) {
    for key in keys {
        client
            .delete_object(DeleteObjectRequest {
                bucket: bucket.into(),
                key: key.to_string(),
                ..Default::default()
            })
            .await
            .unwrap();
    }
    client
        .delete_bucket(DeleteBucketRequest {
            bucket: bucket.into(),
            ..Default::default()
        })
        .await
        .unwrap();
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
        buf: &mut ReadBuf,
    ) -> Poll<io::Result<()>> {
        if buf.filled().len() > self.abort_after {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::Other,
                "explicit, unconditional error",
            )));
        }
        for i in buf.filled_mut().iter_mut() {
            *i = 0;
        }
        self.abort_after -= buf.filled().len();
        Poll::Ready(Ok(()))
    }
}
