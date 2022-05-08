use futures::{
    future::try_join_all,
    stream::{StreamExt, TryStreamExt},
};
use rand::RngCore;
use rusoto_core::Region;
use rusoto_s3::{
    CreateBucketRequest, DeleteBucketRequest, DeleteObjectRequest, PutObjectRequest, S3Client, S3,
};
use s3_ext::{error::S3ExtError, S3Ext};
use std::env;
use tokio::io::AsyncReadExt;

#[tokio::test(flavor = "multi_thread")]
async fn test_iter_example() -> Result<(), S3ExtError> {
    let bucket = format!(
        "s3-ext-iter-module-example-{}",
        rand::thread_rng().next_u64()
    );

    // setup client
    let client = if let Ok(endpoint) = env::var("S3_ENDPOINT") {
        let access_key = "ANTN35UAENTS5UIAEATD".to_string();
        let secret_key = "TtnuieannGt2rGuie2t8Tt7urarg5nauedRndrur".to_string();
        let region = Region::Custom {
            name: "eu-west-1".to_string(),
            endpoint,
        };
        s3_ext::new_s3client_with_credentials(region, access_key, secret_key)?
    } else {
        S3Client::new(Region::UsEast1)
    };

    // create bucket

    client
        .create_bucket(CreateBucketRequest {
            bucket: bucket.clone(),
            ..Default::default()
        })
        .await?;

    // create test objects
    let mut keys = Vec::new();
    for obj in (0..5).map(|n| format!("object_{:02}", n)) {
        client
            .put_object(PutObjectRequest {
                bucket: bucket.clone(),
                key: obj.to_string(),
                body: Some(obj.as_bytes().to_vec().into()),
                ..Default::default()
            })
            .await
            .map(|_| ())?;
        keys.push(obj);
    }

    // iterate over objects objects (sorted alphabetically)
    let objects: Vec<_> = client
        .stream_objects(&bucket)
        .into_stream()
        .map(|res| res.map(|obj| obj.key))
        .try_collect()
        .await?;
    let objects: Vec<_> = objects.into_iter().filter_map(|x| x).collect();

    assert_eq!(
        objects.as_slice(),
        &[
            "object_00",
            "object_01",
            "object_02",
            "object_03",
            "object_04",
        ]
    );

    // iterate object and fetch content on the fly (sorted alphabetically)
    let results: Result<Vec<_>, _> = client
        .stream_get_objects(&bucket)
        .into_stream()
        .map(|res| res.map(|(key, obj)| (key, obj.body)))
        .try_collect()
        .await;

    let futures: Vec<_> = results?
        .into_iter()
        .map(|(key, body)| async move {
            let mut buf = Vec::new();
            if let Some(body) = body {
                match body.into_async_read().read_to_end(&mut buf).await {
                    Ok(_) => Ok(Some((key, buf))),
                    Err(e) => Err(e),
                }
            } else {
                Ok(None)
            }
        })
        .collect();
    let results: Result<Vec<_>, _> = try_join_all(futures).await;
    let bodies: Vec<_> = results?.into_iter().filter_map(|x| x).collect();

    for key in keys {
        client
            .delete_object(DeleteObjectRequest {
                bucket: bucket.clone(),
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

    assert_eq!(
        bodies.as_slice(),
        &[
            ("object_00".to_string(), b"object_00".to_vec()),
            ("object_01".to_string(), b"object_01".to_vec()),
            ("object_02".to_string(), b"object_02".to_vec()),
            ("object_03".to_string(), b"object_03".to_vec()),
            ("object_04".to_string(), b"object_04".to_vec()),
        ]
    );
    Ok(())
}
