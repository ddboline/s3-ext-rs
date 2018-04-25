extern crate fallible_iterator;
extern crate futures;
extern crate rand;
extern crate rusoto_core;
extern crate rusoto_s3;
extern crate s4;

use fallible_iterator::FallibleIterator;
use futures::stream::Stream;
use futures::Future;
use rand::Rng;
use rusoto_core::Region;
use rusoto_s3::{CreateBucketRequest, PutObjectRequest, S3};
use s4::S4;

#[test]
fn main() {
    let bucket = format!("iter-module-example-{}", rand::thread_rng().next_u64());

    // setup client

    let access_key = "ANTN35UAENTS5UIAEATD".to_string();
    let secret_key = "TtnuieannGt2rGuie2t8Tt7urarg5nauedRndrur".to_string();
    let region = Region::Custom {
        name: "eu-west-1".to_string(),
        endpoint: "http://localhost:9000".to_string(),
    };
    let client = s4::new_s3client_with_credentials(region, access_key, secret_key);

    // create bucket

    client
        .create_bucket(&CreateBucketRequest {
            bucket: bucket.clone(),
            ..Default::default()
        })
        .sync()
        .expect("failed to create bucket");

    // create test objects

    for obj in (0..5).map(|n| format!("object_{:02}", n)) {
        client
            .put_object(&PutObjectRequest {
                bucket: bucket.clone(),
                key: obj.to_string(),
                body: Some(obj.as_bytes().to_vec()),
                ..Default::default()
            })
            .sync()
            .expect("failed to store object");
    }

    // iterate over objects objects (sorted alphabetically)

    let objects: Vec<_> = client
        .iter_objects(&bucket)
        .map(|obj| obj.key.unwrap())
        .collect()
        .expect("failed to fetch list of objects");

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

    let bodies: Vec<_> = client
        .iter_get_objects(&bucket)
        .map(|(key, obj)| (key, obj.body.unwrap().concat2().wait().unwrap()))
        .collect()
        .expect("failed to fetch content");

    assert_eq!(
        bodies,
        &[
            ("object_00".to_string(), b"object_00".to_vec()),
            ("object_01".to_string(), b"object_01".to_vec()),
            ("object_02".to_string(), b"object_02".to_vec()),
            ("object_03".to_string(), b"object_03".to_vec()),
            ("object_04".to_string(), b"object_04".to_vec()),
        ]
    );
}
