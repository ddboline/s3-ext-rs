mod common;
use crate::common::ReaderWithError;

use log::warn;
use rand::{thread_rng, Rng, RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use rusoto_core::RusotoError;
use rusoto_s3::{
    GetObjectError, GetObjectRequest, ListMultipartUploadsRequest, PutObjectRequest, S3,
};
use s3_ext::{error::S3ExtError, S3Ext};
use tempdir::TempDir;
use tokio::{
    fs::File,
    io::{self, AsyncReadExt, ErrorKind},
};

const NUMBER_OF_TESTS: usize = 10;

#[tokio::test(flavor = "multi_thread")]
async fn target_file_already_exists() {
    let client = common::get_client();
    let bucket = common::create_test_bucket(&client).await;
    let key = "abcd";

    common::put_object(&client, &bucket, key, vec![]).await;

    let result = client
        .download_to_file(
            GetObjectRequest {
                bucket: bucket.clone(),
                key: key.to_owned(),
                ..Default::default()
            },
            file!(),
        )
        .await;

    common::delete_test_bucket(&client, &bucket, &[key]).await;

    match result {
        Err(S3ExtError::IoError(ref e)) if e.kind() == ErrorKind::AlreadyExists => (),
        e => panic!("unexpected result: {:?}", e),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn target_file_not_created_when_object_does_not_exist() {
    let client = common::get_client();
    let bucket = common::create_test_bucket(&client).await;

    let dir = TempDir::new("").unwrap();
    let file = dir.path().join("no_such_file");

    let result = client
        .download_to_file(
            GetObjectRequest {
                bucket: bucket.clone(),
                key: "no_such_key".to_owned(),
                ..Default::default()
            },
            &file,
        )
        .await;

    common::delete_test_bucket(&client, &bucket, &[]).await;

    match result {
        Err(S3ExtError::GetObjectError(RusotoError::Service(GetObjectError::NoSuchKey(_)))) => (),
        e => panic!("unexpected result: {:?}", e),
    }
    assert!(
        !file.exists(),
        "target file created even though getting the object failed"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_download_to_file() {
    async fn download_to_file(data: Vec<u8>) -> bool {
        let client = common::get_client();
        let bucket = common::create_test_bucket(&client).await;

        let dir = TempDir::new("").unwrap();
        let file = dir.path().join("data");
        let key = "some_key";

        common::put_object(&client, &bucket, key, data.clone()).await;

        let resp = client
            .download_to_file(
                GetObjectRequest {
                    bucket: bucket.clone(),
                    key: key.to_owned(),
                    ..Default::default()
                },
                &file,
            )
            .await
            .unwrap();

        assert_eq!(resp.content_length, Some(data.len() as i64));
        let mut buf = Vec::new();
        File::open(&file)
            .await
            .unwrap()
            .read_to_end(&mut buf)
            .await
            .unwrap();

        common::delete_test_bucket(&client, &bucket, &[key]).await;

        assert_eq!(buf, data);
        true
    }
    let mut gen = thread_rng();
    for _ in 0..NUMBER_OF_TESTS {
        let mut data = vec![0u8; 4096];
        gen.fill_bytes(&mut data);
        assert!(download_to_file(data).await);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_download() {
    async fn download(data: Vec<u8>) -> bool {
        let client = common::get_client();
        let bucket = common::create_test_bucket(&client).await;

        let key = "abc/def/ghi";
        let mut target = Vec::new();

        common::put_object(&client, &bucket, key, data.clone()).await;

        let resp = client
            .download(
                GetObjectRequest {
                    bucket: bucket.clone(),
                    key: key.to_owned(),
                    ..Default::default()
                },
                &mut target,
            )
            .await
            .unwrap();

        common::delete_test_bucket(&client, &bucket, &[key]).await;

        assert_eq!(resp.content_length, Some(data.len() as i64));
        assert_eq!(data, target);
        true
    }
    let mut gen = thread_rng();
    for _ in 0..NUMBER_OF_TESTS {
        let mut data = vec![0u8; 4096];
        gen.fill_bytes(&mut data);
        assert!(download(data).await);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn download_large_object() {
    let client = common::get_client();
    let bucket = common::create_test_bucket(&client).await;

    let key = "abc/def/ghi";
    let mut data = vec![0; 104_857_601];
    XorShiftRng::from_entropy().fill_bytes(data.as_mut());
    let mut target = Vec::new();

    common::put_object(&client, &bucket, key, data.clone()).await;

    let resp = client
        .download(
            GetObjectRequest {
                bucket: bucket.clone(),
                key: key.to_owned(),
                ..Default::default()
            },
            &mut target,
        )
        .await
        .unwrap();

    common::delete_test_bucket(&client, &bucket, &[key]).await;

    assert_eq!(resp.content_length, Some(data.len() as i64));
    assert_eq!(&data[..], &target[..]);
}

#[tokio::test(flavor = "multi_thread")]
async fn no_object_created_when_file_cannot_be_opened_for_upload() {
    let client = common::get_client();
    let bucket = common::create_test_bucket(&client).await;

    let result = client
        .upload_from_file(
            "/no_such_file_or_directory_0V185rt1LhV2WwZdveEM",
            PutObjectRequest {
                bucket: "unused_bucket_name".to_string(),
                key: "key".to_string(),
                ..Default::default()
            },
        )
        .await;
    common::delete_test_bucket(&client, &bucket, &[]).await;
    match result {
        Err(S3ExtError::IoError(ref e)) if e.kind() == io::ErrorKind::NotFound => (),
        r => panic!("unexpected result: {:?}", r),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn upload() {
    let client = common::get_client();
    let bucket = common::create_test_bucket(&client).await;

    let mut file = File::open(file!()).await.unwrap();
    let mut content = Vec::new();
    file.read_to_end(&mut content).await.unwrap();

    client
        .upload_from_file(
            file!(),
            PutObjectRequest {
                bucket: bucket.clone(),
                key: "from_file".to_string(),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    client
        .upload(
            &mut &content[..],
            PutObjectRequest {
                bucket: bucket.clone(),
                key: "from_read".to_string(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        common::get_body(&client, &bucket, "from_file").await,
        content
    );
    assert_eq!(
        common::get_body(&client, &bucket, "from_read").await,
        content
    );
    common::delete_test_bucket(&client, &bucket, &["from_file", "from_read"]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_upload_arbitrary() {
    async fn upload_arbitrary(body: Vec<u8>) -> bool {
        let client = common::get_client();
        let bucket = common::create_test_bucket(&client).await;

        client
            .upload(
                &mut &body[..],
                PutObjectRequest {
                    bucket: bucket.clone(),
                    key: "some_key".to_owned(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let observed_body = common::get_body(&client, &bucket, "some_key").await;
        common::delete_test_bucket(&client, &bucket, &["some_key"]).await;

        observed_body == body
    }
    let mut gen = thread_rng();
    for _ in 0..NUMBER_OF_TESTS {
        let mut data = vec![0u8; 4096];
        gen.fill_bytes(&mut data);
        assert!(upload_arbitrary(data).await);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_upload_multipart() {
    async fn upload_multipart() -> bool {
        common::init_logger();
        let seed = rand::thread_rng().gen();
        println!("rng seed: {:?}", seed);
        let mut rng = XorShiftRng::from_seed(seed);
        let size = rng.gen_range(5 * 1024 * 1024..=15 * 1024 * 1024); // between 5 MiB and 15 MiB
        upload_multipart_helper(&mut rng, 5 * 1024 * 1024, size).await
    }
    for _ in 0..10 {
        assert!(upload_multipart().await);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn upload_multipart_test_part_boundary() {
    common::init_logger();
    for part_count in 1..5 {
        let seed = rand::thread_rng().gen();
        println!("rng seed: {:?}", seed);
        let mut rng = XorShiftRng::from_seed(seed);
        let part_size = 5 * 1024 * 1024 + 1;
        let size = part_size * part_count;

        // `size` is multiple of `part_size` - 1 byte
        assert!(upload_multipart_helper(&mut rng, part_size - 1, size as u64).await);

        // `size` is multiple of `part_size`
        assert!(upload_multipart_helper(&mut rng, part_size, size as u64).await);

        // `size` is multiple of `part_size` + 1 byte
        assert!(upload_multipart_helper(&mut rng, part_size + 1, size as u64).await);
    }
}

async fn upload_multipart_helper(rng: &mut XorShiftRng, part_size: usize, obj_size: u64) -> bool {
    common::init_logger();
    let client = common::get_client();
    let bucket = common::create_test_bucket(&client).await;

    let mut body = vec![0; obj_size as usize];
    rng.fill_bytes(&mut body);

    let put_request = PutObjectRequest {
        bucket: bucket.clone(),
        key: "object123".to_owned(),
        ..Default::default()
    };
    client
        .upload_multipart(&mut &body[..], put_request, part_size)
        .await
        .unwrap();
    let observed_body = common::get_body(&client, &bucket, "object123").await;

    common::delete_test_bucket(&client, &bucket, &["object123"]).await;

    observed_body == body
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multipart_upload_is_aborted() {
    async fn multipart_upload_is_aborted() -> bool {
        common::init_logger();
        let client = common::get_client();
        let bucket = common::create_test_bucket(&client).await;

        let abort_after = rand::thread_rng().gen_range(0..=10 * 1024 * 1024); // between 0 and 10 MiB
        println!("abort location: {}", abort_after);
        let mut reader = ReaderWithError { abort_after };

        let put_request = PutObjectRequest {
            bucket: bucket.clone(),
            key: "aborted_upload".to_owned(),
            ..Default::default()
        };
        let err = client
            .upload_multipart(&mut reader, put_request, 5 * 1024 * 1024)
            .await
            .unwrap_err();
        match err {
            S3ExtError::IoError(e) => assert_eq!(
                format!("{}", e.into_inner().unwrap()),
                "explicit, unconditional error"
            ),
            S3ExtError::CompleteMultipartUploadError(e) => {
                warn!("Unexpected error {:?}", e)
            }
            e => panic!("unexpected error: {:?}", e),
        }

        // all uploads must have been aborted
        let parts = client
            .list_multipart_uploads(ListMultipartUploadsRequest {
                bucket: bucket.to_owned(),
                ..Default::default()
            })
            .await
            .unwrap();

        common::delete_test_bucket(&client, &bucket, &["object123"]).await;
        parts.uploads.is_none()
    }
    for _ in 0..10 {
        assert!(multipart_upload_is_aborted().await)
    }
}
