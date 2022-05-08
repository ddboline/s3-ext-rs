use crate::error::{S3ExtError, S3ExtResult};
use log::{debug, info, warn};
use rusoto_s3::{
    AbortMultipartUploadRequest, CompleteMultipartUploadOutput, CompleteMultipartUploadRequest,
    CompletedMultipartUpload, CompletedPart, CreateMultipartUploadRequest, PutObjectOutput,
    PutObjectRequest, S3Client, UploadPartRequest, S3,
};
use tokio::io::{AsyncRead, AsyncReadExt};

pub(crate) async fn upload<R>(
    client: &S3Client,
    source: &mut R,
    mut target: PutObjectRequest,
) -> S3ExtResult<PutObjectOutput>
where
    R: AsyncRead + Unpin,
{
    let mut content = Vec::new();
    source.read_to_end(&mut content).await?;
    target.body = Some(content.into());
    client.put_object(target).await.map_err(|e| e.into())
}

pub(crate) async fn upload_multipart<R>(
    client: &S3Client,
    source: &mut R,
    target: PutObjectRequest,
    part_size: usize,
) -> S3ExtResult<CompleteMultipartUploadOutput>
where
    R: AsyncRead + Unpin,
{
    let upload = client
        .create_multipart_upload(CreateMultipartUploadRequest {
            acl: target.acl.clone(),
            bucket: target.bucket.clone(),
            cache_control: target.cache_control.clone(),
            content_disposition: target.content_disposition.clone(),
            content_encoding: target.content_encoding.clone(),
            content_language: target.content_language.clone(),
            content_type: target.content_type.clone(),
            expires: target.expires.clone(),
            grant_full_control: target.grant_full_control.clone(),
            grant_read: target.grant_read.clone(),
            grant_read_acp: target.grant_read_acp.clone(),
            grant_write_acp: target.grant_write_acp.clone(),
            key: target.key.clone(),
            metadata: target.metadata.clone(),
            object_lock_legal_hold_status: target.object_lock_legal_hold_status.clone(),
            object_lock_mode: target.object_lock_mode.clone(),
            object_lock_retain_until_date: target.object_lock_retain_until_date.clone(),
            request_payer: target.request_payer.clone(),
            sse_customer_algorithm: target.sse_customer_algorithm.clone(),
            sse_customer_key: target.sse_customer_key.clone(),
            sse_customer_key_md5: target.sse_customer_key_md5.clone(),
            ssekms_key_id: target.ssekms_key_id.clone(),
            server_side_encryption: target.server_side_encryption.clone(),
            storage_class: target.storage_class.clone(),
            tagging: target.tagging.clone(),
            website_redirect_location: target.website_redirect_location.clone(),
            ssekms_encryption_context: target.ssekms_encryption_context.clone(),
            bucket_key_enabled: target.bucket_key_enabled,
            expected_bucket_owner: target.expected_bucket_owner.clone(),
        })
        .await?;

    let upload_id = upload
        .upload_id
        .ok_or(S3ExtError::Other("Missing upload ID"))?;

    debug!(
        "multi-part upload {:?} started (bucket: {}, key: {})",
        upload_id, target.bucket, target.key
    );

    let bucket = target.bucket.clone();
    let key = target.key.clone();
    let request_payer = target.request_payer.clone();
    let expected_bucket_owner = target.expected_bucket_owner.clone();

    match upload_multipart_needs_abort_on_error(client, source, target, part_size, &upload_id).await
    {
        ok @ Ok(_) => ok,
        err @ Err(_) => {
            info!(
                "aborting upload {:?} due to a failure during upload",
                upload_id
            );
            if let Err(e) = client
                .abort_multipart_upload(AbortMultipartUploadRequest {
                    bucket,
                    expected_bucket_owner,
                    key,
                    request_payer,
                    upload_id,
                })
                .await
            {
                warn!("ignoring failure to abort multi-part upload: {:?}", e);
            };
            err
        }
    }
}

// Upload needs to be aborted if this function fails
async fn upload_multipart_needs_abort_on_error<R>(
    client: &S3Client,
    source: &mut R,
    target: PutObjectRequest,
    part_size: usize,
    upload_id: &str,
) -> S3ExtResult<CompleteMultipartUploadOutput>
where
    R: AsyncRead + Unpin,
{
    let mut parts = Vec::new();
    for part_number in 1.. {
        let mut body = vec![0; part_size];
        let size = source.read(&mut body[..]).await?;
        if size == 0 {
            break;
        }
        body.truncate(size);

        let part = client
            .upload_part(UploadPartRequest {
                body: Some(body.into()),
                bucket: target.bucket.clone(),
                content_length: None,
                content_md5: None,
                key: target.key.clone(),
                part_number,
                request_payer: target.request_payer.clone(),
                sse_customer_algorithm: target.sse_customer_algorithm.clone(),
                sse_customer_key: target.sse_customer_key.clone(),
                sse_customer_key_md5: target.sse_customer_key_md5.clone(),
                upload_id: upload_id.to_owned(),
                expected_bucket_owner: target.expected_bucket_owner.clone(),
            })
            .await?;

        parts.push(CompletedPart {
            e_tag: part.e_tag,
            part_number: Some(part_number),
        });
    }

    client
        .complete_multipart_upload(CompleteMultipartUploadRequest {
            bucket: target.bucket.clone(),
            key: target.key.clone(),
            multipart_upload: Some(CompletedMultipartUpload { parts: Some(parts) }),
            request_payer: target.request_payer.clone(),
            upload_id: upload_id.to_owned(),
            expected_bucket_owner: target.expected_bucket_owner.clone(),
        })
        .await
        .map_err(|e| e.into())
}
