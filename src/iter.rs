//! Iterators over `Object`s
//!
//! # Example
//!
//! ```
//! use fallible_iterator::FallibleIterator;
//! use futures::stream::{Stream, StreamExt, TryStreamExt};
//! use futures::future::try_join_all;
//! use std::future::Future;
//! use rand::RngCore;
//! use rusoto_core::Region;
//! use rusoto_s3::{CreateBucketRequest, PutObjectRequest, S3, S3Client};
//! use s4::S4;
//! use std::env;
//! use tokio::io::AsyncReadExt;
//!
//! use s4::error::S4Error;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), S4Error> {
//!     let bucket = format!("iter-module-example-{}", rand::thread_rng().next_u64());
//!
//!     // setup client
//!
//!     let access_key = "ANTN35UAENTS5UIAEATD".to_string();
//!     let secret_key = "TtnuieannGt2rGuie2t8Tt7urarg5nauedRndrur".to_string();
//!     let endpoint = env::var("S3_ENDPOINT").unwrap_or_else(|_| "http://localhost:9000".to_string());
//!     let region = Region::Custom {
//!         name: "eu-west-1".to_string(),
//!         endpoint,
//!     };
//!     let client = s4::new_s3client_with_credentials(region, access_key, secret_key)?;
//!
//!     // create bucket
//!
//!     client
//!         .create_bucket(CreateBucketRequest {
//!             bucket: bucket.clone(),
//!             ..Default::default()
//!         })
//!         .await?;
//!
//!     // create test objects
//!
//!     for obj in (0..5).map(|n| format!("object_{:02}", n)) {
//!         client
//!             .put_object(PutObjectRequest {
//!                 bucket: bucket.clone(),
//!                 key: obj.to_string(),
//!                 body: Some(obj.as_bytes().to_vec().into()),
//!                 ..Default::default()
//!             })
//!             .await?;
//!     }
//!
//!     // iterate over objects objects (sorted alphabetically)
//!
//!    let objects: Vec<_> = client
//!         .iter_objects(&bucket)
//!         .into_stream()
//!         .map(|res| res.map(|obj| obj.key))
//!         .try_collect()
//!         .await?;
//!    let objects: Vec<_> = objects.into_iter().filter_map(|x| x).collect();
//!
//!     assert_eq!(
//!         objects.as_slice(),
//!         &[
//!             "object_00",
//!             "object_01",
//!             "object_02",
//!             "object_03",
//!             "object_04",
//!         ]
//!     );
//!
//!     // iterate object and fetch content on the fly (sorted alphabetically)
//!     let results: Result<Vec<_>, _> = client
//!         .iter_get_objects(&bucket)
//!         .into_stream()
//!         .map(|res| res.map(|(key, obj)| (key, obj.body)))
//!         .try_collect()
//!         .await;
//!
//!     let futures: Vec<_> = results?
//!         .into_iter()
//!         .map(|(key, body)| async move {
//!             let mut buf = Vec::new();
//!             if let Some(body) = body {
//!                 match body.into_async_read().read_to_end(&mut buf).await {
//!                     Ok(_) => Ok(Some((key, buf))),
//!                     Err(e) => Err(e),
//!                 }
//!             } else {
//!                 Ok(None)
//!             }
//!         })
//!         .collect();
//!     let results: Result<Vec<_>, _> = try_join_all(futures).await;
//!     let objects: Vec<_> = results?.into_iter().filter_map(|x| x).collect();
//!
//!     for (i, (key, body)) in objects.iter().enumerate() {
//!         let expected = format!("object_{:02}", i);
//!         assert_eq!(key, &expected);
//!         assert_eq!(body.as_slice(), expected.as_bytes());
//!     }
//!     Ok(())
//! }
//! ```

use crate::error::{S4Error, S4Result};
use futures::stream::{unfold, Stream};
use rusoto_core::{RusotoError, RusotoResult};
use rusoto_s3::{
    GetObjectOutput, GetObjectRequest, ListObjectsV2Error, ListObjectsV2Request, Object, S3Client,
    S3,
};
use std::mem;
use std::vec::IntoIter;

/// Iterator over all objects or objects with a given prefix
pub struct ObjectIter {
    client: S3Client,
    request: ListObjectsV2Request,
    objects: IntoIter<Object>,
    exhausted: bool,
}

impl Clone for ObjectIter {
    fn clone(&self) -> Self {
        ObjectIter {
            client: self.client.clone(),
            request: self.request.clone(),
            objects: self.objects.clone(),
            exhausted: self.exhausted,
        }
    }
}

impl ObjectIter {
    pub(crate) fn new(client: &S3Client, bucket: &str, prefix: Option<&str>) -> Self {
        let request = ListObjectsV2Request {
            bucket: bucket.to_owned(),
            max_keys: Some(1000),
            prefix: prefix.map(|s| s.to_owned()),
            ..Default::default()
        };

        ObjectIter {
            client: client.clone(),
            request,
            objects: Vec::new().into_iter(),
            exhausted: false,
        }
    }

    async fn next_objects(&mut self) -> RusotoResult<(), ListObjectsV2Error> {
        let resp = self.client.list_objects_v2(self.request.clone()).await?;
        self.objects = resp.contents.unwrap_or_else(Vec::new).into_iter();
        match resp.next_continuation_token {
            next @ Some(_) => self.request.continuation_token = next,
            None => self.exhausted = true,
        };
        Ok(())
    }

    async fn last_internal(&mut self) -> RusotoResult<Option<Object>, ListObjectsV2Error> {
        let mut objects = mem::replace(&mut self.objects, Vec::new().into_iter());
        while !self.exhausted {
            self.next_objects().await?;
            if self.objects.len() > 0 {
                objects = mem::replace(&mut self.objects, Vec::new().into_iter());
            }
        }
        Ok(objects.last())
    }

    pub async fn next_object(&mut self) -> RusotoResult<Option<Object>, ListObjectsV2Error> {
        if let object @ Some(_) = self.objects.next() {
            Ok(object)
        } else if self.exhausted {
            Ok(None)
        } else {
            self.next_objects().await?;
            Ok(self.objects.next())
        }
    }

    pub fn into_stream(self) -> impl Stream<Item = RusotoResult<Object, ListObjectsV2Error>> {
        unfold(self, |mut state| async move {
            match state.next_object().await {
                Ok(Some(obj)) => Some((Ok(obj), state)),
                Err(e) => Some((Err(e), state)),
                Ok(None) => None,
            }
        })
    }

    pub async fn next(&mut self) -> Result<Option<Object>, RusotoError<ListObjectsV2Error>> {
        if let object @ Some(_) = self.objects.next() {
            Ok(object)
        } else if self.exhausted {
            Ok(None)
        } else {
            self.next_objects().await?;
            Ok(self.objects.next())
        }
    }

    pub async fn count(mut self) -> Result<usize, RusotoError<ListObjectsV2Error>> {
        let mut count = self.objects.len();
        while !self.exhausted {
            self.next_objects().await?;
            count += self.objects.len();
        }
        Ok(count)
    }

    pub async fn last(mut self) -> Result<Option<Object>, RusotoError<ListObjectsV2Error>> {
        self.last_internal().await
    }

    pub async fn nth(
        &mut self,
        mut n: usize,
    ) -> Result<Option<Object>, RusotoError<ListObjectsV2Error>> {
        while self.objects.len() <= n && !self.exhausted {
            n -= self.objects.len();
            self.next_objects().await?;
        }
        Ok(self.objects.nth(n))
    }
}

/// Iterator retrieving all objects or objects with a given prefix
///
/// The iterator yields tuples of `(key, object)`.
pub struct GetObjectIter {
    inner: ObjectIter,
    request: GetObjectRequest,
}

impl Clone for GetObjectIter {
    fn clone(&self) -> Self {
        GetObjectIter {
            inner: self.inner.clone(),
            request: self.request.clone(),
        }
    }
}

impl GetObjectIter {
    pub(crate) fn new(client: &S3Client, bucket: &str, prefix: Option<&str>) -> Self {
        let request = GetObjectRequest {
            bucket: bucket.to_owned(),
            ..Default::default()
        };

        GetObjectIter {
            inner: ObjectIter::new(client, bucket, prefix),
            request,
        }
    }

    async fn retrieve(
        &mut self,
        object: Option<Object>,
    ) -> S4Result<Option<(String, GetObjectOutput)>> {
        match object {
            Some(object) => {
                self.request.key = object
                    .key
                    .ok_or_else(|| S4Error::Other("response is missing key"))?;
                match self.inner.client.get_object(self.request.clone()).await {
                    Ok(o) => {
                        let key = mem::replace(&mut self.request.key, String::new());
                        Ok(Some((key, o)))
                    }
                    Err(e) => Err(e.into()),
                }
            }
            None => Ok(None),
        }
    }

    pub async fn retrieve_next(&mut self) -> S4Result<Option<(String, GetObjectOutput)>> {
        let next = self.inner.next().await?;
        self.retrieve(next).await
    }

    pub fn into_stream(self) -> impl Stream<Item = S4Result<(String, GetObjectOutput)>> {
        unfold(self, |mut state| async move {
            match state.retrieve_next().await {
                Ok(Some(obj)) => Some((Ok(obj), state)),
                Err(e) => Some((Err(e), state)),
                Ok(None) => None,
            }
        })
    }

    #[inline]
    pub async fn next(&mut self) -> S4Result<Option<(String, GetObjectOutput)>> {
        let next = self.inner.next().await?;
        self.retrieve(next).await
    }

    #[inline]
    pub async fn count(self) -> Result<usize, S4Error> {
        self.inner.count().await.map_err(|e| e.into())
    }

    #[inline]
    pub async fn last(mut self) -> Result<Option<(String, GetObjectOutput)>, S4Error> {
        let last = self.inner.last_internal().await?;
        self.retrieve(last).await
    }

    #[inline]
    pub async fn nth(&mut self, n: usize) -> Result<Option<(String, GetObjectOutput)>, S4Error> {
        let nth = self.inner.nth(n).await?;
        self.retrieve(nth).await
    }
}
