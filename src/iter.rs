//! Iterators over `Object`s
//!
//! # Example
//!
//! ```
//! use futures::stream::{Stream, StreamExt, TryStreamExt};
//! use futures::future::try_join_all;
//! use std::future::Future;
//! use rand::RngCore;
//! use rusoto_core::Region;
//! use rusoto_s3::{CreateBucketRequest, PutObjectRequest, S3, S3Client};
//! use s3_ext::S3Ext;
//! use std::env;
//! use tokio::io::AsyncReadExt;
//!
//! use s3_ext::error::S3ExtError;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), S3ExtError> {
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
//!     let client = s3_ext::new_s3client_with_credentials(region, access_key, secret_key)?;
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
//!         .stream_objects(&bucket)
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
//!         .stream_get_objects(&bucket)
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

use crate::error::{S3ExtError, S3ExtResult};
use futures::{
    ready,
    stream::Stream,
    task::{Context, Poll},
};
use pin_utils::{unsafe_pinned, unsafe_unpinned};
use rusoto_core::{RusotoError, RusotoResult};
use rusoto_s3::{
    GetObjectError, GetObjectOutput, GetObjectRequest, ListObjectsV2Error, ListObjectsV2Output,
    ListObjectsV2Request, Object, S3Client, S3,
};
use std::{future::Future, mem, pin::Pin, vec::IntoIter};

/// Iterator-like objects, forms the basis of ObjectStream
#[derive(Clone)]
pub struct ObjectIter {
    client: S3Client,
    request: ListObjectsV2Request,
    objects: IntoIter<Object>,
    exhausted: bool,
}

impl ObjectIter {
    fn new(client: &S3Client, bucket: &str, prefix: Option<&str>) -> Self {
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
        self.update_objects(resp);
        Ok(())
    }

    fn update_objects(&mut self, resp: ListObjectsV2Output) {
        self.objects = resp.contents.unwrap_or_else(Vec::new).into_iter();
        match resp.next_continuation_token {
            next @ Some(_) => self.request.continuation_token = next,
            None => self.exhausted = true,
        };
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

    /// Get the next object (or None if there are no more objects), may return an error when fetching objects.
    pub async fn next_object(&mut self) -> Result<Option<Object>, RusotoError<ListObjectsV2Error>> {
        if let object @ Some(_) = self.objects.next() {
            Ok(object)
        } else if self.exhausted {
            Ok(None)
        } else {
            self.next_objects().await?;
            Ok(self.objects.next())
        }
    }

    /// Consume the iterator and return the number of objects
    pub async fn count(mut self) -> Result<usize, RusotoError<ListObjectsV2Error>> {
        let mut count = self.objects.len();
        while !self.exhausted {
            self.next_objects().await?;
            count += self.objects.len();
        }
        Ok(count)
    }

    /// Consume the iterator and return the last object
    pub async fn last(mut self) -> Result<Option<Object>, RusotoError<ListObjectsV2Error>> {
        self.last_internal().await
    }

    /// Consume the iterator and return the nth object
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

type ObjResult = RusotoResult<ListObjectsV2Output, ListObjectsV2Error>;
type NextObjFuture = Pin<Box<dyn Future<Output = ObjResult> + Send>>;

/// Stream over objects
pub struct ObjectStream {
    iter: ObjectIter,
    fut: Option<NextObjFuture>,
}

impl ObjectStream {
    pub(crate) fn new(client: &S3Client, bucket: &str, prefix: Option<&str>) -> Self {
        Self {
            iter: ObjectIter::new(client, bucket, prefix),
            fut: None,
        }
    }

    /// Return a reference to ObjectIter
    pub fn get_iter(&self) -> &ObjectIter {
        &self.iter
    }

    /// Consume the string and return the ObjectIter
    pub fn into_iter(self) -> ObjectIter {
        self.iter
    }

    async fn get_objects(
        client: S3Client,
        request: ListObjectsV2Request,
    ) -> RusotoResult<ListObjectsV2Output, ListObjectsV2Error> {
        client.list_objects_v2(request).await
    }

    unsafe_unpinned!(iter: ObjectIter);
    unsafe_pinned!(fut: Option<NextObjFuture>);
}

// This is kind of ugly but seems to work as intended, I hope that one day this can be done more simply...
impl Stream for ObjectStream {
    type Item = RusotoResult<Object, ListObjectsV2Error>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        if self.as_mut().fut().is_none() {
            if let Some(object) = self.as_mut().iter().objects.next() {
                return Poll::Ready(Some(Ok(object)));
            } else if self.as_mut().iter().exhausted {
                return Poll::Ready(None);
            } else {
                let client = self.as_mut().iter().client.clone();
                let request = self.as_mut().iter().request.clone();
                self.as_mut()
                    .fut()
                    .set(Some(Box::pin(Self::get_objects(client, request))));
            }
        }

        let result = ready!(self.as_mut().fut().as_pin_mut().unwrap().poll(cx));
        self.as_mut().fut().set(None);

        match result {
            Ok(resp) => self.as_mut().iter().update_objects(resp),
            Err(e) => return Poll::Ready(Some(Err(e))),
        }
        if let Some(object) = self.as_mut().iter().objects.next() {
            Poll::Ready(Some(Ok(object)))
        } else {
            Poll::Ready(None)
        }
    }
}

/// Iterator-like object retrieving all objects or objects with a given prefix
///
/// The iterator yields tuples of `(key, object)`.
#[derive(Clone)]
pub struct GetObjectIter {
    inner: ObjectIter,
    bucket: String,
}

impl GetObjectIter {
    fn new(client: &S3Client, bucket: &str, prefix: Option<&str>) -> Self {
        GetObjectIter {
            inner: ObjectIter::new(client, bucket, prefix),
            bucket: bucket.to_owned(),
        }
    }

    async fn retrieve(
        &mut self,
        object: Option<Object>,
    ) -> S3ExtResult<Option<(String, GetObjectOutput)>> {
        match object {
            Some(object) => {
                let key = object
                    .key
                    .ok_or_else(|| S3ExtError::Other("response is missing key"))?;
                let request = GetObjectRequest {
                    bucket: self.bucket.clone(),
                    key,
                    ..Default::default()
                };
                match self.inner.client.get_object(request.clone()).await {
                    Ok(o) => {
                        let key = request.key;
                        Ok(Some((key, o)))
                    }
                    Err(e) => Err(e.into()),
                }
            }
            None => Ok(None),
        }
    }

    /// Retrieve the next object
    pub async fn retrieve_next(&mut self) -> S3ExtResult<Option<(String, GetObjectOutput)>> {
        let next = self.inner.next_object().await?;
        self.retrieve(next).await
    }

    #[inline]
    pub async fn next(&mut self) -> S3ExtResult<Option<(String, GetObjectOutput)>> {
        let next = self.inner.next_object().await?;
        self.retrieve(next).await
    }

    #[inline]
    /// Consume the iterator and return the number of elements
    pub async fn count(self) -> Result<usize, S3ExtError> {
        self.inner.count().await.map_err(|e| e.into())
    }

    #[inline]
    /// Consume the iterator and retreive the last element
    pub async fn last(mut self) -> Result<Option<(String, GetObjectOutput)>, S3ExtError> {
        let last = self.inner.last_internal().await?;
        self.retrieve(last).await
    }

    #[inline]
    /// Consume the iterator and return the nth element
    pub async fn nth(&mut self, n: usize) -> Result<Option<(String, GetObjectOutput)>, S3ExtError> {
        let nth = self.inner.nth(n).await?;
        self.retrieve(nth).await
    }
}

type GetObjResult = RusotoResult<GetObjectOutput, GetObjectError>;
type NextGetObjFuture = Pin<Box<dyn Future<Output = GetObjResult> + Send>>;

/// Stream which retrieves objects
pub struct GetObjectStream {
    iter: GetObjectIter,
    next: Option<Object>,
    key: Option<String>,
    fut0: Option<NextObjFuture>,
    fut1: Option<NextGetObjFuture>,
}

impl GetObjectStream {
    pub(crate) fn new(client: &S3Client, bucket: &str, prefix: Option<&str>) -> Self {
        Self {
            iter: GetObjectIter::new(client, bucket, prefix),
            next: None,
            key: None,
            fut0: None,
            fut1: None,
        }
    }

    /// Return a reference to our GetObjectIter object
    pub fn get_iter(&self) -> &GetObjectIter {
        &self.iter
    }

    /// Return our GetObjectIter object
    pub fn into_iter(self) -> GetObjectIter {
        self.iter
    }

    /// Return a reference to our ObjectIter object
    pub fn get_inner(&self) -> &ObjectIter {
        &self.iter.inner
    }

    /// Return our ObjectIter object
    pub fn into_inner(self) -> ObjectIter {
        self.iter.inner
    }

    async fn get_object(
        client: S3Client,
        request: GetObjectRequest,
    ) -> RusotoResult<GetObjectOutput, GetObjectError> {
        client.get_object(request).await
    }

    unsafe_unpinned!(iter: GetObjectIter);
    unsafe_unpinned!(next: Option<Object>);
    unsafe_unpinned!(key: Option<String>);
    unsafe_pinned!(fut0: Option<NextObjFuture>);
    unsafe_pinned!(fut1: Option<NextGetObjFuture>);
}

impl Stream for GetObjectStream {
    type Item = S3ExtResult<(String, GetObjectOutput)>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        if self.as_mut().fut0().is_none() && self.as_mut().fut1().is_none() {
            if let Some(object) = self.as_mut().iter().inner.objects.next() {
                self.as_mut().next().replace(object);
            } else if self.as_mut().iter().inner.exhausted {
                return Poll::Ready(None);
            } else {
                let client = self.as_mut().iter().inner.client.clone();
                let request = self.as_mut().iter().inner.request.clone();
                self.as_mut()
                    .fut0()
                    .set(Some(Box::pin(ObjectStream::get_objects(client, request))));
            }
        }

        assert!(!(self.as_mut().fut0().is_some() && self.as_mut().fut1().is_some()));

        if self.as_mut().fut0().is_some() {
            let result = ready!(self.as_mut().fut0().as_pin_mut().unwrap().poll(cx));
            self.as_mut().fut0().set(None);

            match result {
                Ok(resp) => self.as_mut().iter().inner.update_objects(resp),
                Err(e) => return Poll::Ready(Some(Err(e.into()))),
            }
            match self.as_mut().iter().inner.objects.next() {
                Some(next) => {
                    self.as_mut().next().replace(next);
                }
                None => return Poll::Ready(None),
            }
        }

        if let Some(next) = self.as_mut().next().take() {
            let key = if let Some(key) = next.key {
                key
            } else {
                return Poll::Ready(Some(Err(S3ExtError::Other("response is missing key"))));
            };
            self.as_mut().key().replace(key.clone());
            let client = self.as_mut().iter().inner.client.clone();
            let request = GetObjectRequest {
                bucket: self.as_mut().iter().bucket.clone(),
                key,
                ..Default::default()
            };
            self.as_mut()
                .fut1()
                .set(Some(Box::pin(Self::get_object(client, request))));
        }

        assert!(self.as_mut().fut0().is_none());

        if self.as_mut().fut1().is_some() {
            let result = ready!(self.as_mut().fut1().as_pin_mut().unwrap().poll(cx));
            self.as_mut().fut1().set(None);
            match result {
                Ok(obj) => Poll::Ready(Some(Ok((self.as_mut().key().take().unwrap(), obj)))),
                Err(e) => Poll::Ready(Some(Err(e.into()))),
            }
        } else {
            panic!("We shouldn't ever get here...");
        }
    }
}
