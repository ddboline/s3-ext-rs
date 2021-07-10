# S3-Ext - Simple Storage Service Extensions for Rust

[![crates.io](https://meritbadge.herokuapp.com/s3-ext)](https://crates.io/crates/s3-ext)
[![Build Status](https://github.com/ddboline/s3-ext-rs/workflows/Rust/badge.svg?branch=master)](https://github.com/ddboline/s3-ext-rs/actions?branch=master)
[![Documentation](https://docs.rs/s3-ext/badge.svg)](https://docs.rs/s3-ext/)
[![codecov](https://codecov.io/gh/ddboline/s3-ext-rs/branch/master/graph/badge.svg)](https://codecov.io/gh/ddboline/s3-ext-rs)

## What is S3-Ext

This is a fork of the [S4 Crate](https://crates.io/crates/s4), the name has been changed,
the minimum supported version of rusoto is v0.43.0, and everything is async/await.

S3-ext provides a high-level API for S3 building on top of [Rusoto](https://www.rusoto.org/) and extending it's API.


## What is added that *Rusoto* itself doesn't provide

* simple way to create an `S3Client`
* download object to a file
* download object and [`Write`] it
* upload object from file
* [`Read`] object and upload it
* simple way to create stream of all objects or objects with a given prefix

## Implementation details

Most functionality is provided by the `S3Ext` trait which is implemented for *Rusoto*'s `S3Client`.


[`AsyncRead`]: https://docs.rs/tokio/0.2.11/tokio/io/trait.AsyncReadExt.html
[`AsyncWrite`]: https://docs.rs/tokio/0.2.11/tokio/io/trait.AsyncWriteExt.html


## Running Tests

1. Start Minio

```
docker run -d --rm -p 9000:9000 --env "MINIO_ACCESS_KEY=ANTN35UAENTS5UIAEATD" \
--env "MINIO_SECRET_KEY=TtnuieannGt2rGuie2t8Tt7urarg5nauedRndrur" \
--env MINIO_DOMAIN=localhost minio/minio server /minio
```

2. Run tests

```
cargo test --all
```