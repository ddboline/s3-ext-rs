# S3-Ext - Simple Storage Service Extensions for Rust

[![crates.io](https://meritbadge.herokuapp.com/s3-ext)](https://crates.io/crates/s3-ext)

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
* simple way to iterate through all objects or objects with a given prefix

## Implementation details

Most functionality is provided by the `S3Ext` trait which is implemented for *Rusoto*'s `S3Client`.


[`Read`]: https://doc.rust-lang.org/nightly/std/io/trait.Read.html
[`Write`]: https://doc.rust-lang.org/nightly/std/io/trait.Write.html


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
