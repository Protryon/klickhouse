# Klickhouse

Klickhouse is a pure Rust SDK for working with [Clickhouse](https://clickhouse.tech/docs/en/) with the native protocol in async environments with minimal boilerplate and maximal performance.

## Example Usage

See [example usage](https://github.com/Protryon/klickhouse/blob/master/klickhouse/examples/basic.rs).

## Unsupported Features

- Clickhouse `Enum8` and `Enum16` types -- use `LowCardinality` instead.

## Running the tests

A Clickhouse server is required to run the integration tests. One can be started easily in a Docker container:

```
$ docker run  --rm --name clickhouse -p 19000:9000 --ulimit nofile=262144:262144 clickhouse
$ export KLICKHOUSE_TEST_ADDR=127.0.0.1:19000
$ cargo nexttest run
```

(running the tests simultaneously with `cargo test` is currently not suported, due to loggers initializations.)

## Credit

`klickhouse_derive` was made by copy/paste/simplify of `serde_derive` to get maximal functionality and performance at lowest time-cost. In a prototype, `serde` was directly used, but this was abandoned due to lock-in of `serde`'s data model.
