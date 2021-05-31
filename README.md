# Klickhouse

Klickhouse is a pure Rust SDK for working with [Clickhouse](https://clickhouse.tech/docs/en/) with the native protocol in async environments with minimal boilerplate and maximal performance.

## Example Usage

See [example usage](https://github.com/Protryon/klickhouse/blob/master/klickhouse/examples/basic.rs).

## Unsupported Features

* Clickhouse `Enum8` and `Enum16` types -- use `LowCardinality` instead.

## Credit

`klickhouse_derive` was made by copy/paste/simplify of `serde_derive` to get maximal functionality and performance at lowest time-cost. In a prototype, `serde` was directly used, but this was abandoned due to lock-in of `serde`'s data model.