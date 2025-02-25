use crate::{
    block::Block,
    io::ClickhouseWrite,
    protocol::{
        self, CompressionMethod, ServerHello, DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH,
        DBMS_MIN_REVISION_WITH_CLIENT_INFO, DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET,
        DBMS_MIN_REVISION_WITH_OPENTELEMETRY, DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO,
        DBMS_MIN_REVISION_WITH_VERSION_PATCH,
    },
    Result,
};
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

pub struct InternalClientOut<W: ClickhouseWrite> {
    writer: W,
    pub server_hello: ServerHello,
}

pub struct ClientHello<'a> {
    pub default_database: &'a str,
    pub username: &'a str,
    pub password: &'a str,
}

#[repr(u8)]
#[derive(PartialEq, Clone, Copy)]
#[allow(unused, clippy::enum_variant_names)]
pub enum QueryKind {
    NoQuery,
    InitialQuery,
    SecondaryQuery,
}

pub struct ClientInfo<'a> {
    pub kind: QueryKind,
    pub initial_user: &'a str,
    pub initial_query_id: &'a str,
    pub initial_address: &'a str,
    // interface = TCP = 1
    pub os_user: &'a str,
    pub client_hostname: &'a str,
    pub client_name: &'a str,
    pub client_version_major: u64,
    pub client_version_minor: u64,
    pub client_tcp_protocol_version: u64,

    // if DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO
    pub quota_key: &'a str,
    // if DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH
    pub distributed_depth: u64,
    // if DBMS_MIN_REVISION_WITH_VERSION_PATCH
    pub client_version_patch: u64,
    // if DBMS_MIN_REVISION_WITH_OPENTELEMETRY
    pub open_telemetry: Option<OpenTelemetry<'a>>,
}

impl ClientInfo<'_> {
    pub async fn write<W: ClickhouseWrite>(&self, to: &mut W, revision: u64) -> Result<()> {
        to.write_u8(self.kind as u8).await?;
        if self.kind == QueryKind::NoQuery {
            return Ok(());
        }
        to.write_string(self.initial_user).await?;
        to.write_string(self.initial_query_id).await?;
        to.write_string(self.initial_address).await?;
        to.write_u8(1).await?;
        to.write_string(self.os_user).await?;
        to.write_string(self.client_hostname).await?;
        to.write_string(self.client_name).await?;
        to.write_var_uint(self.client_version_major).await?;
        to.write_var_uint(self.client_version_minor).await?;
        to.write_var_uint(self.client_tcp_protocol_version).await?;
        if revision >= DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO {
            to.write_string(self.quota_key).await?;
        }
        if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH {
            to.write_var_uint(self.distributed_depth).await?;
        }
        if revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH {
            to.write_var_uint(self.client_version_patch).await?;
        }
        if revision >= DBMS_MIN_REVISION_WITH_OPENTELEMETRY {
            if let Some(telemetry) = &self.open_telemetry {
                to.write_u8(1u8).await?;
                to.write_all(&telemetry.trace_id.as_bytes()[..]).await?;
                to.write_u64(telemetry.span_id).await?;
                to.write_string(telemetry.tracestate).await?;
                to.write_u8(telemetry.trace_flags).await?;
            } else {
                to.write_u8(0u8).await?;
            }
        }

        Ok(())
    }
}

pub struct OpenTelemetry<'a> {
    trace_id: Uuid,
    span_id: u64,
    tracestate: &'a str,
    trace_flags: u8,
}

#[repr(u64)]
#[derive(Clone, Copy, Debug)]
#[allow(unused)]
pub enum QueryProcessingStage {
    FetchColumns,
    WithMergeableState,
    Complete,
    WithMergableStateAfterAggregation,
}

pub struct Query<'a> {
    pub id: &'a str,
    pub info: ClientInfo<'a>,
    // pub settings: (), //TODO
    //todo: interserver secret
    pub stage: QueryProcessingStage,
    pub compression: CompressionMethod,
    pub query: &'a str,
    //todo: data
}

impl<W: ClickhouseWrite> InternalClientOut<W> {
    pub fn new(writer: W) -> Self {
        InternalClientOut {
            writer,
            server_hello: ServerHello::default(),
        }
    }

    #[allow(clippy::needless_lifetimes)]
    pub async fn send_query<'a>(&mut self, params: Query<'a>) -> Result<()> {
        self.writer
            .write_var_uint(protocol::ClientPacketId::Query as u64)
            .await?;
        self.writer.write_string(params.id).await?;
        if self.server_hello.revision_version >= DBMS_MIN_REVISION_WITH_CLIENT_INFO {
            params
                .info
                .write(&mut self.writer, self.server_hello.revision_version)
                .await?;
        }
        //todo: settings
        self.writer.write_string("").await?;
        if self.server_hello.revision_version >= DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET {
            //todo interserver secret
            self.writer.write_string("").await?;
        }
        self.writer.write_var_uint(params.stage as u64).await?;
        self.writer
            .write_u8(if matches!(params.compression, CompressionMethod::None) {
                0
            } else {
                1
            })
            .await?;
        self.writer.write_string(params.query).await?;

        self.writer.flush().await?;
        Ok(())
    }

    #[cfg(feature = "compression")]
    async fn compress_data(&mut self, byte: u8, block: Block) -> Result<()> {
        let (out, decompressed_size) =
            crate::compression::compress_block(block, self.server_hello.revision_version).await?;
        let mut new_out = Vec::with_capacity(out.len() + 5);
        new_out.push(byte);
        new_out.extend_from_slice(&(out.len() as u32 + 9).to_le_bytes()[..]);
        new_out.extend_from_slice(&(decompressed_size as u32).to_le_bytes()[..]);
        new_out.extend(out);

        let hash = cityhash_rs::cityhash_102_128(&new_out[..]);
        self.writer.write_u64_le((hash >> 64) as u64).await?;
        self.writer.write_u64_le(hash as u64).await?;
        // self.writer.write_u8(byte).await?;
        // self.writer.write_u32_le(out.len() as u32).await?;
        self.writer.write_all(&new_out[..]).await?;
        Ok(())
    }

    #[cfg(not(feature = "compression"))]
    async fn compress_data(&mut self, _byte: u8, _block: &Block) -> Result<()> {
        panic!("attempted to use compression when not compiled with `compression` feature in klickhouse");
    }

    pub async fn send_data(
        &mut self,
        block: Block,
        compression: CompressionMethod,
        name: &str,
        scalar: bool,
    ) -> Result<()> {
        if scalar {
            self.writer
                .write_var_uint(protocol::ClientPacketId::Scalar as u64)
                .await?;
        } else {
            self.writer
                .write_var_uint(protocol::ClientPacketId::Data as u64)
                .await?;
        }
        self.writer.write_string(name).await?;
        match compression {
            CompressionMethod::None => {
                block
                    .write(&mut self.writer, self.server_hello.revision_version)
                    .await?;
            }
            CompressionMethod::LZ4 => {
                self.compress_data(CompressionMethod::LZ4.byte(), block)
                    .await?;
            }
        }

        self.writer.flush().await?;

        Ok(())
    }

    #[allow(clippy::needless_lifetimes)]
    pub async fn send_hello<'a>(&mut self, params: ClientHello<'a>) -> Result<()> {
        self.writer
            .write_var_uint(protocol::ClientPacketId::Hello as u64)
            .await?;
        self.writer
            .write_string(&format!(
                "ClickHouse Rust-Klickhouse {}",
                env!("CARGO_PKG_VERSION")
            ))
            .await?;
        self.writer.write_var_uint(crate::VERSION_MAJOR).await?;
        self.writer.write_var_uint(crate::VERSION_MINOR).await?;
        self.writer
            .write_var_uint(protocol::DBMS_TCP_PROTOCOL_VERSION)
            .await?;
        self.writer.write_string(params.default_database).await?;
        self.writer.write_string(params.username).await?;
        self.writer.write_string(params.password).await?;
        self.writer.flush().await?;
        Ok(())
    }

    // pub async fn send_ping(&mut self) -> Result<()> {
    //     self.writer.write_var_uint(protocol::ClientPacketId::Ping as u64).await?;
    //     Ok(())
    // }
}
