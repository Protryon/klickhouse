use crate::{block::{Block}, io::ClickhouseWrite, protocol::{self, DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH, DBMS_MIN_REVISION_WITH_CLIENT_INFO, DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET, DBMS_MIN_REVISION_WITH_OPENTELEMETRY, DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO, DBMS_MIN_REVISION_WITH_VERSION_PATCH, ServerHello}};
use anyhow::*;
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
#[allow(unused)]
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

impl<'a> ClientInfo<'a> {
    pub async fn write<W: ClickhouseWrite>(&self, to: &mut W, revision: u64) -> Result<()> {
        to.write_u8(self.kind as u8).await?;
        if self.kind == QueryKind::NoQuery {
            return Ok(())
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
    pub compression: bool,
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
        self.writer.write_var_uint(protocol::ClientPacketId::Query as u64).await?;
        self.writer.write_string(params.id).await?;
        if self.server_hello.revision_version >= DBMS_MIN_REVISION_WITH_CLIENT_INFO {
            params.info.write(&mut self.writer, self.server_hello.revision_version).await?;
        }
        //todo: settings
        self.writer.write_string("").await?;
        if self.server_hello.revision_version >= DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET {
            //todo interserver secret
            self.writer.write_string("").await?;
        }
        self.writer.write_var_uint(params.stage as u64).await?;
        self.writer.write_u8(if params.compression { 1 } else { 0 }).await?;
        self.writer.write_string(params.query).await?;

        self.writer.flush().await?;
        Ok(())
    }

    pub async fn send_data(&mut self, block: &Block, name: &str, scalar: bool) -> Result<()> {
        if scalar {
            self.writer.write_var_uint(protocol::ClientPacketId::Scalar as u64).await?;
        } else {
            self.writer.write_var_uint(protocol::ClientPacketId::Data as u64).await?;
        }
        self.writer.write_string(name).await?;
        block.write(&mut self.writer, self.server_hello.revision_version).await?;
        self.writer.flush().await?;

        Ok(())
    }

    #[allow(clippy::needless_lifetimes)]
    pub async fn send_hello<'a>(&mut self, params: ClientHello<'a>) -> Result<()> {
        self.writer.write_var_uint(protocol::ClientPacketId::Hello as u64).await?;
        self.writer.write_string(&*format!("ClickHouse Rust-Klickhouse {}", env!("CARGO_PKG_VERSION"))).await?;
        self.writer.write_var_uint(crate::VERSION_MAJOR).await?;
        self.writer.write_var_uint(crate::VERSION_MINOR).await?;
        self.writer.write_var_uint(protocol::DBMS_TCP_PROTOCOL_VERSION).await?;
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