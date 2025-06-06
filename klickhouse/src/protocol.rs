use indexmap::IndexMap;
use uuid::Uuid;

use crate::{block::Block, progress::Progress, KlickhouseError, Result};

pub const DBMS_MIN_REVISION_WITH_CLIENT_INFO: u64 = 54032;
pub const DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE: u64 = 54058;
pub const DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO: u64 = 54060;
// pub const DBMS_MIN_REVISION_WITH_TABLES_STATUS: u64 = 54226;
// pub const DBMS_MIN_REVISION_WITH_TIME_ZONE_PARAMETER_IN_DATETIME_DATA_TYPE: u64 = 54337;
pub const DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME: u64 = 54372;
pub const DBMS_MIN_REVISION_WITH_VERSION_PATCH: u64 = 54401;
// pub const DBMS_MIN_REVISION_WITH_SERVER_LOGS: u64 = 54406;
// pub const DBMS_MIN_REVISION_WITH_CLIENT_SUPPORT_EMBEDDED_DATA: u64 = 54415;
// pub const DBMS_MIN_REVISION_WITH_CURRENT_AGGREGATION_VARIANT_SELECTION_METHOD: u64 = 54431;
// pub const DBMS_MIN_REVISION_WITH_COLUMN_DEFAULTS_METADATA: u64 = 54410;
// pub const DBMS_MIN_REVISION_WITH_LOW_CARDINALITY_TYPE: u64 = 54405;
pub const DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO: u64 = 54420;
// pub const DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS: u64 = 54429;
pub const DBMS_MIN_REVISION_WITH_OPENTELEMETRY: u64 = 54442;
pub const DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET: u64 = 54441;
// pub const DBMS_MIN_REVISION_WITH_X_FORWARDED_FOR_IN_CLIENT_INFO: u64 = 54443;
// pub const DBMS_MIN_REVISION_WITH_REFERER_IN_CLIENT_INFO: u64 = 54447;
pub const DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH: u64 = 54448;

pub const DBMS_TCP_PROTOCOL_VERSION: u64 = 54448;

pub const MAX_STRING_SIZE: usize = 1 << 30;

#[repr(u64)]
#[derive(Clone, Copy, Debug)]
#[allow(unused)]
pub enum ClientPacketId {
    Hello,
    Query,
    Data,
    Cancel,
    Ping,
    TablesStatusRequest,
    KeepAlive,
    Scalar,
    IgnoredPartUUIDs,
    ReadTaskResponse,
}

#[repr(u64)]
#[derive(Clone, Copy, Debug)]
pub enum ServerPacketId {
    Hello,
    Data,
    Exception,
    Progress,
    Pong,
    EndOfStream,
    ProfileInfo,
    Totals,
    Extremes,
    TablesStatusResponse,
    Log,
    TableColumns,
    PartUUIDs,
    ReadTaskRequest,
}

impl ServerPacketId {
    pub fn from_u64(i: u64) -> Result<Self> {
        Ok(match i {
            0 => ServerPacketId::Hello,
            1 => ServerPacketId::Data,
            2 => ServerPacketId::Exception,
            3 => ServerPacketId::Progress,
            4 => ServerPacketId::Pong,
            5 => ServerPacketId::EndOfStream,
            6 => ServerPacketId::ProfileInfo,
            7 => ServerPacketId::Totals,
            8 => ServerPacketId::Extremes,
            9 => ServerPacketId::TablesStatusResponse,
            10 => ServerPacketId::Log,
            11 => ServerPacketId::TableColumns,
            12 => ServerPacketId::PartUUIDs,
            13 => ServerPacketId::ReadTaskRequest,
            x => {
                return Err(KlickhouseError::ProtocolError(format!(
                    "invalid packet id from server: {x}"
                )))
            }
        })
    }
}

#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
pub struct ServerHello {
    pub server_name: String,
    pub major_version: u64,
    pub minor_version: u64,
    pub revision_version: u64,
    pub timezone: Option<String>,
    pub display_name: Option<String>,
    pub patch_version: u64,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ServerData {
    pub table_name: String,
    pub block: Block,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ServerException {
    pub code: i32,
    pub name: String,
    pub message: String,
    pub stack_trace: String,
    pub has_nested: bool,
}

impl ServerException {
    pub fn emit(&self) -> KlickhouseError {
        KlickhouseError::ServerException {
            code: self.code,
            name: self.name.clone(),
            message: self.message.clone(),
            stack_trace: self.stack_trace.clone(),
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct BlockStreamProfileInfo {
    pub rows: u64,
    pub blocks: u64,
    pub bytes: u64,
    pub applied_limit: bool,
    pub rows_before_limit: u64,
    pub calculated_rows_before_limit: bool,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct TableColumns {
    pub name: String,
    pub description: String,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct TableStatus {
    pub is_replicated: bool,
    pub absolute_delay: u32,
}

#[derive(Debug, Clone)]
pub struct TablesStatusResponse {
    pub database_tables: IndexMap<String, IndexMap<String, TableStatus>>,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum ServerPacket {
    Hello(ServerHello),
    Data(ServerData),
    Exception(ServerException),
    Progress(Progress),
    Pong,
    EndOfStream,
    ProfileInfo(BlockStreamProfileInfo),
    Totals(ServerData),
    Extremes(ServerData),
    TablesStatusResponse(TablesStatusResponse),
    Log(ServerData),
    TableColumns(TableColumns),
    PartUUIDs(Vec<Uuid>),
    ReadTaskRequest,
}

#[derive(Clone, Copy, Debug, Default)]
#[allow(unused)]
pub enum CompressionMethod {
    #[cfg_attr(not(feature = "compression"), default)]
    None,
    #[cfg_attr(feature = "compression", default)]
    LZ4,
}

impl CompressionMethod {
    pub fn byte(&self) -> u8 {
        match self {
            CompressionMethod::None => 0x02,
            CompressionMethod::LZ4 => 0x82,
        }
    }
}
