use crate::protocol::{
    DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS,
    DBMS_MIN_REVISION_WITH_SERVER_LOGS,
};
use crate::Result;
use crate::{
    block::Block,
    io::ClickhouseRead,
    progress::Progress,
    protocol::{
        self, BlockStreamProfileInfo, CompressionMethod, ServerData, ServerException, ServerHello,
        ServerPacket, TableColumns, TableStatus, TablesStatusResponse,
        DBMS_MIN_PROTOCOL_VERSION_WITH_PASSWORD_COMPLEXITY_RULES,
        DBMS_MIN_PROTOCOL_VERSION_WITH_PROFILE_EVENTS_IN_INSERT,
        DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO, DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_V2,
        DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME, DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE,
        DBMS_MIN_REVISION_WITH_VERSION_PATCH, DBMS_TCP_PROTOCOL_VERSION, MAX_STRING_SIZE,
    },
    KlickhouseError,
};
use indexmap::IndexMap;
use protocol::ServerPacketId;
use tokio::io::AsyncReadExt;
use uuid::Uuid;

#[cfg(feature = "compression")]
pub(crate) const MAX_COMPRESSION_SIZE: u32 = 0x40000000;

pub struct InternalClientIn<R: ClickhouseRead> {
    reader: R,
    pub server_hello: ServerHello,
}

impl<R: ClickhouseRead + 'static> InternalClientIn<R> {
    pub fn new(reader: R) -> Self {
        InternalClientIn {
            reader,
            server_hello: ServerHello::default(),
        }
    }

    async fn read_exception(&mut self) -> Result<ServerException> {
        let code = self.reader.read_i32_le().await?;
        let name = self.reader.read_utf8_string().await?;
        let message =
            String::from_utf8_lossy(self.reader.read_string().await?.as_ref()).to_string();
        let stack_trace = self.reader.read_utf8_string().await?;
        let has_nested = self.reader.read_u8().await? != 0;

        Ok(ServerException {
            code,
            name,
            message,
            stack_trace,
            has_nested,
        })
    }

    #[cfg(feature = "compression")]
    async fn decompress_data(&mut self, compression: CompressionMethod) -> Result<Block> {
        let mut reader =
            crate::compression::DecompressionReader::new(compression, &mut self.reader);

        let block = Block::read(&mut reader, self.server_hello.revision_version).await?;

        Ok(block)
    }

    #[cfg(not(feature = "compression"))]
    async fn decompress_data(&mut self, _compression: CompressionMethod) -> Result<Block> {
        panic!("attempted to use compression when not compiled with `compression` feature in klickhouse");
    }

    async fn receive_data(&mut self, compression: CompressionMethod) -> Result<ServerData> {
        let table_name = self.reader.read_utf8_string().await?;

        let block = match compression {
            CompressionMethod::None => {
                Block::read(&mut self.reader, self.server_hello.revision_version).await?
            }
            _ => self.decompress_data(compression).await?,
        };

        Ok(ServerData { table_name, block })
    }

    /// receive_packet
    ///
    /// Receives a raw packet from the server and forwards it to the wrapping
    /// Client (if successful).
    pub async fn receive_packet(&mut self) -> Result<ServerPacket> {
        let packet_id = ServerPacketId::from_u64(self.reader.read_var_uint().await?)?;
        let packet: Result<ServerPacket> = match packet_id {
            ServerPacketId::Hello => self.read_hello().await.map(ServerPacket::Hello),
            ServerPacketId::Data => Ok(ServerPacket::Data(
                self.receive_data(CompressionMethod::default()).await?,
            )),
            ServerPacketId::Exception => {
                log::debug!("receiving server exception packet");
                Ok(ServerPacket::Exception(self.read_exception().await?))
            }
            ServerPacketId::Progress => Ok(ServerPacket::Progress(self.read_progress().await?)),
            ServerPacketId::Pong => Ok(ServerPacket::Pong),
            ServerPacketId::EndOfStream => Ok(ServerPacket::EndOfStream),
            ServerPacketId::ProfileInfo => {
                let rows = self.reader.read_var_uint().await?;
                let blocks = self.reader.read_var_uint().await?;
                let bytes = self.reader.read_var_uint().await?;
                let applied_limit = self.reader.read_u8().await? != 0;
                let rows_before_limit = self.reader.read_var_uint().await?;
                let calculated_rows_before_limit = self.reader.read_u8().await? != 0;
                Ok(ServerPacket::ProfileInfo(BlockStreamProfileInfo {
                    rows,
                    blocks,
                    bytes,
                    applied_limit,
                    rows_before_limit,
                    calculated_rows_before_limit,
                }))
            }
            ServerPacketId::Totals => Ok(ServerPacket::Totals(
                self.receive_data(CompressionMethod::default()).await?,
            )),
            ServerPacketId::Extremes => Ok(ServerPacket::Extremes(
                self.receive_data(CompressionMethod::default()).await?,
            )),
            ServerPacketId::TablesStatusResponse => {
                let mut response = TablesStatusResponse {
                    database_tables: IndexMap::new(),
                };
                let size = self.reader.read_var_uint().await?;
                if size as usize > MAX_STRING_SIZE {
                    return Err(KlickhouseError::ProtocolError(format!(
                        "table status response size too large. {} > {}",
                        size, MAX_STRING_SIZE
                    )));
                }
                for _ in 0..size {
                    let database_name = self.reader.read_utf8_string().await?;
                    let table_name = self.reader.read_utf8_string().await?;
                    let is_replicated = self.reader.read_u8().await? != 0;
                    let absolute_delay = if is_replicated {
                        self.reader.read_var_uint().await? as u32
                    } else {
                        0
                    };
                    response
                        .database_tables
                        .entry(database_name)
                        .or_default()
                        .insert(
                            table_name,
                            TableStatus {
                                is_replicated,
                                absolute_delay,
                            },
                        );
                }
                Ok(ServerPacket::TablesStatusResponse(response))
            }
            ServerPacketId::Log => {
                let data = self.read_log_data().await?;
                Ok(ServerPacket::Log(data))
            }
            ServerPacketId::TableColumns => {
                let name = self.reader.read_utf8_string().await?;
                let description = self.reader.read_utf8_string().await?;
                Ok(ServerPacket::TableColumns(TableColumns {
                    name,
                    description,
                }))
            }
            ServerPacketId::PartUUIDs => {
                let len = self.reader.read_var_uint().await?;
                if len as usize > MAX_STRING_SIZE {
                    return Err(KlickhouseError::ProtocolError(format!(
                        "PartUUIDs response size too large. {} > {}",
                        len, MAX_STRING_SIZE
                    )));
                }
                let mut out = Vec::with_capacity(len as usize);
                let mut bytes = [0u8; 16];
                for _ in 0..len {
                    self.reader.read_exact(&mut bytes[..]).await?;

                    out.push(Uuid::from_bytes(bytes));
                }
                log::debug!("received part uuids: {:?}", out);
                Ok(ServerPacket::PartUUIDs(out))
            }
            ServerPacketId::ReadTaskRequest => Ok(ServerPacket::ReadTaskRequest),
            ServerPacketId::ProfileEvents => Ok(ServerPacket::ProfileEvents(
                self.read_profile_events().await?,
            )),
        };

        Ok(packet?)
    }

    pub async fn receive_hello(&mut self) -> Result<ServerHello> {
        match self.receive_packet().await? {
            ServerPacket::Hello(hello) => Ok(hello),
            ServerPacket::Exception(e) => Err(e.emit()),
            packet => Err(KlickhouseError::ProtocolError(format!(
                "unexpected packet {:?}, expected server hello",
                packet
            ))),
        }
    }

    async fn read_hello(&mut self) -> Result<ServerHello> {
        log::debug!("receiving server hello packet");
        let server_name = self.reader.read_utf8_string().await?;
        let major_version = self.reader.read_var_uint().await?;
        let minor_version = self.reader.read_var_uint().await?;

        let revision_version = self.reader.read_var_uint().await?;
        let revision_version = std::cmp::min(revision_version, DBMS_TCP_PROTOCOL_VERSION);

        let timezone = if revision_version >= DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE {
            Some(self.reader.read_utf8_string().await?)
        } else {
            None
        };

        let display_name = if revision_version >= DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME {
            Some(self.reader.read_utf8_string().await?)
        } else {
            None
        };
        let patch_version = if revision_version >= DBMS_MIN_REVISION_WITH_VERSION_PATCH {
            self.reader.read_var_uint().await?
        } else {
            revision_version
        };

        if revision_version >= DBMS_MIN_PROTOCOL_VERSION_WITH_PASSWORD_COMPLEXITY_RULES {
            let rules_size = self.reader.read_var_uint().await?;
            for _ in 0..rules_size {
                let _ = self.reader.read_utf8_string().await?;
            }
        }

        if revision_version >= DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_V2 {
            self.reader.read_u64_le().await?;
        }

        log::debug!(
            "Connected to {} server version {}.{}.{}, revision: {}",
            server_name,
            major_version,
            minor_version,
            patch_version,
            revision_version
        );

        Ok(ServerHello {
            server_name,
            major_version,
            minor_version,
            revision_version,
            timezone,
            display_name,
            patch_version,
        })
    }

    async fn read_log_data(&mut self) -> Result<ServerData> {
        // TODO: Deserialize log data into proper struct
        self.receive_data(CompressionMethod::None).await
    }

    async fn read_progress(&mut self) -> Result<Progress> {
        let read_rows = self.reader.read_var_uint().await?;
        let read_bytes = self.reader.read_var_uint().await?;
        let new_total_rows_to_read =
            if self.server_hello.revision_version >= DBMS_MIN_REVISION_WITH_SERVER_LOGS {
                self.reader.read_var_uint().await?
            } else {
                0
            };
        let new_written_rows =
            if self.server_hello.revision_version >= DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO {
                Some(self.reader.read_var_uint().await?)
            } else {
                None
            };
        let new_written_bytes =
            if self.server_hello.revision_version >= DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO {
                Some(self.reader.read_var_uint().await?)
            } else {
                None
            };
        let elapsed_ns = if self.server_hello.revision_version
            >= DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS
        {
            Some(self.reader.read_var_uint().await?)
        } else {
            None
        };
        Ok(Progress {
            read_rows,
            read_bytes,
            new_total_rows_to_read,
            new_written_rows,
            new_written_bytes,
            elapsed_ns,
        })
    }

    async fn read_profile_events(&mut self) -> Result<Option<ServerData>> {
        if self.server_hello.revision_version
            < DBMS_MIN_PROTOCOL_VERSION_WITH_PROFILE_EVENTS_IN_INSERT
        {
            return Ok(None);
        }

        self.receive_data(CompressionMethod::None)
            .await
            .map(Option::Some)
    }
}
