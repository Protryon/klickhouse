use crate::{block::{Block}, io::ClickhouseRead, progress::Progress, protocol::{self, BlockStreamProfileInfo, DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO, DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME, DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE, DBMS_MIN_REVISION_WITH_VERSION_PATCH, MAX_STRING_SIZE, ServerData, ServerException, ServerHello, ServerPacket, TableColumns, TableStatus, TablesStatusResponse}};
use anyhow::*;
use indexmap::IndexMap;
use protocol::ServerPacketId;
use tokio::io::{AsyncReadExt};
use uuid::Uuid;


pub struct InternalClientIn<R: ClickhouseRead> {
    reader: R,
    pub server_hello: ServerHello,
}

impl<R: ClickhouseRead> InternalClientIn<R> {
    pub fn new(reader: R) -> Self {
        InternalClientIn {
            reader,
            server_hello: ServerHello::default(),
        }
    }

    async fn read_exception(&mut self) -> Result<ServerException> {
        let code = self.reader.read_i32().await?;
        let name = self.reader.read_string().await?;
        let message = self.reader.read_string().await?;
        let stack_trace = self.reader.read_string().await?;
        let has_nested = self.reader.read_u8().await? != 0;

        Ok(ServerException {
            code,
            name,
            message,
            stack_trace,
            has_nested,
        })
    }

    async fn receive_data(&mut self) -> Result<ServerData> {
        let table_name = self.reader.read_string().await?;

        let block = Block::read(&mut self.reader, self.server_hello.revision_version).await?;

        Ok(ServerData {
            table_name,
            block,
        })
    }

    async fn receive_log_data(&mut self) -> Result<ServerData> {
        unimplemented!()
    }

    pub async fn receive_packet(&mut self) -> Result<ServerPacket> {
        let packet_id = ServerPacketId::from_u64(self.reader.read_var_uint().await?)?;
        match packet_id {
            ServerPacketId::Hello => {
                let server_name = self.reader.read_string().await?;
                let major_version = self.reader.read_var_uint().await?;
                let minor_version = self.reader.read_var_uint().await?;
                let revision_version = self.reader.read_var_uint().await?;
                let timezone = if revision_version > DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE {
                    Some(self.reader.read_string().await?)
                } else {
                    None
                };
                let display_name = if revision_version > DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME {
                    Some(self.reader.read_string().await?)
                } else {
                    None
                };
                let patch_version = if revision_version > DBMS_MIN_REVISION_WITH_VERSION_PATCH {
                    self.reader.read_var_uint().await?
                } else {
                    revision_version
                };
                Ok(ServerPacket::Hello(ServerHello {
                    server_name,
                    major_version,
                    minor_version,
                    revision_version,
                    timezone,
                    display_name,
                    patch_version,
                }))
            }
            ServerPacketId::Data => {
                Ok(ServerPacket::Data(self.receive_data().await?))
            }
            ServerPacketId::Exception => {
                Ok(ServerPacket::Exception(self.read_exception().await?))
            }
            ServerPacketId::Progress => {
                let read_rows = self.reader.read_var_uint().await?;
                let read_bytes = self.reader.read_var_uint().await?;
                let new_total_rows_to_read = self.reader.read_var_uint().await?;
                let new_written_rows = if self.server_hello.revision_version >= DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO {
                    Some(self.reader.read_var_uint().await?)
                } else {
                    None
                };
                let new_written_bytes = if self.server_hello.revision_version >= DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO {
                    Some(self.reader.read_var_uint().await?)
                } else {
                    None
                };
                Ok(ServerPacket::Progress(Progress {
                    read_rows,
                    read_bytes,
                    new_total_rows_to_read,
                    new_written_rows,
                    new_written_bytes,
                }))
            }
            ServerPacketId::Pong => {
                Ok(ServerPacket::Pong)
            }
            ServerPacketId::EndOfStream => {
                Ok(ServerPacket::EndOfStream)
            }
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
            ServerPacketId::Totals => {
                Ok(ServerPacket::Totals(self.receive_data().await?))
            }
            ServerPacketId::Extremes => {
                Ok(ServerPacket::Extremes(self.receive_data().await?))
            }
            ServerPacketId::TablesStatusResponse => {
                let mut response = TablesStatusResponse {
                    database_tables: IndexMap::new(),
                };
                let size = self.reader.read_var_uint().await?;
                if size as usize > MAX_STRING_SIZE {
                    return Err(anyhow!("table status response size too large"));
                }
                for _ in 0..size {
                    let database_name = self.reader.read_string().await?;
                    let table_name = self.reader.read_string().await?;
                    let is_replicated = self.reader.read_u8().await? != 0;
                    let absolute_delay = if is_replicated {
                        self.reader.read_var_uint().await? as u32
                    } else {
                        0
                    };
                    response.database_tables
                        .entry(database_name)
                        .or_insert_with(IndexMap::new)
                        .insert(table_name, TableStatus {
                            is_replicated,
                            absolute_delay,
                        });
                }
                Ok(ServerPacket::TablesStatusResponse(response))
            }
            ServerPacketId::Log => {
                Ok(ServerPacket::Log(self.receive_log_data().await?))
            }
            ServerPacketId::TableColumns => {
                let name = self.reader.read_string().await?;
                let description = self.reader.read_string().await?;
                Ok(ServerPacket::TableColumns(TableColumns {
                    name,
                    description,
                }))
            }
            ServerPacketId::PartUUIDs => {
                let len = self.reader.read_var_uint().await?;
                let mut out = Vec::with_capacity(len as usize);
                let mut bytes = [0u8; 16];
                for _ in 0..len {
                    self.reader.read_exact(&mut bytes[..]).await?;

                    out.push(Uuid::from_bytes(bytes));
                }
                Ok(ServerPacket::PartUUIDs(out))
            }
            ServerPacketId::ReadTaskRequest => {
                Ok(ServerPacket::ReadTaskRequest)
            }
        }
    }

    pub async fn receive_hello(&mut self) -> Result<ServerHello> {
        match self.receive_packet().await? {
            ServerPacket::Hello(hello) => Ok(hello),
            ServerPacket::Exception(e) => Err(e.emit()),
            packet => Err(anyhow!("unexpected packet {:?}, expected server hello", packet)),
        }
    }
}