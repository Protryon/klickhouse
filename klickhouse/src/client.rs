use std::{collections::VecDeque};

use futures::{Stream, StreamExt, stream};
use indexmap::IndexMap;
use tokio::{io::{AsyncRead, AsyncWrite, BufReader, BufWriter}, net::{TcpStream, ToSocketAddrs}, select, sync::{ mpsc::{self, Receiver}, oneshot}};
use tokio_stream::wrappers::ReceiverStream;

use crate::{block::{Block, BlockInfo}, convert::{Row}, internal_client_in::InternalClientIn, internal_client_out::{ClientHello, ClientInfo, InternalClientOut, Query, QueryKind, QueryProcessingStage}, io::{ClickhouseRead, ClickhouseWrite}, protocol::{self, ServerPacket}};
use log::*;
use anyhow::*;

struct InnerClient<R: ClickhouseRead, W: ClickhouseWrite> {
    input: InternalClientIn<R>,
    output: InternalClientOut<W>,
    options: ClientOptions,
    pending_queries: VecDeque<mpsc::Sender<Block>>,
}

impl<R: ClickhouseRead, W: ClickhouseWrite> InnerClient<R, W> {
    pub fn new(reader: R, writer: W, options: ClientOptions) -> Self {
        Self {
            input: InternalClientIn::new(reader),
            output: InternalClientOut::new(writer),
            options,
            pending_queries: VecDeque::new(),
        }
    }

    async fn handle_request(&mut self, request: ClientRequest) -> Result<()> {
        match request.data {
            ClientRequestData::Query { query, response } => {
                self.output.send_query(Query {
                    id: "",
                    info: ClientInfo {
                        kind: QueryKind::InitialQuery,
                        initial_user: "",
                        initial_query_id: "",
                        initial_address: "0.0.0.0:0",
                        os_user: "",
                        client_hostname: "localhost",
                        client_name: "ClickHouseclient",
                        client_version_major: crate::VERSION_MAJOR,
                        client_version_minor: crate::VERSION_MINOR,
                        client_tcp_protocol_version: protocol::DBMS_TCP_PROTOCOL_VERSION,
                        quota_key: "",
                        distributed_depth: 1,
                        client_version_patch: 1,
                        open_telemetry: None,
                    },
                    stage: QueryProcessingStage::Complete,
                    compression: false,
                    query: &*query,
                }).await?;
                
                let (sender, receiver) = mpsc::channel(32);
                response.send(receiver).ok();
                self.pending_queries.push_back(sender);
                self.output.send_data(&Block {
                    info: BlockInfo::default(),
                    rows: 0,
                    column_types: IndexMap::new(),
                    column_data: IndexMap::new(),
                }, "", false).await?;
            },
            ClientRequestData::SendData { block, response } => {
                self.output.send_data(&block, "", false).await?;
                response.send(()).ok();
            }
        }
        Ok(())
    }

    async fn receive_packet(&mut self, packet: ServerPacket) -> Result<()> {
        match packet {
            ServerPacket::Hello(_) => return Err(anyhow!("unexpected retransmission of server hello")),
            ServerPacket::Data(block) => {
                if let Some(current) = self.pending_queries.front() {
                    current.send(block.block).await.ok();
                } else {
                    return Err(anyhow!("received data block, but no pending queries"));
                }
            }
            ServerPacket::Exception(e) => {
                return Err(e.emit());
            }
            ServerPacket::Progress(_) => {

            }
            ServerPacket::Pong => {

            }
            ServerPacket::EndOfStream => {
                if self.pending_queries.pop_front().is_some() {
                    // drop sender
                } else {
                    return Err(anyhow!("received end of stream, but no pending queries"));
                }
            }
            ServerPacket::ProfileInfo(_) => {

            }
            ServerPacket::Totals(_) => {

            }
            ServerPacket::Extremes(_) => {

            }
            ServerPacket::TablesStatusResponse(_) => {

            }
            ServerPacket::Log(_) => {

            }
            ServerPacket::TableColumns(_) => {

            }
            ServerPacket::PartUUIDs(_) => {

            }
            ServerPacket::ReadTaskRequest => {

            }
        }
        Ok(())
    }

    async fn run_inner(mut self, mut input: Receiver<ClientRequest>) -> Result<()> {
        self.output.send_hello(ClientHello {
            default_database: &self.options.default_database,
            username: &self.options.username,
            password: &self.options.password,
        }).await?;
        let hello_response = self.input.receive_hello().await?;
        self.input.server_hello = hello_response.clone();
        self.output.server_hello = hello_response.clone();

        loop {
            select! {
                request = input.recv() => {
                    if request.is_none() {
                        return Ok(());
                    }
                    self.handle_request(request.unwrap()).await?;
                },
                packet = self.input.receive_packet() => {
                    let packet = packet?;
                    self.receive_packet(packet).await?;
                },
            }
        }
    }

    pub async fn run(self, input: Receiver<ClientRequest>) {
        if let Err(e) = self.run_inner(input).await {
            error!("clickhouse client failed: {:?}", e);
        }
    }
}

enum ClientRequestData {
    Query {
        query: String,
        response: oneshot::Sender<mpsc::Receiver<Block>>,
    },
    SendData {
        block: Block,
        response: oneshot::Sender<()>,
    }
}

struct ClientRequest {
    data: ClientRequestData,
}

#[derive(Clone)]
pub struct Client {
    sender: mpsc::Sender<ClientRequest>,
}

#[derive(Debug, Clone)]
pub struct ClientOptions {
    pub username: String,
    pub password: String,
    pub default_database: String,
}

impl Default for ClientOptions {
    fn default() -> Self {
        ClientOptions {
            username: "default".to_string(),
            password: String::new(),
            default_database: String::new(),
        }
    }
}

impl Client {
    pub fn connect_stream(read: impl AsyncRead + Unpin + Send + Sync + 'static, writer: impl AsyncWrite + Unpin + Send + Sync + 'static, options: ClientOptions) -> Self {
        Self::start(InnerClient::new(BufReader::new(read), BufWriter::new(writer), options))
    }

    pub async fn connect<A: ToSocketAddrs>(destination: A, options: ClientOptions) -> std::io::Result<Self> {
        let (read, writer) = TcpStream::connect(destination).await?.into_split();
        Self::connect_direct(read, writer, options).await
    }

    pub async fn connect_direct<R: AsyncRead + Unpin + Send + Sync + 'static, W: AsyncWrite + Unpin + Send + Sync + 'static>(reader: R, writer: W, options: ClientOptions) -> std::io::Result<Self> {
        Ok(Self::start(InnerClient::new(BufReader::new(reader), BufWriter::new(writer), options)))
    }

    fn start<R: ClickhouseRead, W: ClickhouseWrite>(inner: InnerClient<R, W>) -> Self {
        let (sender, receiver) = mpsc::channel(1024);
        tokio::spawn(inner.run(receiver));
        Client {
            sender
        }
    }

    pub async fn query_raw(&self, query: &str) -> Result<impl Stream<Item=Block>> {
        let (sender, receiver) = oneshot::channel();
        self.sender.send(ClientRequest {
            data: ClientRequestData::Query {
                query: query.to_string(),
                response: sender,
            }
        }).await.map_err(|_| anyhow!("failed to send query"))?;
        let receiver = receiver.await?;

        Ok(ReceiverStream::new(receiver))
    }

    async fn send_data(&self, block: Block) -> Result<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender.send(ClientRequest {
            data: ClientRequestData::SendData {
                block,
                response: sender,
            }
        }).await.map_err(|_| anyhow!("failed to send block"))?;
        receiver.await?;

        Ok(())
    }

    pub async fn insert_native_raw(&self, query: &str, mut blocks: impl Stream<Item=Block> + Send + Sync + Unpin + 'static) -> Result<impl Stream<Item=Block>> {
        let (sender, receiver) = oneshot::channel();
        self.sender.send(ClientRequest {
            data: ClientRequestData::Query {
                query: query.to_string(),
                response: sender,
            }
        }).await.map_err(|_| anyhow!("failed to send query"))?;
        let receiver = receiver.await?;
        
        while let Some(block) = blocks.next().await {
            self.send_data(block).await?;
        }
        self.send_data(Block {
            info: BlockInfo::default(),
            rows: 0,
            column_types: IndexMap::new(),
            column_data: IndexMap::new(),
        }).await?;

        Ok(ReceiverStream::new(receiver))
    }

    pub async fn insert_native<T: Row + Send + Sync + 'static>(&self, query: &str, mut blocks: impl Stream<Item=Vec<T>> + Send + Sync + Unpin + 'static) -> Result<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender.send(ClientRequest {
            data: ClientRequestData::Query {
                query: query.to_string(),
                response: sender,
            }
        }).await.map_err(|_| anyhow!("failed to send query"))?;
        let mut receiver = receiver.await?;
        let first_block = receiver.recv().await.ok_or_else(|| anyhow!("missing header block from server"))?;
        while let Some(rows) = blocks.next().await {
            let mut block = Block {
                info: BlockInfo::default(),
                rows: rows.len() as u64,
                column_types: first_block.column_types.clone(),
                column_data: IndexMap::new(),
            };
            rows.into_iter()
                .map(|x| x.serialize_row())
                .filter_map(|x| match x {
                    Err(e) => {
                        error!("serialization error during insert (SKIPPED ROWS!): {:?}", e);
                        None
                    },
                    Ok(x) => Some(x)
                })
                .try_for_each(|x| -> Result<()> {
                    for (key, value) in x {
                        let type_ = first_block.column_types.get(key).ok_or_else(|| anyhow!("missing type for data"))?;
                        type_.validate_value(&value)?;
                        if let Some(column) = block.column_data.get_mut(key) {
                            column.push(value);
                        } else {
                            block.column_data.insert(key.to_string(), vec![value]);
                        }
                    }
                    Ok(())
                })?;
            self.send_data(block).await?;
        }
        self.send_data(Block {
            info: BlockInfo::default(),
            rows: 0,
            column_types: IndexMap::new(),
            column_data: IndexMap::new(),
        }).await?;
        Ok(())
    }

    pub async fn insert_native_block<T: Row + Send + Sync + 'static>(&self, query: &str, blocks: Vec<T>) -> Result<()> {
        let blocks = Box::pin(async move { blocks });
        let stream = futures::stream::once(blocks);
        self.insert_native(query, stream).await
    }

    pub async fn query<T: Row>(&self, query: &str) -> Result<impl Stream<Item=Result<T>>> {
        let raw = self.query_raw(query).await?;
        Ok(raw.flat_map(|mut block| {
            let blocks = block.take_iter_rows()
                .filter(|x| !x.is_empty())
                .map(|m| {
                    T::deserialize_row(m)
                }).collect::<Vec<_>>();
            stream::iter(blocks)
        }))
    }
}