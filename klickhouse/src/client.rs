use std::collections::VecDeque;

use futures::{Stream, StreamExt};
use protocol::CompressionMethod;
use tokio::{
    io::{AsyncRead, AsyncWrite, BufReader, BufWriter},
    net::{TcpStream, ToSocketAddrs},
    select,
    sync::{
        broadcast,
        mpsc::{self, Receiver},
        oneshot,
    },
};
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

use crate::{
    block::{Block, BlockInfo},
    convert::Row,
    internal_client_in::InternalClientIn,
    internal_client_out::{
        ClientHello, ClientInfo, InternalClientOut, Query, QueryKind, QueryProcessingStage,
    },
    io::{ClickhouseRead, ClickhouseWrite},
    progress::Progress,
    protocol::{self, ServerPacket},
    KlickhouseError, ParsedQuery, RawRow, Result,
};
use log::*;

// Maximum number of progress statuses to keep in memory. New statuses evict old ones.
const PROGRESS_CAPACITY: usize = 100;

struct InnerClient<R: ClickhouseRead, W: ClickhouseWrite> {
    input: InternalClientIn<R>,
    output: InternalClientOut<W>,
    options: ClientOptions,
    pending_queries: VecDeque<PendingQuery>,
    executing_query: Option<(Uuid, mpsc::Sender<Result<Block>>)>,
    progress: broadcast::Sender<(Uuid, Progress)>,
}

#[derive(Debug)]
struct PendingQuery {
    query: String,
    response: oneshot::Sender<mpsc::Receiver<Result<Block>>>,
}

impl<R: ClickhouseRead + 'static, W: ClickhouseWrite> InnerClient<R, W> {
    pub fn new(reader: R, writer: W, options: ClientOptions) -> Self {
        Self {
            input: InternalClientIn::new(reader),
            output: InternalClientOut::new(writer),
            options,
            pending_queries: VecDeque::new(),
            executing_query: None,
            progress: broadcast::channel(PROGRESS_CAPACITY).0,
        }
    }

    async fn dispatch_query(&mut self, query: PendingQuery) -> Result<()> {
        let id = Uuid::new_v4();
        self.output
            .send_query(Query {
                id: &id.to_string(),
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
                compression: CompressionMethod::default(),
                query: &query.query,
            })
            .await?;

        let (sender, receiver) = mpsc::channel(32);
        query.response.send(receiver).ok();
        self.executing_query = Some((id, sender));
        self.output
            .send_data(
                Block {
                    info: BlockInfo::default(),
                    rows: 0,
                    ..Default::default()
                },
                CompressionMethod::default(),
                // TODO: Why is name blank here??
                "",
            )
            .await?;
        Ok(())
    }

    async fn handle_request(&mut self, request: ClientRequest) -> Result<()> {
        match request.data {
            ClientRequestData::Query { query, response } => {
                let query = PendingQuery { query, response };
                if self.pending_queries.is_empty() && self.executing_query.is_none() {
                    self.dispatch_query(query).await?;
                } else {
                    self.pending_queries.push_back(query);
                }
            }
            ClientRequestData::SendData { block, response } => {
                match self
                    .output
                    // TODO: Why is name blank here???
                    .send_data(block, CompressionMethod::default(), "")
                    .await
                {
                    Ok(_) => response.send(Ok(())).ok(),
                    Err(e) => {
                        error!("failed to send data block: {:#?}", e);
                        response.send(Err(e)).ok()
                    }
                };
            }
        }

        Ok(())
    }

    /// receive_packet
    ///
    /// After inner client deserializes server packet, it is fowarded to this function.
    async fn receive_packet(&mut self, packet: Result<ServerPacket>) -> Result<()> {
        // NOTE: Unwrapping the result here so we can notify the consumer of any errors
        let packet = match packet {
            Ok(packet) => packet,
            Err(e) => {
                if let Some((_, current)) = self.executing_query.take() {
                    current.send(Err(e)).await.ok();
                    if let Some(query) = self.pending_queries.pop_front() {
                        self.dispatch_query(query).await?;
                    }
                } else {
                    return Err(e);
                }
                return Ok(());
            }
        };

        match packet {
            ServerPacket::Hello(_) => {
                return Err(KlickhouseError::ProtocolError(
                    "unexpected retransmission of server hello".to_string(),
                ));
            }
            ServerPacket::Data(block) => {
                if let Some((_, current)) = self.executing_query.as_ref() {
                    current.send(Ok(block.block)).await.ok();
                } else {
                    return Err(KlickhouseError::ProtocolError(
                        "received data block, but no pending queries".to_string(),
                    ));
                }
            }
            ServerPacket::Exception(e) => {
                if let Some((_, current)) = self.executing_query.take() {
                    current.send(Err(e.emit())).await.ok();
                    if let Some(query) = self.pending_queries.pop_front() {
                        self.dispatch_query(query).await?;
                    }
                } else {
                    return Err(e.emit());
                }
            }
            ServerPacket::Progress(progress) => {
                if let Some((id, _)) = &self.executing_query {
                    let _ = self.progress.send((*id, progress));
                }
            }
            ServerPacket::Pong => {}
            ServerPacket::EndOfStream => {
                if self.executing_query.take().is_none() {
                    return Err(KlickhouseError::ProtocolError(
                        "received end of stream, but no executing query".to_string(),
                    ));
                }
                if let Some(query) = self.pending_queries.pop_front() {
                    self.dispatch_query(query).await?;
                }
            }
            ServerPacket::ProfileInfo(_) => {}
            ServerPacket::Totals(_) => {}
            ServerPacket::Extremes(_) => {}
            ServerPacket::TablesStatusResponse(_) => {}
            ServerPacket::Log(_) => {}
            ServerPacket::TableColumns(_) => {}
            ServerPacket::PartUUIDs(_) => {}
            ServerPacket::ReadTaskRequest => {}
            ServerPacket::ProfileEvents(_events) => {}
        }
        Ok(())
    }

    async fn run_inner(mut self, mut input: Receiver<ClientRequest>) -> Result<()> {
        match self
            .output
            .send_hello(ClientHello {
                default_database: &self.options.default_database,
                username: &self.options.username,
                password: &self.options.password,
            })
            .await
        {
            Ok(_) => {}
            Err(e) => {
                error!("failed to send hello: {:#?}", e);
                return Err(e);
            }
        };

        let hello_response = match self.input.receive_hello().await {
            Ok(hello_response) => hello_response,
            Err(e) => {
                error!("failed to receive hello: {:#?}", e);
                return Err(e);
            }
        };

        self.input.server_hello = hello_response.clone();
        self.output.server_hello = hello_response.clone();

        // No-op if revision doesn't match
        self.output.send_addendum().await?;

        loop {
            select! {
                request = input.recv() => {
                    if request.is_none() {
                        return Ok(());
                    }
                    match self.handle_request(request.unwrap()).await {
                        Ok(_) => {},
                        Err(e) => error!("failed to handle request: {:?}", e)
                    };
                },
                packet = self.input.receive_packet() => {
                    match self.receive_packet(packet).await {
                        Ok(_) => {},
                        Err(e) => return Err(e)
                    };
                },
            }
        }
    }

    pub async fn run(self, input: Receiver<ClientRequest>) {
        if let Err(e) = self.run_inner(input).await {
            error!("clickhouse client failed: {:#?}", e);
        }
    }

    // TODO: Add ping here, specifically in the context of re-connecting on connection failure
}

enum ClientRequestData {
    Query {
        query: String,
        response: oneshot::Sender<mpsc::Receiver<Result<Block>>>,
    },
    SendData {
        block: Block,
        response: oneshot::Sender<Result<()>>,
    },
}

struct ClientRequest {
    data: ClientRequestData,
}

/// Client handle for a Clickhouse connection, has internal reference to connection, and can be freely cloned and sent across threads.
#[derive(Clone)]
pub struct Client {
    sender: mpsc::Sender<ClientRequest>,
    progress: broadcast::Sender<(Uuid, Progress)>,
}

/// Options set for a Clickhouse connection.
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
    /// Consumes a reader and writer to connect to Klickhouse. To be used for exotic setups or TLS. Generally prefer [`Client::connect()`]
    pub async fn connect_stream(
        read: impl AsyncRead + Unpin + Send + Sync + 'static,
        writer: impl AsyncWrite + Unpin + Send + Sync + 'static,
        options: ClientOptions,
    ) -> Result<Self> {
        Self::start(InnerClient::new(
            BufReader::new(read),
            BufWriter::new(writer),
            options,
        ))
        .await
    }

    /// Connects to a specific socket address over plaintext TCP for Clickhouse.
    pub async fn connect<A: ToSocketAddrs>(destination: A, options: ClientOptions) -> Result<Self> {
        let (read, writer) = TcpStream::connect(destination).await?.into_split();
        Self::connect_stream(read, writer, options).await
    }

    /// Connects to a specific socket address over TLS (rustls) for Clickhouse.
    #[cfg(feature = "tls")]
    pub async fn connect_tls<A: ToSocketAddrs>(
        destination: A,
        options: ClientOptions,
        name: tokio_rustls::rustls::ServerName,
        connector: &tokio_rustls::TlsConnector,
    ) -> Result<Self> {
        let stream = TcpStream::connect(destination).await?;
        let tls_stream = connector.connect(name, stream).await?;
        let (read, writer) = tokio::io::split(tls_stream);
        Self::connect_stream(read, writer, options).await
    }

    async fn start<R: ClickhouseRead + 'static, W: ClickhouseWrite>(
        inner: InnerClient<R, W>,
    ) -> Result<Self> {
        let progress = inner.progress.clone();
        let (sender, receiver) = mpsc::channel(1024);

        tokio::spawn(inner.run(receiver));
        let client = Client { sender, progress };
        Ok(client)
    }

    async fn send_data(&self, block: Block) -> Result<()> {
        let (sender, receiver) = oneshot::channel::<Result<()>>();
        self.sender
            .send(ClientRequest {
                data: ClientRequestData::SendData {
                    block,
                    response: sender,
                },
            })
            .await
            .map_err(|e| KlickhouseError::ProtocolError(format!("failed to send block: {e}")))?;
        receiver.await.map_err(|e| {
            KlickhouseError::ProtocolError(format!("failed to receive blocks from upstream: {e}"))
        })??;

        Ok(())
    }

    /// Sends a query string with streaming associated data (i.e. insert) over native protocol.
    /// Once all outgoing blocks are written (EOF of `blocks` stream), then any response blocks from Clickhouse are read.
    /// You probably want [`Client::insert_native`].
    pub async fn insert_block(
        &self,
        query: impl TryInto<ParsedQuery, Error = KlickhouseError>,
        block: Block,
    ) -> Result<impl Stream<Item = Result<Block>>> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(ClientRequest {
                data: ClientRequestData::Query {
                    query: query.try_into()?.0,
                    response: sender,
                },
            })
            .await
            .map_err(|e| KlickhouseError::ProtocolError(format!("failed to send query: {e}")))?;
        let receiver = receiver.await.map_err(|e| {
            KlickhouseError::ProtocolError(format!("failed to receive blocks from upstream: {e}"))
        })?;

        self.send_data(block).await?;
        self.send_data(Block {
            info: BlockInfo::default(),
            rows: 0,
            ..Default::default()
        })
        .await?;

        Ok(ReceiverStream::new(receiver))
    }

    /// Sends a query string with streaming associated data (i.e. insert) over native protocol.
    /// Once all outgoing blocks are written (EOF of `blocks` stream), then any response blocks from Clickhouse are read.
    /// You probably want [`Client::insert_native`].
    pub async fn insert_native_raw(
        &self,
        query: impl TryInto<ParsedQuery, Error = KlickhouseError>,
        mut blocks: impl Stream<Item = Block> + Send + Sync + Unpin + 'static,
    ) -> Result<impl Stream<Item = Result<Block>>> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(ClientRequest {
                data: ClientRequestData::Query {
                    query: query.try_into()?.0,
                    response: sender,
                },
            })
            .await
            .map_err(|e| KlickhouseError::ProtocolError(format!("failed to send query: {e}")))?;
        let receiver = receiver.await.map_err(|e| {
            KlickhouseError::ProtocolError(format!("failed to receive blocks from upstream: {e}"))
        })?;

        while let Some(block) = blocks.next().await {
            self.send_data(block).await?;
        }
        self.send_data(Block {
            info: BlockInfo::default(),
            rows: 0,
            ..Default::default()
        })
        .await?;

        Ok(ReceiverStream::new(receiver))
    }

    /// Runs a query against Clickhouse, returning a stream of deserialized rows.
    /// Note that no rows are returned until Clickhouse sends a full block (but it usually sends more than one block).
    pub async fn query<T: Row>(
        &self,
        query: impl TryInto<ParsedQuery, Error = KlickhouseError>,
    ) -> Result<impl Stream<Item = Result<Vec<T>>>> {
        let raw = self.query_raw(query).await?;
        Ok(raw.map(|block| match block {
            Ok(mut block) => block
                .take_iter_rows()
                .map(T::deserialize_row)
                .collect::<Result<Vec<_>>>(),
            Err(e) => return Err(e),
        }))
    }

    /// Same as `query`, but returns the first row and discards the rest.
    pub async fn query_one<T: Row>(
        &self,
        query: impl TryInto<ParsedQuery, Error = KlickhouseError>,
    ) -> Result<Vec<T>> {
        self.query::<T>(query)
            .await?
            .next()
            .await
            .unwrap_or_else(|| Err(KlickhouseError::MissingRow))
    }

    /// Sends a query string and read column blocks over a stream.
    /// You probably want [`Client::query()`]
    pub async fn query_raw(
        &self,
        query: impl TryInto<ParsedQuery, Error = KlickhouseError>,
    ) -> Result<impl Stream<Item = Result<Block>>> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(ClientRequest {
                data: ClientRequestData::Query {
                    query: query.try_into()?.0,
                    response: sender,
                },
            })
            .await
            .map_err(|e| KlickhouseError::ProtocolError(format!("failed to send query: {e}")))?;
        let receiver = receiver.await.map_err(|e| {
            KlickhouseError::ProtocolError(format!("failed to receive blocks from upstream: {e}"))
        })?;

        Ok(ReceiverStream::new(receiver))
    }

    /// Same as `query`, but discards all returns blocks. Waits until the first block returns from the server to check for errors.
    /// Waiting for the first response block or EOS also prevents the server from aborting the query potentially due to client disconnection.
    pub async fn execute(
        &self,
        query: impl TryInto<ParsedQuery, Error = KlickhouseError>,
    ) -> Result<()> {
        let mut stream = self.query::<RawRow>(query).await?;
        while let Some(next) = stream.next().await {
            next?;
        }
        Ok(())
    }

    /// Same as `execute`, but doesn't wait for a server response. The query could get aborted if the connection is closed quickly.
    pub async fn execute_now(
        &self,
        query: impl TryInto<ParsedQuery, Error = KlickhouseError>,
    ) -> Result<()> {
        let _ = self.query::<RawRow>(query).await?;
        Ok(())
    }

    /// true if the Client is closed
    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }

    /// Receive progress on the queries as they execute.
    ///
    /// TODO: There is currently no way to retrieve the ID of a query launched
    ///       with `query` or `execute.`
    ///       The signature of these functions should be modified to also return
    ///       an ID (and possibly directly the streaming broadcast).
    pub fn subscribe_progress(&self) -> broadcast::Receiver<(Uuid, Progress)> {
        self.progress.subscribe()
    }
}
