use crate::{store::StoreCommand, RaftResult, Shared};
use bstr::ByteSlice;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::{net::TcpStream, sync::TryAcquireError};
use tokio_util::codec::Framed;
use zakros_raft::{Error as RaftError, NodeId};
use zakros_redis::{
    command::{Arity, RedisCommand, SystemCommand, TransactionCommand},
    error::{ConnectionError, Error as RedisError, ResponseError},
    resp::{RespCodec, Value},
    BytesExt, RedisResult,
};

pub(crate) async fn serve(shared: Arc<Shared>, mut conn: TcpStream) -> std::io::Result<()> {
    match shared.conn_limit.clone().try_acquire_owned() {
        Ok(_permit) => RedisConnection::new(shared, conn).serve().await,
        Err(TryAcquireError::Closed) => unreachable!(),
        Err(TryAcquireError::NoPermits) => {
            use tokio::io::AsyncWriteExt;
            conn.write_all(b"-ERR max number of clients reached").await
        }
    }
}

struct RedisConnection {
    shared: Arc<Shared>,
    framed: Framed<TcpStream, RespCodec>,
    txn: Transaction,
    is_readonly: bool,
}

impl RedisConnection {
    pub(crate) fn new(shared: Arc<Shared>, conn: TcpStream) -> Self {
        Self {
            shared: shared.clone(),
            framed: Framed::new(conn, RespCodec::default()),
            txn: Transaction::Inactive,
            is_readonly: false,
        }
    }

    pub async fn serve(mut self) -> std::io::Result<()> {
        while let Some(decoded) = self.framed.next().await {
            match decoded {
                Ok(strings) => {
                    tracing::trace!("received {:?}", DebugQuery(&strings));
                    let Some((command, args)) = strings.split_first() else {
                        continue;
                    };
                    self.handle_command(command, args).await?;
                }
                Err(ConnectionError::Io(err)) => return Err(err),
                Err(ConnectionError::Protocol(err)) => {
                    return self
                        .framed
                        .send(&Err(ResponseError::ProtocolError(err).into()))
                        .await;
                }
            }
        }
        Ok(())
    }

    async fn handle_command(&mut self, command: &[u8], args: &[Bytes]) -> std::io::Result<()> {
        match self.parse_and_process_command(command, args).await {
            Ok(()) => Ok(()),
            Err(Error::Io(err)) => Err(err),
            Err(Error::Redis(err)) => self.framed.send(&Err(err)).await,
            Err(Error::Raft(err)) => match err {
                RaftError::NotLeader { leader_id: None } => {
                    self.framed
                        .send(&Err(RedisError::ClusterDown("No leader".to_owned())))
                        .await
                }
                RaftError::NotLeader {
                    leader_id: Some(leader_id),
                } => {
                    let addr =
                        &self.shared.opts.cluster_addrs[Into::<u64>::into(leader_id) as usize];
                    self.framed
                        .send(&Err(RedisError::Moved {
                            slot: 0,
                            addr: *addr,
                        }))
                        .await
                }
                RaftError::Shutdown => panic!("Raft server is shut down"),
            },
        }
    }

    async fn parse_and_process_command(
        &mut self,
        command: &[u8],
        args: &[Bytes],
    ) -> Result<(), Error> {
        let command = match RedisCommand::try_from(command) {
            Ok(command) => command,
            Err(err) => {
                if matches!(self.txn, Transaction::Queued(_)) {
                    self.txn = Transaction::Error;
                }
                return Err(err.into());
            }
        };
        match command.arity() {
            Arity::Fixed(n) if args.len() == n => (),
            Arity::AtLeast(n) if args.len() >= n => (),
            _ => {
                if matches!(self.txn, Transaction::Queued(_)) {
                    self.txn = Transaction::Error;
                }
                return Err(RedisError::from(ResponseError::WrongArity).into());
            }
        }

        match command {
            RedisCommand::Transaction(command) => match command {
                TransactionCommand::Multi => match self.txn {
                    Transaction::Inactive => {
                        self.txn = Transaction::Queued(Default::default());
                        self.framed.send(&Ok(Value::ok())).await?;
                        Ok(())
                    }
                    Transaction::Queued(_) | Transaction::Error => Err(RedisError::from(
                        ResponseError::Other("MULTI calls can not be nested"),
                    )
                    .into()),
                },
                TransactionCommand::Exec => match &mut self.txn {
                    Transaction::Inactive => {
                        Err(RedisError::from(ResponseError::Other("EXEC without MULTI")).into())
                    }
                    Transaction::Queued(queue) => {
                        let commands = std::mem::take(queue);
                        self.txn = Transaction::Inactive;
                        self.exec(commands).await
                    }
                    Transaction::Error => {
                        self.txn = Transaction::Inactive;
                        Err(RedisError::ExecAbort.into())
                    }
                },
                TransactionCommand::Discard => match self.txn {
                    Transaction::Inactive => {
                        Err(RedisError::from(ResponseError::Other("DISCARD without MULTI")).into())
                    }
                    Transaction::Queued(_) | Transaction::Error => {
                        self.txn = Transaction::Inactive;
                        self.framed.send(&Ok(Value::ok())).await?;
                        Ok(())
                    }
                },
            },
            RedisCommand::System(_) if !matches!(self.txn, Transaction::Inactive) => {
                self.txn = Transaction::Error;
                Err(RedisError::from(ResponseError::Other(
                    "Command not allowed inside a transaction",
                ))
                .into())
            }
            _ if !matches!(self.txn, Transaction::Inactive) => {
                if let Transaction::Queued(queue) = &mut self.txn {
                    queue.push((command, args.to_vec()));
                }
                self.framed.send(&Ok("QUEUED".into())).await?;
                Ok(())
            }
            _ => self.call_single(command, args).await,
        }
    }

    async fn call_single(&mut self, command: RedisCommand, args: &[Bytes]) -> Result<(), Error> {
        let result = match command {
            RedisCommand::Write(command) => {
                self.shared
                    .raft
                    .write(StoreCommand::SingleWrite((command, args.to_vec())))
                    .await?
            }
            RedisCommand::Read(command) => {
                if !self.is_readonly {
                    self.shared.raft.read().await?;
                }
                command.call(&self.shared.store, args)
            }
            RedisCommand::Stateless(command) => command.call(args),
            RedisCommand::System(command) => match command {
                SystemCommand::Cluster => self.shared.cluster(args).await?,
                SystemCommand::Info => self.shared.info(args),
                SystemCommand::ReadOnly => self.readonly(args),
                SystemCommand::ReadWrite => self.readwrite(args),
                SystemCommand::Select => select(args),
                SystemCommand::Shutdown => shutdown(args),
            },
            RedisCommand::Transaction(_) => unreachable!(),
        };
        self.framed.send(&result).await?;
        Ok(())
    }

    async fn exec(&mut self, commands: Vec<(RedisCommand, Vec<Bytes>)>) -> Result<(), Error> {
        let result = self.shared.raft.write(StoreCommand::Exec(commands)).await?;
        self.framed.send(&result).await?;
        Ok(())
    }

    fn readonly(&mut self, args: &[Bytes]) -> RedisResult {
        if args.is_empty() {
            self.is_readonly = true;
            Ok(Value::ok())
        } else {
            Err(ResponseError::WrongArity.into())
        }
    }

    fn readwrite(&mut self, args: &[Bytes]) -> RedisResult {
        if args.is_empty() {
            self.is_readonly = false;
            Ok(Value::ok())
        } else {
            Err(ResponseError::WrongArity.into())
        }
    }
}

#[derive(Default)]
enum Transaction {
    #[default]
    Inactive,
    Queued(Vec<(RedisCommand, Vec<Bytes>)>),
    Error,
}

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Redis(#[from] RedisError),

    #[error(transparent)]
    Raft(#[from] RaftError),
}

struct DebugQuery<'a>(&'a [Bytes]);

impl std::fmt::Debug for DebugQuery<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.is_empty() {
            return f.write_str("(empty)");
        }
        for s in self.0 {
            if s.len() > 30 {
                write!(f, "{:?}... ", &s[..30].as_bstr())?;
            } else {
                write!(f, "{:?} ", s.as_bstr())?;
            }
        }
        Ok(())
    }
}

impl Shared {
    async fn cluster(&self, args: &[Bytes]) -> RaftResult<RedisResult> {
        fn format_node_id(node_id: NodeId) -> RedisResult {
            Ok(Bytes::from(format!("{:0>40x}", Into::<u64>::into(node_id)).into_bytes()).into())
        }

        fn format_node(node_id: NodeId, addr: SocketAddr) -> RedisResult {
            Ok(Value::Array(vec![
                Ok(Bytes::from(addr.ip().to_string().into_bytes()).into()),
                Ok((addr.port() as i64).into()),
                format_node_id(node_id),
            ]))
        }

        let [subcommand, _args @ ..] = args else {
            return Ok(Err(ResponseError::WrongArity.into()));
        };
        match subcommand.to_ascii_uppercase().as_slice() {
            b"MYID" => Ok(format_node_id(NodeId::from(self.opts.id))),
            b"SLOTS" => {
                const CLUSTER_SLOTS: i64 = 16384;
                let leader_id = self.raft.status().await?.leader_id;
                let Some(leader_id) = leader_id else {
                    return Err(RaftError::NotLeader { leader_id: None });
                };
                let leader_index = Into::<u64>::into(leader_id) as usize;
                let addrs = &self.opts.cluster_addrs;
                let mut responses = vec![Ok(0.into()), Ok((CLUSTER_SLOTS - 1).into())];
                responses.reserve(addrs.len());
                responses.push(format_node(leader_id, addrs[leader_index]));
                for (i, addr) in addrs.iter().enumerate() {
                    if i != leader_index {
                        responses.push(format_node(NodeId::from(i as u64), *addr));
                    }
                }
                Ok(Ok(Value::Array(vec![Ok(Value::Array(responses))])))
            }
            _ => Ok(Err(ResponseError::UnknownSubcommand(
                String::from_utf8_lossy(subcommand).into_owned(),
            )
            .into())),
        }
    }

    fn info(&self, args: &[Bytes]) -> RedisResult {
        let mut sections;
        if args.is_empty() {
            sections = ALL;
        } else {
            sections = 0;
            for section in args {
                match section.to_ascii_lowercase().as_slice() {
                    b"server" => sections |= SERVER,
                    b"clients" => sections |= CLIENTS,
                    b"cluster" => sections |= CLUSTER,
                    b"default" | b"all" | b"everything" => sections |= ALL,
                    _ => (),
                }
            }
        }
        Ok(self.generate_info_str(sections).unwrap().into())
    }

    fn generate_info_str(&self, sections: u8) -> std::io::Result<Bytes> {
        use std::io::Write;

        let mut out = Vec::new();
        let mut is_first = true;
        if sections & SERVER != 0 {
            is_first = false;
            out.write_all(b"# Server\r\n")?;
            write!(out, "tcp_port:{}\r\n", self.opts.bind_addr.port())?;
            let now = SystemTime::now();
            let since_epoch = now
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or(Duration::ZERO)
                .as_micros();
            let uptime = now
                .duration_since(self.started_at)
                .unwrap_or(Duration::ZERO)
                .as_secs();
            write!(out, "server_time_usec:{}\r\n", since_epoch)?;
            write!(out, "uptime_in_seconds:{}\r\n", uptime)?;
            write!(out, "uptime_in_days:{}\r\n", uptime / (3600 * 24))?;
        }
        if sections & CLIENTS != 0 {
            if !is_first {
                out.write_all(b"\r\n")?;
            }
            out.write_all(b"# Clients\r\n")?;
            write!(
                out,
                "connected_clients:{}\r\n",
                self.opts.max_num_clients - self.conn_limit.available_permits()
            )?;
            write!(out, "maxclients:{}\r\n", self.opts.max_num_clients)?;
        }
        if sections & CLUSTER != 0 {
            if !is_first {
                out.write_all(b"\r\n")?;
            }
            out.write_all(b"# Cluster\r\n")?;
            write!(out, "cluster_enabled:1\r\n")?;
        }
        Ok(Bytes::from(out))
    }
}

const SERVER: u8 = 0x1;
const CLIENTS: u8 = 0x2;
const CLUSTER: u8 = 0x4;
const ALL: u8 = SERVER | CLIENTS | CLUSTER;

fn select(args: &[Bytes]) -> RedisResult {
    let [index] = args else {
        return Err(ResponseError::WrongArity.into());
    };
    if index.to_i32()? == 0 {
        Ok(Value::ok())
    } else {
        Err(ResponseError::Other("SELECT is not allowed in cluster mode").into())
    }
}

fn shutdown(_: &[Bytes]) -> RedisResult {
    // TODO: gracefully shutdown
    std::process::exit(0)
}
