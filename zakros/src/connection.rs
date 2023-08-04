use crate::{store::StoreCommand, RaftResult, Shared};
use async_trait::async_trait;
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
    command::{RedisCommand, SystemCommand},
    error::{ConnectionError, Error as RedisError, ResponseError},
    resp::{RespCodec, Value},
    session::{Session, SessionHandler},
    BytesExt, RedisResult,
};

pub struct RedisConnection {
    shared: Arc<Shared>,
    session: Session<Handler>,
}

impl RedisConnection {
    pub(crate) fn new(shared: Arc<Shared>) -> Self {
        Self {
            shared: shared.clone(),
            session: Session::new(Handler::new(shared)),
        }
    }

    pub async fn serve(mut self, mut conn: TcpStream) -> std::io::Result<()> {
        let _permit = match self.shared.conn_limit.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(TryAcquireError::Closed) => unreachable!(),
            Err(TryAcquireError::NoPermits) => {
                use tokio::io::AsyncWriteExt;
                return conn.write_all(b"-ERR max number of clients reached").await;
            }
        };

        let mut framed = Framed::new(conn, RespCodec::default());
        while let Some(decoded) = framed.next().await {
            match decoded {
                Ok(strings) => {
                    tracing::trace!("received {:?}", DebugQuery(&strings));
                    let Some((command, args)) = strings.split_first() else {
                        continue;
                    };
                    let value = self.call(command, args).await;
                    framed.send(&value).await?;
                }
                Err(ConnectionError::Io(err)) => return Err(err),
                Err(ConnectionError::Protocol(err)) => {
                    return framed
                        .send(&Err(ResponseError::ProtocolError(err).into()))
                        .await;
                }
            }
        }
        Ok(())
    }

    async fn call(&mut self, command: &[u8], args: &[Bytes]) -> RedisResult {
        let result = self.session.call(command, args).await;
        match result {
            Ok(value) => value,
            Err(RaftError::NotLeader { leader_id: None }) => {
                Err(RedisError::ClusterDown("No leader".to_owned()))
            }
            Err(RaftError::NotLeader {
                leader_id: Some(leader_id),
            }) => {
                let addr = &self.shared.opts.cluster_addrs[Into::<u64>::into(leader_id) as usize];
                Err(RedisError::Moved {
                    slot: 0,
                    addr: *addr,
                })
            }
            Err(RaftError::Shutdown) => panic!("Raft server is shut down"),
        }
    }
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

struct Handler {
    shared: Arc<Shared>,
    is_readonly: bool,
}

#[async_trait]
impl SessionHandler for Handler {
    type Error = RaftError;

    async fn call(&mut self, command: RedisCommand, args: &[Bytes]) -> RaftResult<RedisResult> {
        match command {
            RedisCommand::Write(command) => {
                self.shared
                    .raft
                    .write(StoreCommand::SingleWrite((command, args.to_vec())))
                    .await
            }
            RedisCommand::Read(command) => {
                if !self.is_readonly {
                    self.shared.raft.read().await?;
                }
                Ok(command.call(&self.shared.store, args))
            }
            RedisCommand::Stateless(command) => Ok(command.call(args)),
            RedisCommand::System(command) => self.handle_system_command(command, args).await,
        }
    }

    async fn exec(&mut self, commands: Vec<(RedisCommand, Vec<Bytes>)>) -> RaftResult<RedisResult> {
        self.shared.raft.write(StoreCommand::Exec(commands)).await
    }
}

impl Handler {
    fn new(shared: Arc<Shared>) -> Self {
        Self {
            shared,
            is_readonly: false,
        }
    }

    async fn handle_system_command(
        &mut self,
        command: SystemCommand,
        args: &[Bytes],
    ) -> RaftResult<RedisResult> {
        match command {
            SystemCommand::Cluster => self.shared.handle_cluster(args).await,
            SystemCommand::Info => Ok(self.shared.handle_info(args)),
            SystemCommand::ReadOnly => Ok(self.handle_readonly(args)),
            SystemCommand::ReadWrite => Ok(self.handle_readwrite(args)),
            SystemCommand::Select => Ok(handle_select(args)),
            SystemCommand::Shutdown => Ok(handle_shutdown(args)),
        }
    }

    fn handle_readonly(&mut self, args: &[Bytes]) -> RedisResult {
        if args.is_empty() {
            self.is_readonly = true;
            Ok(Value::ok())
        } else {
            Err(ResponseError::WrongArity.into())
        }
    }

    fn handle_readwrite(&mut self, args: &[Bytes]) -> RedisResult {
        if args.is_empty() {
            self.is_readonly = false;
            Ok(Value::ok())
        } else {
            Err(ResponseError::WrongArity.into())
        }
    }
}

impl Shared {
    async fn handle_cluster(&self, args: &[Bytes]) -> RaftResult<RedisResult> {
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

    fn handle_info(&self, args: &[Bytes]) -> RedisResult {
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

fn handle_select(args: &[Bytes]) -> RedisResult {
    let [index] = args else {
        return Err(ResponseError::WrongArity.into());
    };
    if index.to_i32()? == 0 {
        Ok(Value::ok())
    } else {
        Err(ResponseError::Other("SELECT is not allowed in cluster mode").into())
    }
}

fn handle_shutdown(_: &[Bytes]) -> RedisResult {
    // TODO: gracefully shutdown
    std::process::exit(0)
}
