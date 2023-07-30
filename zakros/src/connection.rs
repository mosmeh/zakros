use crate::{store::StoreCommand, RaftResult, SharedState};
use async_trait::async_trait;
use bstr::ByteSlice;
use futures::{SinkExt, StreamExt};
use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::{net::TcpStream, sync::TryAcquireError};
use tokio_util::codec::{FramedRead, FramedWrite};
use zakros_raft::{Error as RaftError, NodeId};
use zakros_redis::{
    command::{ConnectionCommand, RedisCommand},
    error::{ConnectionError, Error as RedisError, ResponseError},
    resp::{QueryDecoder, ResponseEncoder, Value},
    session::{RedisSession, SessionHandler},
    BytesExt, RedisResult,
};

pub struct RedisConnection {
    shared: Arc<SharedState>,
    session: RedisSession<Handler>,
}

impl RedisConnection {
    pub(crate) fn new(shared: Arc<SharedState>) -> Self {
        Self {
            shared: shared.clone(),
            session: RedisSession::new(Handler::new(shared)),
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

        let (reader, writer) = conn.into_split();
        let mut reader = FramedRead::new(reader, QueryDecoder);
        let mut writer = FramedWrite::new(writer, ResponseEncoder);
        while let Some(decoded) = reader.next().await {
            match decoded {
                Ok(strings) => {
                    tracing::trace!("received {:?}", DebugQuery(&strings));
                    let Some((command, args)) = strings.split_first() else {
                        continue;
                    };
                    let value = self.call(command, args).await;
                    writer.send(value).await?;
                }
                Err(ConnectionError::Io(err)) => return Err(err),
                Err(ConnectionError::Protocol(err)) => {
                    return writer
                        .send(Err(ResponseError::ProtocolError(err).into()))
                        .await;
                }
            }
        }
        Ok(())
    }

    async fn call(&mut self, command: &[u8], args: &[Vec<u8>]) -> RedisResult {
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

struct DebugQuery<'a>(&'a [Vec<u8>]);

impl std::fmt::Debug for DebugQuery<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.is_empty() {
            return f.write_str("(empty)");
        }
        for s in self.0 {
            if s.len() > 30 {
                write!(f, "{:?}... ", s[..30].as_bstr())?;
            } else {
                write!(f, "{:?} ", s.as_bstr())?;
            }
        }
        Ok(())
    }
}

struct Handler {
    shared: Arc<SharedState>,
    is_readonly: bool,
}

impl Handler {
    fn new(shared: Arc<SharedState>) -> Self {
        Self {
            shared,
            is_readonly: false,
        }
    }

    async fn handle_connection_command(
        &mut self,
        command: ConnectionCommand,
        args: &[Vec<u8>],
    ) -> RaftResult<RedisResult> {
        match command {
            ConnectionCommand::Cluster => self.handle_cluster(args).await,
            ConnectionCommand::Info => Ok(self.handle_info(args)),
            ConnectionCommand::ReadOnly => Ok(self.handle_readonly(args)),
            ConnectionCommand::ReadWrite => Ok(self.handle_readwrite(args)),
            ConnectionCommand::Select => Ok(self.handle_select(args)),
            ConnectionCommand::Shutdown => Ok(self.handle_shutdown(args)),
        }
    }

    async fn handle_cluster(&self, args: &[Vec<u8>]) -> RaftResult<RedisResult> {
        fn format_node_id(node_id: NodeId) -> RedisResult {
            Ok(format!("{:0>40x}", Into::<u64>::into(node_id))
                .into_bytes()
                .into())
        }

        fn format_node(node_id: NodeId, addr: SocketAddr) -> RedisResult {
            Ok(Value::Array(vec![
                Ok(addr.ip().to_string().into_bytes().into()),
                Ok((addr.port() as i64).into()),
                format_node_id(node_id),
            ]))
        }

        let [subcommand, _args @ ..] = args else {
            return Ok(Err(ResponseError::WrongArity.into()));
        };
        match subcommand.to_ascii_uppercase().as_slice() {
            b"MYID" => Ok(format_node_id(NodeId::from(self.shared.opts.id))),
            b"SLOTS" => {
                const CLUSTER_SLOTS: i64 = 16384;
                let leader_id = self.shared.raft.status().await?.leader_id;
                let Some(leader_id) = leader_id else {
                    return Err(RaftError::NotLeader { leader_id: None });
                };
                let mut responses = vec![Ok(0.into()), Ok((CLUSTER_SLOTS - 1).into())];
                let leader_index = Into::<u64>::into(leader_id) as usize;
                let addrs = &self.shared.opts.cluster_addrs;
                responses.reserve(addrs.len());
                let addr = addrs[leader_index];
                responses.push(format_node(leader_id, addr));
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

    fn handle_info(&self, args: &[Vec<u8>]) -> RedisResult {
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
        Ok(generate_info_str(sections, &self.shared).unwrap().into())
    }

    fn handle_readonly(&mut self, args: &[Vec<u8>]) -> RedisResult {
        if args.is_empty() {
            self.is_readonly = true;
            Ok(Value::ok())
        } else {
            Err(ResponseError::WrongArity.into())
        }
    }

    fn handle_readwrite(&mut self, args: &[Vec<u8>]) -> RedisResult {
        if args.is_empty() {
            self.is_readonly = false;
            Ok(Value::ok())
        } else {
            Err(ResponseError::WrongArity.into())
        }
    }

    fn handle_select(&self, args: &[Vec<u8>]) -> RedisResult {
        let [index] = args else {
            return Err(ResponseError::WrongArity.into());
        };
        if index.to_i32()? == 0 {
            Ok(Value::ok())
        } else {
            Err(ResponseError::Other("SELECT is not allowed in cluster mode").into())
        }
    }

    fn handle_shutdown(&self, _: &[Vec<u8>]) -> RedisResult {
        // TODO: gracefully shutdown
        std::process::exit(0)
    }
}

const SERVER: u8 = 0x1;
const CLIENTS: u8 = 0x2;
const CLUSTER: u8 = 0x4;
const ALL: u8 = SERVER | CLIENTS | CLUSTER;

fn generate_info_str(sections: u8, shared: &SharedState) -> std::io::Result<Vec<u8>> {
    use std::io::Write;

    let mut out = Vec::new();
    let mut is_first = true;
    if sections & SERVER != 0 {
        is_first = false;
        out.write_all(b"# Server\r\n")?;
        write!(out, "tcp_port:{}\r\n", shared.opts.bind_addr.port())?;
        let now = SystemTime::now();
        let since_epoch = now
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_micros();
        let uptime = now
            .duration_since(shared.started_at)
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
            shared.opts.max_num_clients - shared.conn_limit.available_permits()
        )?;
        write!(out, "maxclients:{}\r\n", shared.opts.max_num_clients)?;
    }
    if sections & CLUSTER != 0 {
        if !is_first {
            out.write_all(b"\r\n")?;
        }
        out.write_all(b"# Cluster\r\n")?;
        write!(out, "cluster_enabled:1\r\n")?;
    }
    Ok(out)
}

#[async_trait]
impl SessionHandler for Handler {
    type Error = RaftError;

    async fn call(&mut self, command: RedisCommand, args: &[Vec<u8>]) -> RaftResult<RedisResult> {
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
                Ok(command.call(self.shared.store.as_ref(), args))
            }
            RedisCommand::Stateless(command) => Ok(command.call(args)),
            RedisCommand::Connection(command) => {
                self.handle_connection_command(command, args).await
            }
        }
    }

    async fn exec(
        &mut self,
        commands: Vec<(RedisCommand, Vec<Vec<u8>>)>,
    ) -> RaftResult<RedisResult> {
        self.shared.raft.write(StoreCommand::Exec(commands)).await
    }
}
