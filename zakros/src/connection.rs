use crate::{store::Command, RaftResult, SharedState};
use async_trait::async_trait;
use bstr::ByteSlice;
use futures::{SinkExt, StreamExt};
use std::{net::SocketAddr, sync::Arc};
use tokio::net::TcpStream;
use tokio_util::codec::{FramedRead, FramedWrite};
use zakros_raft::NodeId;
use zakros_redis::{
    command::{ConnectionCommand, RedisCommand},
    error::{ConnectionError, Error},
    resp::{QueryDecoder, RedisValue, ResponseEncoder},
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

    pub async fn serve(mut self, conn: TcpStream) -> std::io::Result<()> {
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
                Err(ConnectionError::Protocol) => {
                    return writer.send(Err(Error::ProtocolError)).await;
                }
            }
        }
        Ok(())
    }

    async fn call(&mut self, command: &[u8], args: &[Vec<u8>]) -> RedisResult {
        let result = self.session.call(command, args).await;
        match result {
            Ok(value) => value,
            Err(zakros_raft::Error::NotLeader { leader_id: None }) => {
                Err(Error::Custom("CLUSTERDOWN No leader".to_owned()))
            }
            Err(zakros_raft::Error::NotLeader {
                leader_id: Some(leader_id),
            }) => {
                let addr = &self.shared.args.cluster_addrs[Into::<u64>::into(leader_id) as usize];
                Err(Error::Custom(format!(
                    "MOVED 0 {}:{}",
                    addr.ip(),
                    addr.port()
                )))
            }
            Err(zakros_raft::Error::Shutdown) => panic!("Raft server is shut down"),
        }
    }
}

struct DebugQuery<'a>(&'a [Vec<u8>]);

impl std::fmt::Debug for DebugQuery<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for s in self.0 {
            if s.len() > 30 {
                write!(f, "\"{}...\" ", s[..30].as_bstr())?;
            } else {
                write!(f, "\"{}\" ", s.as_bstr())?;
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
            ConnectionCommand::ReadOnly => Ok(self.handle_readonly(args)),
            ConnectionCommand::ReadWrite => Ok(self.handle_readwrite(args)),
            ConnectionCommand::Select => Ok(self.handle_select(args)),
        }
    }

    async fn handle_cluster(&self, args: &[Vec<u8>]) -> RaftResult<RedisResult> {
        fn format_node_id(node_id: NodeId) -> RedisValue {
            format!("{:0>40x}", Into::<u64>::into(node_id))
                .into_bytes()
                .into()
        }

        fn format_node(node_id: NodeId, addr: SocketAddr) -> RedisValue {
            RedisValue::Array(vec![
                addr.ip().to_string().into_bytes().into(),
                (addr.port() as i64).into(),
                format_node_id(node_id),
            ])
        }

        let [subcommand, _args @ ..] = args else {
            return Ok(Err(Error::WrongArity));
        };
        match subcommand.to_ascii_uppercase().as_slice() {
            b"MYID" => Ok(Ok(format_node_id(NodeId::from(self.shared.args.id)))),
            b"SLOTS" => {
                const CLUSTER_SLOTS: i64 = 16384;
                let leader_id = self.shared.raft.status().await?.leader_id;
                let Some(leader_id) = leader_id else {
                    return Err(zakros_raft::Error::NotLeader { leader_id: None });
                };
                let mut response = vec![0.into(), (CLUSTER_SLOTS - 1).into()];
                let leader_index = Into::<u64>::into(leader_id) as usize;
                let addrs = &self.shared.args.cluster_addrs;
                response.reserve(addrs.len());
                let addr = addrs[leader_index];
                response.push(format_node(leader_id, addr));
                for (i, addr) in addrs.iter().enumerate() {
                    if i != leader_index {
                        response.push(format_node(NodeId::from(i as u64), *addr));
                    }
                }
                Ok(Ok(RedisValue::Array(vec![RedisValue::Array(response)])))
            }
            b"INFO" | b"NODES" => todo!(),
            _ => Ok(Err(Error::UnknownSubcommand)),
        }
    }

    fn handle_readonly(&mut self, args: &[Vec<u8>]) -> RedisResult {
        if args.is_empty() {
            self.is_readonly = true;
            Ok(RedisValue::ok())
        } else {
            Err(Error::WrongArity)
        }
    }

    fn handle_readwrite(&mut self, args: &[Vec<u8>]) -> RedisResult {
        if args.is_empty() {
            self.is_readonly = false;
            Ok(RedisValue::ok())
        } else {
            Err(Error::WrongArity)
        }
    }

    fn handle_select(&mut self, args: &[Vec<u8>]) -> RedisResult {
        let [index] = args else {
            return Err(Error::WrongArity);
        };
        if index.to_i32()? == 0 {
            Ok(RedisValue::ok())
        } else {
            Err(Error::Custom(
                "ERR SELECT is not allowed in cluster mode".to_owned(),
            ))
        }
    }
}

#[async_trait]
impl SessionHandler for Handler {
    type Error = zakros_raft::Error;

    async fn call(&mut self, command: RedisCommand, args: &[Vec<u8>]) -> RaftResult<RedisResult> {
        match command {
            RedisCommand::Write(command) => {
                self.shared
                    .raft
                    .write(Command::SingleWrite {
                        command,
                        args: args.to_vec(),
                    })
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
        self.shared.raft.write(Command::Exec(commands)).await
    }
}
