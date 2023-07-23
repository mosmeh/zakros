use super::{Arity, CommandSpec, ConnectionCommandHandler};
use crate::{command, connection::RedisConnection, error::Error, resp::RedisValue, RedisResult};
use std::net::SocketAddr;
use zakros_raft::{async_trait::async_trait, NodeId};

impl CommandSpec for command::Cluster {
    const NAME: &'static str = "CLUSTER";
    const ARITY: Arity = Arity::AtLeast(1);
}

#[async_trait]
impl ConnectionCommandHandler for command::Cluster {
    async fn call(conn: &mut RedisConnection, args: &[Vec<u8>]) -> RedisResult {
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
            return Err(Error::WrongArity);
        };
        match subcommand.to_ascii_uppercase().as_slice() {
            b"MYID" => Ok(format_node_id(NodeId::from(conn.shared.args.id))),
            b"SLOTS" => {
                const CLUSTER_SLOTS: i64 = 16384;
                let leader_id = conn.shared.raft.status().await?.leader_id;
                let Some(leader_id) = leader_id else {
                    return Err(Error::Raft(zakros_raft::Error::NotLeader {
                        leader_id: None,
                    }));
                };
                let mut response = vec![0.into(), (CLUSTER_SLOTS - 1).into()];
                let leader_index = Into::<u64>::into(leader_id) as usize;
                let addrs = &conn.shared.args.cluster_addrs;
                response.reserve(addrs.len());
                let addr = addrs[leader_index];
                response.push(format_node(leader_id, addr));
                for (i, addr) in addrs.iter().enumerate() {
                    if i != leader_index {
                        response.push(format_node(NodeId::from(i as u64), *addr));
                    }
                }
                Ok(RedisValue::Array(vec![RedisValue::Array(response)]))
            }
            b"INFO" | b"NODES" => todo!(),
            _ => Err(Error::UnknownSubcommand),
        }
    }
}

impl CommandSpec for command::ReadOnly {
    const NAME: &'static str = "READONLY";
    const ARITY: Arity = Arity::Fixed(0);
}

#[async_trait]
impl ConnectionCommandHandler for command::ReadOnly {
    async fn call(conn: &mut RedisConnection, args: &[Vec<u8>]) -> RedisResult {
        if args.is_empty() {
            conn.is_readonly = true;
            Ok(RedisValue::ok())
        } else {
            Err(Error::WrongArity)
        }
    }
}

impl CommandSpec for command::ReadWrite {
    const NAME: &'static str = "READWRITE";
    const ARITY: Arity = Arity::Fixed(0);
}

#[async_trait]
impl ConnectionCommandHandler for command::ReadWrite {
    async fn call(conn: &mut RedisConnection, args: &[Vec<u8>]) -> RedisResult {
        if args.is_empty() {
            conn.is_readonly = false;
            Ok(RedisValue::ok())
        } else {
            Err(Error::WrongArity)
        }
    }
}
