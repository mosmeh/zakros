use super::ConnectionCommandHandler;
use crate::{command, connection::RedisConnection, error::Error, resp::RedisValue, RedisResult};
use std::net::SocketAddr;
use zakros_raft::async_trait::async_trait;

#[async_trait]
impl ConnectionCommandHandler for command::Cluster {
    async fn call(conn: &mut RedisConnection, args: &[Vec<u8>]) -> RedisResult {
        fn addr_to_value(addr: SocketAddr) -> RedisValue {
            vec![
                RedisValue::BulkString(addr.ip().to_string().into_bytes()),
                (addr.port() as i64).into(),
            ]
            .into()
        }

        let &[subcommand, _args @ ..] = &args else {
            return Err(Error::WrongArity);
        };
        match subcommand.to_ascii_uppercase().as_slice() {
            b"MYID" => Ok(conn.shared.args.id.to_string().into_bytes().into()),
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
                response.push(addr_to_value(addr));
                for (i, addr) in addrs.iter().enumerate() {
                    if i != leader_index {
                        response.push(addr_to_value(*addr));
                    }
                }
                Ok(RedisValue::Array(vec![RedisValue::Array(response)]))
            }
            b"INFO" | b"NODES" => todo!(),
            _ => Err(Error::UnknownSubcommand),
        }
    }
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
