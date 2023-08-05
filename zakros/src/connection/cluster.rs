use super::RedisConnection;
use crate::{RaftResult, Shared};
use bytes::Bytes;
use std::net::SocketAddr;
use zakros_raft::{Error as RaftError, NodeId};
use zakros_redis::{error::ResponseError, resp::Value, RedisResult};

impl RedisConnection {
    pub fn readonly(&mut self, args: &[Bytes]) -> RedisResult {
        if args.is_empty() {
            self.is_readonly = true;
            Ok(Value::ok())
        } else {
            Err(ResponseError::WrongArity.into())
        }
    }

    pub fn readwrite(&mut self, args: &[Bytes]) -> RedisResult {
        if args.is_empty() {
            self.is_readonly = false;
            Ok(Value::ok())
        } else {
            Err(ResponseError::WrongArity.into())
        }
    }
}

impl Shared {
    pub async fn cluster(&self, args: &[Bytes]) -> RaftResult<RedisResult> {
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
}
