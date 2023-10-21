use super::CommandError;
use crate::connection::RedisConnection;
use bytes::Bytes;
use std::net::SocketAddr;
use zakros_raft::{NodeId, RaftError};
use zakros_redis::{resp::Value, RedisError, RedisResult, ResponseError};

pub async fn cluster(conn: &RedisConnection, args: &[Bytes]) -> Result<Value, CommandError> {
    let [subcommand, _args @ ..] = args else {
        return Err(RedisError::from(ResponseError::WrongArity).into());
    };
    let Some(raft) = &conn.shared.raft else {
        return Err(RedisError::from(ResponseError::ClusterDisabled).into());
    };
    match subcommand.to_ascii_uppercase().as_slice() {
        b"HELP" => Ok(Value::Array(
            [
                "CLUSTER <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                "MYID",
                "    Return the node id.",
                "SLOTS",
                "    Return information about slots range mappings. Each range is made of:",
                "    start, end, master and replicas IP addresses, ports and ids",
                "HELP",
                "    Print this help.",
            ]
            .iter()
            .map(|s| Ok((*s).into()))
            .collect(),
        )),
        b"MYID" => Ok(format_node_id(NodeId::from(conn.shared.config.node_id))),
        b"SLOTS" => {
            const CLUSTER_SLOTS: i64 = 16384;
            let leader_id = raft
                .status()
                .await?
                .leader_id
                .ok_or(RaftError::NotLeader { leader_id: None })?;
            let leader_index = Into::<u64>::into(leader_id) as usize;
            let addrs = &conn.shared.config.cluster_addrs;
            let mut responses = vec![Ok(0.into()), Ok((CLUSTER_SLOTS - 1).into())];
            responses.reserve(addrs.len());
            responses.push(Ok(format_node(leader_id, addrs[leader_index])));
            for (i, addr) in addrs.iter().enumerate() {
                if i != leader_index {
                    responses.push(Ok(format_node(NodeId::from(i as u64), *addr)));
                }
            }
            Ok(Value::Array(vec![Ok(Value::Array(responses))]))
        }
        _ => Err(RedisError::from(ResponseError::UnknownSubcommand).into()),
    }
}

fn format_node_id(node_id: NodeId) -> Value {
    Bytes::from(format!("{:0>40x}", Into::<u64>::into(node_id)).into_bytes()).into()
}

fn format_node(node_id: NodeId, addr: SocketAddr) -> Value {
    Value::Array(vec![
        Ok(Bytes::from(addr.ip().to_string().into_bytes()).into()),
        Ok((addr.port() as i64).into()),
        Ok(format_node_id(node_id)),
    ])
}

pub fn readonly(conn: &mut RedisConnection, args: &[Bytes]) -> RedisResult {
    if !args.is_empty() {
        return Err(ResponseError::WrongArity.into());
    }
    if conn.shared.raft.is_none() {
        return Err(ResponseError::ClusterDisabled.into());
    }
    conn.is_readonly = true;
    Ok(Value::ok())
}

pub fn readwrite(conn: &mut RedisConnection, args: &[Bytes]) -> RedisResult {
    if !args.is_empty() {
        return Err(ResponseError::WrongArity.into());
    }
    if conn.shared.raft.is_none() {
        return Err(ResponseError::ClusterDisabled.into());
    }
    conn.is_readonly = false;
    Ok(Value::ok())
}
