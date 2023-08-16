use super::CommandError;
use crate::connection::RedisConnection;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use zakros_raft::NodeId;
use zakros_redis::{pubsub::PubSubMessage, resp::Value, RedisError, RedisResult, ResponseError};

pub async fn psubscribe(conn: &mut RedisConnection, args: &[Bytes]) -> Result<(), CommandError> {
    if args.is_empty() {
        return Err(RedisError::from(ResponseError::WrongArity).into());
    }
    for pattern in args {
        conn.subscriber.subscribe_to_pattern(pattern.clone());
        let response = Value::Array(vec![
            Ok(Bytes::from(b"psubscribe".as_slice()).into()),
            Ok(pattern.clone().into()),
            Ok((conn.subscriber.num_subscriptions() as i64).into()),
        ]);
        conn.framed.feed(Ok(response)).await?;
    }
    conn.framed.flush().await?;
    while let Some(message) = conn.subscriber.next().await.transpose()? {
        conn.framed
            .send(Ok(Value::Array(vec![
                Ok(Bytes::from(b"pmessage".as_slice()).into()),
                Ok(message.channel.into()),
                Ok(message.payload.into()),
            ])))
            .await?;
    }
    Ok(())
}

pub async fn publish(conn: &RedisConnection, args: &[Bytes]) -> Result<Value, CommandError> {
    let [channel, message] = args else {
        return Err(RedisError::from(ResponseError::WrongArity).into());
    };

    // Make sure we can reach majority of nodes
    conn.shared.raft.read().await?;

    let message = PubSubMessage {
        channel: channel.clone(),
        payload: message.clone(),
    };
    let num_receivers = conn.shared.publisher.publish(message.clone());
    for i in 0..conn.shared.opts.cluster_addrs.len() as u64 {
        if i != conn.shared.opts.id {
            let rpc_handler = conn.shared.rpc_client.clone();
            let message = message.clone();
            tokio::spawn(async move { rpc_handler.publish(NodeId::from(i), message).await });
        }
    }
    Ok((num_receivers as i64).into())
}

pub fn pubsub(conn: &RedisConnection, args: &[Bytes]) -> RedisResult {
    let [subcommand, args @ ..] = args else {
        return Err(ResponseError::WrongArity.into());
    };
    let shared = &conn.shared;
    match subcommand.to_ascii_uppercase().as_slice() {
        b"HELP" => Ok(Value::Array(
            [
                "PUBSUB <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                "CHANNELS [<pattern>]",
                "    Return the currently active channels matching a <pattern> (default: '*').",
                "NUMPAT",
                "    Return number of subscriptions to patterns.",
                "NUMSUB [<channel> ...]",
                "    Return the number of subscribers for the specified channels, excluding",
                "    pattern subscriptions(default: no channels).",
                "HELP",
                "    Print this help.",
            ]
            .iter()
            .map(|s| Ok((*s).into()))
            .collect(),
        )),
        b"CHANNELS" => {
            let channels = match args {
                [] => shared.publisher.active_channels(),
                [pattern] => shared.publisher.active_channels_matching_pattern(pattern),
                _ => return Err(ResponseError::WrongArity.into()),
            };
            Ok(Value::Array(
                channels
                    .into_iter()
                    .map(|channel| Ok(channel.into()))
                    .collect(),
            ))
        }
        b"NUMPAT" => {
            if args.is_empty() {
                Ok((shared.publisher.num_patterns() as i64).into())
            } else {
                Err(ResponseError::WrongArity.into())
            }
        }
        b"NUMSUB" => {
            let mut responses = Vec::with_capacity(args.len() * 2);
            for channel in args {
                responses.push(Ok(channel.clone().into()));
                responses.push(Ok((shared.publisher.num_subscribers(channel) as i64).into()));
            }
            Ok(responses.into())
        }
        _ => Err(ResponseError::UnknownSubcommand.into()),
    }
}

pub async fn punsubscribe(conn: &mut RedisConnection, args: &[Bytes]) -> Result<(), CommandError> {
    for pattern in args {
        conn.subscriber.unsubscribe_from_pattern(pattern.clone());
        let response = Value::Array(vec![
            Ok(Bytes::from(b"punsubscribe".as_slice()).into()),
            Ok(pattern.clone().into()),
            Ok((conn.subscriber.num_subscriptions() as i64).into()),
        ]);
        conn.framed.feed(Ok(response)).await?;
    }
    conn.framed.flush().await?;
    Ok(())
}

pub async fn subscribe(conn: &mut RedisConnection, args: &[Bytes]) -> Result<(), CommandError> {
    if args.is_empty() {
        return Err(RedisError::from(ResponseError::WrongArity).into());
    }
    for channel in args {
        conn.subscriber.subscribe_to_channel(channel.clone());
        let response = Value::Array(vec![
            Ok(Bytes::from(b"subscribe".as_slice()).into()),
            Ok(channel.clone().into()),
            Ok((conn.subscriber.num_subscriptions() as i64).into()),
        ]);
        conn.framed.feed(Ok(response)).await?;
    }
    conn.framed.flush().await?;
    while let Some(message) = conn.subscriber.next().await.transpose()? {
        conn.framed
            .send(Ok(Value::Array(vec![
                Ok(Bytes::from(b"message".as_slice()).into()),
                Ok(message.channel.into()),
                Ok(message.payload.into()),
            ])))
            .await?;
    }
    Ok(())
}

pub async fn unsubscribe(conn: &mut RedisConnection, args: &[Bytes]) -> Result<(), CommandError> {
    for channel in args {
        conn.subscriber.unsubscribe_from_channel(channel.clone());
        let response = Value::Array(vec![
            Ok(Bytes::from(b"unsubscribe".as_slice()).into()),
            Ok(channel.clone().into()),
            Ok((conn.subscriber.num_subscriptions() as i64).into()),
        ]);
        conn.framed.feed(Ok(response)).await?;
    }
    conn.framed.flush().await?;
    Ok(())
}
