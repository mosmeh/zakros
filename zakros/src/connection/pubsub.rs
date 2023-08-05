use super::{Error, RedisConnection};
use crate::Shared;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use zakros_raft::NodeId;
use zakros_redis::{
    error::{Error as RedisError, ResponseError},
    pubsub::PubSubMessage,
    resp::Value,
    RedisResult,
};

impl RedisConnection {
    pub async fn psubscribe(&mut self, args: &[Bytes]) -> Result<(), Error> {
        if args.is_empty() {
            return Err(RedisError::from(ResponseError::WrongArity).into());
        }
        for pattern in args {
            self.subscriber.subscribe_pattern(pattern.clone());
            let response = Value::Array(vec![
                Ok(Bytes::from(b"psubscribe".as_slice()).into()),
                Ok(pattern.clone().into()),
                Ok((self.subscriber.num_subscriptions() as i64).into()),
            ]);
            self.framed.feed(Ok(response)).await?;
        }
        self.framed.flush().await?;
        while let Some(message) = self.subscriber.next().await.transpose()? {
            self.framed
                .send(Ok(Value::Array(vec![
                    Ok(Bytes::from(b"pmessage".as_slice()).into()),
                    Ok(message.channel.into()),
                    Ok(message.payload.into()),
                ])))
                .await?;
        }
        Ok(())
    }

    pub async fn punsubscribe(&mut self, args: &[Bytes]) -> Result<(), Error> {
        for pattern in args {
            self.subscriber.unsubscribe_pattern(pattern.clone());
            let response = Value::Array(vec![
                Ok(Bytes::from(b"punsubscribe".as_slice()).into()),
                Ok(pattern.clone().into()),
                Ok((self.subscriber.num_subscriptions() as i64).into()),
            ]);
            self.framed.feed(Ok(response)).await?;
        }
        self.framed.flush().await?;
        Ok(())
    }

    pub async fn subscribe(&mut self, args: &[Bytes]) -> Result<(), Error> {
        if args.is_empty() {
            return Err(RedisError::from(ResponseError::WrongArity).into());
        }
        for channel in args {
            self.subscriber.subscribe_channel(channel.clone());
            let response = Value::Array(vec![
                Ok(Bytes::from(b"subscribe".as_slice()).into()),
                Ok(channel.clone().into()),
                Ok((self.subscriber.num_subscriptions() as i64).into()),
            ]);
            self.framed.feed(Ok(response)).await?;
        }
        self.framed.flush().await?;
        while let Some(message) = self.subscriber.next().await.transpose()? {
            self.framed
                .send(Ok(Value::Array(vec![
                    Ok(Bytes::from(b"message".as_slice()).into()),
                    Ok(message.channel.into()),
                    Ok(message.payload.into()),
                ])))
                .await?;
        }
        Ok(())
    }

    pub async fn unsubscribe(&mut self, args: &[Bytes]) -> Result<(), Error> {
        for channel in args {
            self.subscriber.unsubscribe_channel(channel.clone());
            let response = Value::Array(vec![
                Ok(Bytes::from(b"unsubscribe".as_slice()).into()),
                Ok(channel.clone().into()),
                Ok((self.subscriber.num_subscriptions() as i64).into()),
            ]);
            self.framed.feed(Ok(response)).await?;
        }
        self.framed.flush().await?;
        Ok(())
    }
}

impl Shared {
    pub async fn publish(&self, args: &[Bytes]) -> RedisResult {
        let [channel, message] = args else {
            return Err(ResponseError::WrongArity.into());
        };
        let message = PubSubMessage {
            channel: channel.clone(),
            payload: message.clone(),
        };
        let num_receivers = self.publisher.publish(message.clone());
        for i in 0..self.opts.cluster_addrs.len() as u64 {
            if i != self.opts.id {
                let rpc_handler = self.rpc_handler.clone();
                let message = message.clone();
                tokio::spawn(async move { rpc_handler.publish(NodeId::from(i), message).await });
            }
        }
        Ok((num_receivers as i64).into())
    }

    pub fn pubsub(&self, args: &[Bytes]) -> RedisResult {
        let [subcommand, args @ ..] = args else {
            return Err(ResponseError::WrongArity.into());
        };
        match subcommand.to_ascii_uppercase().as_slice() {
            b"CHANNELS" => {
                let channels = match args {
                    [] => self.publisher.active_channels(),
                    [pattern] => self.publisher.active_channels_matching_pattern(pattern),
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
                    Ok((self.publisher.num_patterns() as i64).into())
                } else {
                    Err(ResponseError::WrongArity.into())
                }
            }
            b"NUMSUB" => {
                let mut responses = Vec::with_capacity(args.len() * 2);
                for channel in args {
                    responses.push(Ok(channel.clone().into()));
                    responses.push(Ok((self.publisher.num_subscribers(channel) as i64).into()));
                }
                Ok(responses.into())
            }
            _ => Err(ResponseError::UnknownSubcommand(
                String::from_utf8_lossy(subcommand).into_owned(),
            )
            .into()),
        }
    }
}
