# zakros

[![build](https://github.com/mosmeh/zakros/workflows/build/badge.svg)](https://github.com/mosmeh/zakros/actions)

Redis-compatible in-memory database replicated with Raft consensus algorithm

## Usage

With no options, zakros starts as a persistent single node database:

```sh
$ cargo run

# You can use any Redis client
$ redis-cli
127.0.0.1:6379> set foo bar
OK
```

Configuration can be provided with the same style as Redis:

```sh
cargo run -- --port 1234
cargo run -- zakros.conf
```

See [the example configuration file](zakros.conf) for available options.

## Commands

See [the Redis command compatibility table](commands.md).

## Recipes

### Use as a highly available persistent database

```sh
# In separate terminals:

$ cargo run -- --node-id 0 --port 6379 \
    --cluster-addrs '127.0.0.1:6379 127.0.0.1:6380 127.0.0.1:6381'

$ cargo run -- --node-id 1 --port 6380 \
    --cluster-addrs '127.0.0.1:6379 127.0.0.1:6380 127.0.0.1:6381'

$ cargo run -- --node-id 2 --port 6381 \
    --cluster-addrs '127.0.0.1:6379 127.0.0.1:6380 127.0.0.1:6381'

# With the cluster mode (-c), you can connect to any node and
# you will be redirected to the Raft leader node
$ redis-cli -c -p 6379
127.0.0.1:6379> sadd foo bar
-> Redirected to slot [0] located at 127.0.0.1:6381
(integer) 1
```

### Use as a single node volatile database

```sh
$ cargo run -- --raft-enabled no

$ redis-cli
127.0.0.1:6379> set foo bar
OK
```

Zakros relies on Raft log for persistence, so the database is not persisted when
Raft is disabled:

```sh
$ cargo run -- --raft-enabled no

$ redis-cli
127.0.0.1:6379> get foo
(nil)
```

## Inspirations

- [RedisRaft](https://github.com/RedisLabs/redisraft):
Redis module that implements Raft.
You can say zakros is basically a reimplementation of RedisRaft in Rust :)
- [Dragonfly](https://github.com/dragonflydb/dragonfly):
multi-threaded Redis-compatible database
- [mini-redis](https://github.com/tokio-rs/mini-redis):
implementation of Redis-ish server using Tokio
- [async-raft](https://github.com/async-raft/async-raft):
asynchronous Raft library using Tokio.
Our Raft library's API is highly influenced by async-raft's design.
