use crate::{connection::RedisConnection, Shared};
use bytes::Bytes;
use std::{
    io::Write,
    time::{Duration, SystemTime},
};
use zakros_redis::RedisResult;

const SERVER: u8 = 0x1;
const CLIENTS: u8 = 0x2;
const CLUSTER: u8 = 0x4;
const ALL: u8 = u8::MAX;

pub fn info(conn: &RedisConnection, args: &[Bytes]) -> RedisResult {
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
    Ok(generate_info_str(&conn.shared, sections).unwrap().into())
}

fn generate_info_str(shared: &Shared, sections: u8) -> std::io::Result<Bytes> {
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
    Ok(out.into())
}
