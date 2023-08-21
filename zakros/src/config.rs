use bstr::ByteSlice;
use serde::Deserialize;
use std::{
    net::{IpAddr, SocketAddr},
    path::PathBuf,
};

#[derive(Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct Config {
    #[serde(default = "defaults::bind")]
    pub bind: IpAddr,

    #[serde(default = "defaults::port")]
    pub port: u16,

    #[serde(rename = "maxclients", default = "defaults::max_clients")]
    pub max_clients: usize,

    #[serde(default = "defaults::dir")]
    pub dir: PathBuf,

    #[serde(default = "defaults::cluster_addrs")]
    pub cluster_addrs: Vec<SocketAddr>,

    #[serde(default = "defaults::raft_enabled")]
    pub raft_enabled: bool,

    #[serde(default = "defaults::raft_node_id")]
    pub raft_node_id: u64,

    #[serde(default = "defaults::raft_storage")]
    pub raft_storage: RaftStorageKind,
}

#[derive(Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RaftStorageKind {
    Disk,
    Memory,
}

mod defaults {
    use super::RaftStorageKind;
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        path::PathBuf,
    };

    pub const fn bind() -> IpAddr {
        IpAddr::V4(Ipv4Addr::LOCALHOST)
    }

    pub const fn port() -> u16 {
        6379
    }

    pub const fn max_clients() -> usize {
        10000
    }

    pub fn dir() -> PathBuf {
        "./data".into()
    }

    pub const fn cluster_addrs() -> Vec<SocketAddr> {
        Vec::new()
    }

    pub const fn raft_enabled() -> bool {
        true
    }

    pub const fn raft_node_id() -> u64 {
        0
    }

    pub const fn raft_storage() -> RaftStorageKind {
        RaftStorageKind::Disk
    }
}

impl Config {
    pub fn from_args() -> anyhow::Result<Self> {
        let mut args = std::env::args_os().skip(1).peekable();
        let mut bytes = match args.peek() {
            Some(arg)
                if <[u8]>::from_os_str(arg)
                    .map(|arg| !arg.starts_with(b"-"))
                    .unwrap_or(false) =>
            {
                let content = std::fs::read(arg)?;
                args.next().unwrap();
                content
            }
            _ => Vec::new(),
        };
        for arg in args {
            let mut arg = <[u8]>::from_os_str(&arg).unwrap();
            if let Some(dashes_removed) = arg.strip_prefix(b"--") {
                bytes.push(b'\n');
                arg = dashes_removed;
            }
            bytes.extend_from_slice(arg);
            bytes.push(b' ');
        }

        let mut config: Self = zakros_redis::config::from_bytes(&bytes)?;
        if config.cluster_addrs.is_empty() {
            config.cluster_addrs.push((config.bind, config.port).into());
        }
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::Config;

    #[test]
    fn empty_config_is_valid() {
        let _: Config = zakros_redis::config::from_bytes(&[]).unwrap();
    }
}
