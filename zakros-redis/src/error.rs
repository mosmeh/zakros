use std::net::SocketAddr;

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum Error {
    #[error("ERR {0}")]
    Response(#[from] ResponseError),

    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,

    #[error("EXECABORT Transaction discarded because of previous errors.")]
    ExecAbort,

    #[error("MOVED {slot} {addr}")]
    Moved { slot: u16, addr: SocketAddr },

    #[error("CLUSTERDOWN {0}")]
    ClusterDown(String),
}

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum ResponseError {
    #[error("Protocol error: {0}")]
    ProtocolError(String),

    #[error("unknown command '{0:.128}'")]
    UnknownCommand(String),

    #[error("unknown subcommand '{0:.128}'")]
    UnknownSubcommand(String),

    #[error("syntax error")]
    SyntaxError,

    #[error("wrong number of arguments")]
    WrongArity,

    #[error("value is not an integer or out of range")]
    NotInteger,

    #[error("value is out of range")]
    ValueOutOfRange,

    #[error("index out of range")]
    IndexOutOfRange,

    #[error("no such key")]
    NoKey,

    #[error("{0}")]
    Other(&'static str),
}

#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("Protocol error: {0}")]
    Protocol(String),
}
