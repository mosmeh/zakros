use crate::command::{RedisCommand, TransactionCommand};

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum Error {
    #[error("ERR Protocol error")]
    ProtocolError,

    #[error("ERR unknown command")]
    UnknownCommand,

    #[error("ERR unknown subcommand")]
    UnknownSubcommand,

    #[error("ERR syntax error")]
    SyntaxError,

    #[error("ERR wrong number of arguments")]
    WrongArity,

    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,

    #[error("ERR value is not an integer or out of range")]
    NotInteger,

    #[error("ERR value is out of range")]
    ValueOutOfRange,

    #[error("ERR index out of range")]
    IndexOutOfRange,

    #[error("ERR no such key")]
    NoKey,

    #[error(transparent)]
    Transaction(#[from] TransactionError),

    #[error("{0}")]
    Custom(String),
}

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum TransactionError {
    #[error("ERR MULTI calls can not be nested")]
    NestedMulti,

    #[error("ERR {0} without MULTI")]
    CommandWithoutMulti(TransactionCommand),

    #[error("ERR {0} inside MULTI is not allowed")]
    CommandInsideMulti(RedisCommand),

    #[error("EXECABORT Transaction discarded because of previous errors.")]
    ExecAborted,
}

#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("Protocol error")]
    Protocol,
}
