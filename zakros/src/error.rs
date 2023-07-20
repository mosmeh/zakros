use crate::command::RedisCommand;

#[derive(thiserror::Error, Debug, Clone)]
pub enum Error {
    #[error(transparent)]
    Raft(#[from] zakros_raft::Error),

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
}

#[derive(thiserror::Error, Debug, Clone)]
pub enum TransactionError {
    #[error("ERR MULTI calls can not be nested")]
    NestedMulti,

    #[error("ERR {0} without MULTI")]
    CommandWithoutMulti(RedisCommand),

    #[error("ERR {0} inside MULTI is not allowed")]
    CommandInsideMulti(RedisCommand),

    #[error("EXECABORT Transaction discarded because of previous errors.")]
    ExecAborted,
}
