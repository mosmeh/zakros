use super::{Arity, CommandSpec};
use crate::command;

impl CommandSpec for command::Cluster {
    const NAME: &'static str = "CLUSTER";
    const ARITY: Arity = Arity::AtLeast(1);
}

impl CommandSpec for command::ReadOnly {
    const NAME: &'static str = "READONLY";
    const ARITY: Arity = Arity::Fixed(0);
}

impl CommandSpec for command::ReadWrite {
    const NAME: &'static str = "READWRITE";
    const ARITY: Arity = Arity::Fixed(0);
}
