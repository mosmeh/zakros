use super::{Arity, CommandSpec};
use crate::command;

impl CommandSpec for command::Discard {
    const NAME: &'static str = "DISCARD";
    const ARITY: Arity = Arity::Fixed(0);
}

impl CommandSpec for command::Exec {
    const NAME: &'static str = "EXEC";
    const ARITY: Arity = Arity::Fixed(0);
}

impl CommandSpec for command::Multi {
    const NAME: &'static str = "MULTI";
    const ARITY: Arity = Arity::Fixed(0);
}
