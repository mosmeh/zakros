use super::{Arity, CommandSpec};
use crate::command;

impl CommandSpec for command::Cluster {
    const NAME: &'static str = "CLUSTER";
    const ARITY: Arity = Arity::AtLeast(1);
}

impl CommandSpec for command::Config {
    const NAME: &'static str = "CONFIG";
    const ARITY: Arity = Arity::AtLeast(1);
}

impl CommandSpec for command::Debug {
    const NAME: &'static str = "DEBUG";
    const ARITY: Arity = Arity::AtLeast(1);
}

impl CommandSpec for command::Function {
    const NAME: &'static str = "FUNCTION";
    const ARITY: Arity = Arity::AtLeast(1);
}

impl CommandSpec for command::Hello {
    const NAME: &'static str = "HELLO";
    const ARITY: Arity = Arity::AtLeast(0);
}

impl CommandSpec for command::Info {
    const NAME: &'static str = "INFO";
    const ARITY: Arity = Arity::AtLeast(0);
}

impl CommandSpec for command::Memory {
    const NAME: &'static str = "MEMORY";
    const ARITY: Arity = Arity::AtLeast(1);
}

impl CommandSpec for command::PSubscribe {
    const NAME: &'static str = "PSUBSCRIBE";
    const ARITY: Arity = Arity::AtLeast(1);
}

impl CommandSpec for command::Publish {
    const NAME: &'static str = "PUBLISH";
    const ARITY: Arity = Arity::Fixed(2);
}

impl CommandSpec for command::PubSub {
    const NAME: &'static str = "PUBSUB";
    const ARITY: Arity = Arity::AtLeast(1);
}

impl CommandSpec for command::PUnsubscribe {
    const NAME: &'static str = "PUNSUBSCRIBE";
    const ARITY: Arity = Arity::AtLeast(0);
}

impl CommandSpec for command::ReadOnly {
    const NAME: &'static str = "READONLY";
    const ARITY: Arity = Arity::Fixed(0);
}

impl CommandSpec for command::ReadWrite {
    const NAME: &'static str = "READWRITE";
    const ARITY: Arity = Arity::Fixed(0);
}

impl CommandSpec for command::Select {
    const NAME: &'static str = "SELECT";
    const ARITY: Arity = Arity::Fixed(1);
}

impl CommandSpec for command::Shutdown {
    const NAME: &'static str = "SHUTDOWN";
    const ARITY: Arity = Arity::AtLeast(0);
}

impl CommandSpec for command::Subscribe {
    const NAME: &'static str = "SUBSCRIBE";
    const ARITY: Arity = Arity::AtLeast(1);
}

impl CommandSpec for command::Unsubscribe {
    const NAME: &'static str = "UNSUBSCRIBE";
    const ARITY: Arity = Arity::AtLeast(0);
}
