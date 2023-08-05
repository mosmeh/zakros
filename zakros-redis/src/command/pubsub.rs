use super::{Arity, CommandSpec};
use crate::command;

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

impl CommandSpec for command::Subscribe {
    const NAME: &'static str = "SUBSCRIBE";
    const ARITY: Arity = Arity::AtLeast(1);
}

impl CommandSpec for command::Unsubscribe {
    const NAME: &'static str = "UNSUBSCRIBE";
    const ARITY: Arity = Arity::AtLeast(0);
}
