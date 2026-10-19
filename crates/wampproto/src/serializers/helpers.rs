use crate::messages::abort::{Abort, MESSAGE_TYPE_ABORT};
use crate::messages::authenticate::{Authenticate, MESSAGE_TYPE_AUTHENTICATE};
use crate::messages::call::{Call, MESSAGE_TYPE_CALL};
use crate::messages::cancel::{Cancel, MESSAGE_TYPE_CANCEL};
use crate::messages::challenge::{Challenge, MESSAGE_TYPE_CHALLENGE};
use crate::messages::error::{Error, MESSAGE_TYPE_ERROR};
use crate::messages::event::{Event, MESSAGE_TYPE_EVENT};
use crate::messages::goodbye::{Goodbye, MESSAGE_TYPE_GOODBYE};
use crate::messages::hello::{Hello, MESSAGE_TYPE_HELLO};
use crate::messages::interrupt::{Interrupt, MESSAGE_TYPE_INTERRUPT};
use crate::messages::invocation::{Invocation, MESSAGE_TYPE_INVOCATION};
use crate::messages::message::Message;
use crate::messages::publish::{MESSAGE_TYPE_PUBLISH, Publish};
use crate::messages::published::{MESSAGE_TYPE_PUBLISHED, Published};
use crate::messages::register::{MESSAGE_TYPE_REGISTER, Register};
use crate::messages::registered::{MESSAGE_TYPE_REGISTERED, Registered};
use crate::messages::result::{MESSAGE_TYPE_RESULT, Result_};
use crate::messages::subscribe::{MESSAGE_TYPE_SUBSCRIBE, Subscribe};
use crate::messages::subscribed::{MESSAGE_TYPE_SUBSCRIBED, Subscribed};
use crate::messages::types::{Error as XError, Value};
use crate::messages::unregister::{MESSAGE_TYPE_UNREGISTER, Unregister};
use crate::messages::unregistered::{MESSAGE_TYPE_UNREGISTERED, Unregistered};
use crate::messages::unsubscribe::{MESSAGE_TYPE_UNSUBSCRIBE, Unsubscribe};
use crate::messages::unsubscribed::{MESSAGE_TYPE_UNSUBSCRIBED, Unsubscribed};
use crate::messages::welcome::{MESSAGE_TYPE_WELCOME, Welcome};
use crate::messages::yield_::{MESSAGE_TYPE_YIELD, Yield};

pub fn to_message(wamp_msg: Vec<Value>) -> Result<Box<dyn Message>, XError> {
    if wamp_msg.is_empty() {
        return Err(XError::new("received empty wamp message array"));
    }

    match wamp_msg[0] {
        MESSAGE_TYPE_ABORT => Abort::parse(wamp_msg),
        MESSAGE_TYPE_AUTHENTICATE => Authenticate::parse(wamp_msg),
        MESSAGE_TYPE_CALL => Call::parse(wamp_msg),
        MESSAGE_TYPE_CANCEL => Cancel::parse(wamp_msg),
        MESSAGE_TYPE_CHALLENGE => Challenge::parse(wamp_msg),
        MESSAGE_TYPE_ERROR => Error::parse(wamp_msg),
        MESSAGE_TYPE_EVENT => Event::parse(wamp_msg),
        MESSAGE_TYPE_GOODBYE => Goodbye::parse(wamp_msg),
        MESSAGE_TYPE_HELLO => Hello::parse(wamp_msg),
        MESSAGE_TYPE_INTERRUPT => Interrupt::parse(wamp_msg),
        MESSAGE_TYPE_INVOCATION => Invocation::parse(wamp_msg),
        MESSAGE_TYPE_PUBLISH => Publish::parse(wamp_msg),
        MESSAGE_TYPE_PUBLISHED => Published::parse(wamp_msg),
        MESSAGE_TYPE_REGISTER => Register::parse(wamp_msg),
        MESSAGE_TYPE_REGISTERED => Registered::parse(wamp_msg),
        MESSAGE_TYPE_RESULT => Result_::parse(wamp_msg),
        MESSAGE_TYPE_SUBSCRIBE => Subscribe::parse(wamp_msg),
        MESSAGE_TYPE_SUBSCRIBED => Subscribed::parse(wamp_msg),
        MESSAGE_TYPE_UNSUBSCRIBE => Unsubscribe::parse(wamp_msg),
        MESSAGE_TYPE_UNSUBSCRIBED => Unsubscribed::parse(wamp_msg),
        MESSAGE_TYPE_UNREGISTER => Unregister::parse(wamp_msg),
        MESSAGE_TYPE_UNREGISTERED => Unregistered::parse(wamp_msg),
        MESSAGE_TYPE_WELCOME => Welcome::parse(wamp_msg),
        MESSAGE_TYPE_YIELD => Yield::parse(wamp_msg),
        _ => Err(XError::new(format!(
            "received invalid wamp message of type {:?}",
            wamp_msg[0]
        ))),
    }
}

/// Convert foreign integer representations in protocol fields without changing application payloads.
pub fn to_typed_message(mut wire: Vec<Value>) -> Result<Box<dyn Message>, XError> {
    use crate::messages::schema::{self, Rule, WireValue};
    let code = wire
        .first()
        .and_then(WireValue::integer)
        .ok_or_else(|| XError::new("invalid message code"))?;
    let spec = schema::schema(code).ok_or_else(|| XError::new("unknown message code"))?;
    wire[0] = Value::Int(code);

    for field in spec.fields {
        if matches!(field.rule, Rule::Id | Rule::Int)
            && let Some(Value::UInt(value)) = wire.get(field.index)
        {
            wire[field.index] =
                Value::Int(i64::try_from(*value).map_err(|_| XError::new("protocol integer exceeds range"))?);
        }
    }

    to_message(wire)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn foreign_protocol_integers_do_not_change_payload_signedness() {
        let message = to_typed_message(vec![
            Value::UInt(50),
            Value::UInt(7),
            Value::Dict(Default::default()),
            Value::List(vec![Value::UInt(42), Value::UInt(u64::MAX), Value::Int(-1)]),
        ])
        .unwrap();
        let wire = message.marshal();
        assert_eq!(wire[0], Value::Int(50));
        assert_eq!(wire[1], Value::Int(7));
        assert_eq!(
            wire[3],
            Value::List(vec![Value::UInt(42), Value::UInt(u64::MAX), Value::Int(-1)])
        );
        assert!(
            to_typed_message(vec![
                Value::UInt(50),
                Value::UInt(u64::MAX),
                Value::Dict(Default::default())
            ])
            .is_err()
        );
    }
}
