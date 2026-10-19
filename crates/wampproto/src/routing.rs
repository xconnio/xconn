//! Shared sans-I/O router data and message adaptation.
use crate::messages::{
    message::Message,
    schema,
    types::{Error, Value},
};
use crate::serializers::helpers::to_message;
use std::collections::{BTreeMap, HashMap};

pub type Fields = BTreeMap<String, Value>;

pub struct RoutedMessage {
    pub message: Box<dyn Message>,
    pub recipient: i64,
}

impl RoutedMessage {
    pub(crate) fn new(recipient: i64, message: Box<dyn Message>) -> Self {
        Self { recipient, message }
    }
}

pub(crate) fn fields(message: &dyn Message) -> Result<(i64, Fields), Error> {
    let code = schema::WireValue::integer(&message.message_type()).ok_or_else(|| Error::new("invalid message type"))?;
    let spec = schema::schema(code).ok_or_else(|| Error::new("unknown message type"))?;
    let fields = spec
        .parse(&Value::List(message.marshal()))
        .map_err(|e| Error::new(e.join("; ")))?;
    Ok((code, fields))
}

pub(crate) fn make(code: i64, fields: Fields) -> Result<Box<dyn Message>, Error> {
    let spec = schema::schema(code).ok_or_else(|| Error::new("unknown message type"))?;
    let Value::List(wire) = spec.marshal(&Value::Null, &fields) else {
        unreachable!()
    };
    to_message(wire)
}

pub(crate) fn id(f: &Fields, name: &str) -> i64 {
    schema::WireValue::integer(&f[name]).expect("validated ID")
}

pub(crate) fn string<'a>(f: &'a Fields, name: &str) -> &'a str {
    if let Value::Str(v) = &f[name] {
        v
    } else {
        unreachable!("validated string")
    }
}

pub(crate) fn options(f: &Fields, name: &str) -> HashMap<String, Value> {
    if let Value::Dict(v) = &f[name] {
        v.clone()
    } else {
        HashMap::new()
    }
}

pub(crate) fn flag(d: &HashMap<String, Value>, key: &str) -> bool {
    matches!(d.get(key), Some(Value::Bool(true)))
}

pub(crate) fn option<'a>(d: &'a HashMap<String, Value>, key: &str, default: &'a str) -> &'a str {
    if let Some(Value::Str(v)) = d.get(key) {
        v
    } else {
        default
    }
}

pub(crate) fn payload(f: &Fields, out: &mut Fields) {
    for key in ["args", "kwargs"] {
        out.insert(key.into(), f.get(key).cloned().unwrap_or(Value::Null));
    }
}

pub(crate) fn error(recipient: i64, code: i64, request: i64, uri: &str) -> Result<RoutedMessage, Error> {
    Ok(RoutedMessage::new(
        recipient,
        make(
            8,
            BTreeMap::from([
                ("message_type".into(), Value::Int(code)),
                ("request_id".into(), Value::Int(request)),
                ("details".into(), Value::Dict(HashMap::new())),
                ("uri".into(), Value::Str(uri.into())),
            ]),
        )?,
    ))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MatchPolicy {
    Exact,
    Prefix,
    Wildcard,
}

impl MatchPolicy {
    pub fn parse(value: &str) -> Result<Self, Error> {
        match value {
            "" | "exact" => Ok(Self::Exact),
            "prefix" => Ok(Self::Prefix),
            "wildcard" => Ok(Self::Wildcard),
            _ => Err(Error::new("invalid match policy")),
        }
    }

    pub fn matches(self, pattern: &str, uri: &str) -> bool {
        match self {
            Self::Exact => pattern == uri,
            Self::Prefix => uri.starts_with(pattern),
            Self::Wildcard => wildcard_match(pattern, uri),
        }
    }
}

#[derive(Debug)]
enum Atom {
    Star,
    Any,
    Literal(char),
    Class { ranges: Vec<(char, char)>, negate: bool },
}

impl Atom {
    fn matches(&self, c: char) -> bool {
        match self {
            Self::Star => false,
            Self::Any => c != '/',
            Self::Literal(v) => *v == c,
            Self::Class { ranges, negate } => ranges.iter().any(|(lo, hi)| *lo <= c && c <= *hi) != *negate,
        }
    }
}

fn glob(pattern: &str) -> Option<Vec<Atom>> {
    let chars: Vec<_> = pattern.chars().collect();
    let mut i = 0;
    let mut out = Vec::new();

    fn class_char(chars: &[char], i: &mut usize) -> Option<char> {
        let mut c = *chars.get(*i)?;
        *i += 1;

        if c == '\\' {
            c = *chars.get(*i)?;
            *i += 1;
        } else if c == '-' || c == ']' {
            return None;
        }

        Some(c)
    }

    while i < chars.len() {
        let c = chars[i];
        i += 1;
        out.push(match c {
            '*' => Atom::Star,
            '?' => Atom::Any,
            '\\' => {
                let c = *chars.get(i)?;
                i += 1;
                Atom::Literal(c)
            }
            '[' => {
                let negate = chars.get(i) == Some(&'^');

                if negate {
                    i += 1;
                }

                let mut ranges = Vec::new();

                loop {
                    let lo = class_char(&chars, &mut i)?;
                    let hi = if chars.get(i) == Some(&'-') {
                        i += 1;
                        class_char(&chars, &mut i)?
                    } else {
                        lo
                    };
                    ranges.push((lo, hi));

                    if chars.get(i) == Some(&']') {
                        i += 1;
                        break;
                    }
                }

                Atom::Class { ranges, negate }
            }
            _ => Atom::Literal(c),
        });
    }

    Some(out)
}

/// Empty dot-components match one component. Legacy Go path globs are accepted too.
pub fn wildcard_match(pattern: &str, uri: &str) -> bool {
    if !pattern.contains(['*', '?', '[', '\\']) {
        let p: Vec<_> = pattern.split('.').collect();
        let u: Vec<_> = uri.split('.').collect();
        return p.len() == u.len() && p.iter().zip(u).all(|(p, u)| !u.is_empty() && (p.is_empty() || *p == u));
    }

    let Some(p) = glob(pattern) else { return false };
    let u: Vec<_> = uri.chars().collect();
    let (mut i, mut j, mut star, mut retry) = (0, 0, None, 0);

    while j < u.len() {
        if i < p.len() && p[i].matches(u[j]) {
            i += 1;
            j += 1;
        } else if i < p.len() && matches!(p[i], Atom::Star) {
            star = Some(i);
            i += 1;
            retry = j;
        } else if let Some(s) = star {
            if u[retry] == '/' {
                return false;
            }

            retry += 1;
            j = retry;
            i = s + 1;
        } else {
            return false;
        }
    }

    while i < p.len() && matches!(p[i], Atom::Star) {
        i += 1;
    }

    i == p.len()
}

pub(crate) fn forward(
    code: i64,
    fields: Fields,
    source: &dyn Message,
    static_serializer: bool,
) -> Result<Box<dyn Message>, Error> {
    let message = make(code, fields)?;

    if static_serializer && let Some(payload) = source.opaque_payload() {
        return Ok(Box::new(crate::messages::payload::PayloadMessage::new(
            message,
            payload.clone(),
        )?));
    }

    Ok(message)
}
