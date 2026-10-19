//! Shared message layouts and validation, usable with native or foreign values.
//! Foreign adapters keep payload values opaque; protocol rules live here.

use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Null,
    Int,
    Float,
    Bool,
    Str,
    List,
    Dict,
    Bytes,
    Other,
}

pub trait WireValue: Clone {
    fn kind(&self) -> Kind;

    fn type_name(&self) -> String;

    fn display(&self) -> String;

    fn integer(&self) -> Option<i64>;

    fn sequence(&self) -> Option<Vec<Self>>;

    fn member(&self, key: &str) -> Option<Self>;

    fn keys(&self) -> Vec<Self>;

    fn null(&self) -> Self;

    fn int(&self, value: i64) -> Self;

    fn list(&self, values: Vec<Self>) -> Self;

    fn dict(&self, values: BTreeMap<String, Self>) -> Self;
}

#[derive(Debug, Clone, Copy)]
pub enum Rule {
    Id,
    Int,
    Str,
    Dict,
    OptionalList,
    OptionalDict,
    Hello,
    Welcome,
}

#[derive(Debug, Clone, Copy)]
pub struct Field {
    pub index: usize,
    pub name: &'static str,
    pub rule: Rule,
}

#[derive(Debug, Clone, Copy)]
pub struct Schema {
    pub code: i64,
    pub name: &'static str,
    pub class_name: &'static str,
    pub min: usize,
    pub max: usize,
    pub fields: &'static [Field],
    pub constructor_fields: &'static [&'static str],
}

pub const ROLES: &[&str] = &["callee", "caller", "publisher", "subscriber", "dealer", "broker"];
pub const MAX_ID: i64 = 1 << 53;

macro_rules! layout {
    ($code:expr, $name:literal, $class:literal, $min:literal, $max:literal,
     [$(($index:literal, $field:ident, $rule:ident)),*], [$($arg:ident),*]) => {
        Schema { code: $code, name: $name, class_name: $class, min: $min, max: $max,
            fields: &[$(Field { index: $index, name: stringify!($field), rule: Rule::$rule }),*],
            constructor_fields: &[$(stringify!($arg)),*] }
    };
}

pub fn schema(code: i64) -> Option<Schema> {
    Some(match code {
        1 => layout!(
            code,
            "HELLO",
            "Hello",
            3,
            3,
            [(1, realm, Str), (2, hello_details, Hello)],
            [realm, roles, authid, authmethods, authextra]
        ),
        2 => layout!(
            code,
            "WELCOME",
            "Welcome",
            3,
            3,
            [(1, session_id, Id), (2, welcome_details, Welcome)],
            [session_id, roles, authid, authrole, authmethod, authextra]
        ),
        3 => layout!(
            code,
            "ABORT",
            "Abort",
            3,
            5,
            [
                (1, details, Dict),
                (2, reason, Str),
                (3, args, OptionalList),
                (4, kwargs, OptionalDict)
            ],
            [details, reason, args, kwargs]
        ),
        4 => layout!(
            code,
            "CHALLENGE",
            "Challenge",
            3,
            3,
            [(1, authmethod, Str), (2, extra, Dict)],
            [authmethod, extra]
        ),
        5 => layout!(
            code,
            "AUTHENTICATE",
            "Authenticate",
            3,
            3,
            [(1, signature, Str), (2, extra, Dict)],
            [signature, extra]
        ),
        6 => layout!(
            code,
            "GOODBYE",
            "Goodbye",
            3,
            3,
            [(1, details, Dict), (2, reason, Str)],
            [details, reason]
        ),
        8 => layout!(
            code,
            "ERROR",
            "Error",
            5,
            7,
            [
                (1, message_type, Int),
                (2, request_id, Id),
                (3, details, Dict),
                (4, uri, Str),
                (5, args, OptionalList),
                (6, kwargs, OptionalDict)
            ],
            [message_type, request_id, uri, args, kwargs, details]
        ),
        16 => layout!(
            code,
            "PUBLISH",
            "Publish",
            4,
            6,
            [
                (1, request_id, Id),
                (2, options, Dict),
                (3, topic, Str),
                (4, args, OptionalList),
                (5, kwargs, OptionalDict)
            ],
            [request_id, topic, args, kwargs, options]
        ),
        17 => layout!(
            code,
            "PUBLISHED",
            "Published",
            3,
            3,
            [(1, request_id, Id), (2, publication_id, Id)],
            [request_id, publication_id]
        ),
        32 => layout!(
            code,
            "SUBSCRIBE",
            "Subscribe",
            4,
            4,
            [(1, request_id, Id), (2, options, Dict), (3, topic, Str)],
            [request_id, topic, options]
        ),
        33 => layout!(
            code,
            "SUBSCRIBED",
            "Subscribed",
            3,
            3,
            [(1, request_id, Id), (2, subscription_id, Id)],
            [request_id, subscription_id]
        ),
        34 => layout!(
            code,
            "UNSUBSCRIBE",
            "Unsubscribe",
            3,
            3,
            [(1, request_id, Id), (2, subscription_id, Id)],
            [request_id, subscription_id]
        ),
        35 => layout!(
            code,
            "UNSUBSCRIBED",
            "Unsubscribed",
            2,
            2,
            [(1, request_id, Id)],
            [request_id]
        ),
        36 => layout!(
            code,
            "EVENT",
            "Event",
            4,
            6,
            [
                (1, subscription_id, Id),
                (2, publication_id, Id),
                (3, details, Dict),
                (4, args, OptionalList),
                (5, kwargs, OptionalDict)
            ],
            [subscription_id, publication_id, args, kwargs, details]
        ),
        48 => layout!(
            code,
            "CALL",
            "Call",
            4,
            6,
            [
                (1, request_id, Id),
                (2, options, Dict),
                (3, procedure, Str),
                (4, args, OptionalList),
                (5, kwargs, OptionalDict)
            ],
            [request_id, procedure, args, kwargs, options]
        ),
        49 => layout!(
            code,
            "CANCEL",
            "Cancel",
            3,
            3,
            [(1, request_id, Id), (2, options, Dict)],
            [request_id, options]
        ),
        50 => layout!(
            code,
            "RESULT",
            "Result",
            3,
            5,
            [
                (1, request_id, Id),
                (2, details, Dict),
                (3, args, OptionalList),
                (4, kwargs, OptionalDict)
            ],
            [request_id, args, kwargs, details]
        ),
        64 => layout!(
            code,
            "REGISTER",
            "Register",
            4,
            4,
            [(1, request_id, Id), (2, options, Dict), (3, procedure, Str)],
            [request_id, procedure, options]
        ),
        65 => layout!(
            code,
            "REGISTERED",
            "Registered",
            3,
            3,
            [(1, request_id, Id), (2, registration_id, Id)],
            [request_id, registration_id]
        ),
        66 => layout!(
            code,
            "UNREGISTER",
            "Unregister",
            3,
            3,
            [(1, request_id, Id), (2, registration_id, Id)],
            [request_id, registration_id]
        ),
        67 => layout!(
            code,
            "UNREGISTERED",
            "Unregistered",
            2,
            2,
            [(1, request_id, Id)],
            [request_id]
        ),
        68 => layout!(
            code,
            "INVOCATION",
            "Invocation",
            4,
            6,
            [
                (1, request_id, Id),
                (2, registration_id, Id),
                (3, details, Dict),
                (4, args, OptionalList),
                (5, kwargs, OptionalDict)
            ],
            [request_id, registration_id, args, kwargs, details]
        ),
        69 => layout!(
            code,
            "INTERRUPT",
            "Interrupt",
            3,
            3,
            [(1, request_id, Id), (2, options, Dict)],
            [request_id, options]
        ),
        70 => layout!(
            code,
            "YIELD",
            "Yield",
            3,
            5,
            [
                (1, request_id, Id),
                (2, options, Dict),
                (3, args, OptionalList),
                (4, kwargs, OptionalDict)
            ],
            [request_id, args, kwargs, options]
        ),
        _ => return None,
    })
}

pub fn sanity_check<V: WireValue>(value: &V, min: usize, max: usize, code: i64, name: &str) -> Result<Vec<V>, String> {
    let data = value.sequence().ok_or_else(|| {
        format!(
            "invalid message type {} for {name}, type should be a list",
            value.type_name()
        )
    })?;

    if data.len() < min {
        return Err(format!("invalid message length {}, must be at least {min}", data.len()));
    }

    if data.len() > max {
        return Err(format!("invalid message length {}, must be at most {max}", data.len()));
    }

    let Some(first) = data.first() else {
        return Err("received empty wamp message array".into());
    };

    if first.integer() != Some(code) {
        return Err(format!(
            "invalid message id {} for {name}, expected {code}",
            first.display()
        ));
    }

    Ok(data)
}

pub fn validate_type<V: WireValue>(value: &V, kind: Kind, index: usize, name: &str) -> Option<String> {
    if value.kind() == kind {
        return None;
    }

    let expected = match kind {
        Kind::Int => "int",
        Kind::Str => "string",
        Kind::List => "list",
        Kind::Dict => "dict",
        _ => "unknown",
    };
    Some(format!(
        "{name}: value at index {index} must be of type '{expected}' but was {}",
        value.type_name()
    ))
}

pub fn validate_id<V: WireValue>(value: &V, index: usize, name: &str) -> Option<String> {
    if let Some(error) = validate_type(value, Kind::Int, index, name) {
        return Some(error);
    }

    if !value.integer().is_some_and(|id| (1..=MAX_ID).contains(&id)) {
        return Some(format!(
            "{name}: value at index {index} must be between '1' and '{MAX_ID}' but was {}",
            value.display()
        ));
    }

    None
}

fn roles_error<V: WireValue>(value: &V, index: usize, name: &str) -> Option<String> {
    if value.kind() != Kind::Dict {
        return Some(format!(
            "{name}: value at index {index} for key 'roles' must be of type 'dict' but was {}",
            value.type_name()
        ));
    }

    let allowed = format!(
        "[{}]",
        ROLES
            .iter()
            .map(|role| format!("'{role}'"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let keys = value.keys();

    if keys.is_empty() {
        return Some(format!(
            "{name}: value at index {index} for roles key must be in {allowed} but was empty"
        ));
    }

    for key in keys {
        if key.kind() != Kind::Str || !ROLES.contains(&key.display().as_str()) {
            return Some(format!(
                "{name}: value at index {index} for roles key must be in {allowed} but was {}",
                key.display()
            ));
        }
    }

    None
}

pub fn detail_fields<V: WireValue>(
    value: &V,
    index: usize,
    name: &str,
    welcome: bool,
) -> (BTreeMap<String, V>, Vec<String>) {
    let mut fields = BTreeMap::new();
    let mut errors = Vec::new();

    if let Some(error) = validate_type(value, Kind::Dict, index, name) {
        return (fields, vec![error]);
    }

    let keys: &[(&str, Kind)] = if welcome {
        &[
            ("roles", Kind::Dict),
            ("authid", Kind::Str),
            ("authrole", Kind::Str),
            ("authextra", Kind::Dict),
            ("authmethod", Kind::Str),
        ]
    } else {
        &[
            ("authid", Kind::Str),
            ("authrole", Kind::Str),
            ("authmethods", Kind::List),
            ("roles", Kind::Dict),
            ("authextra", Kind::Dict),
        ]
    };

    for &(key, kind) in keys {
        let entry = value.member(key).unwrap_or_else(|| value.null());

        if key == "roles" {
            if let Some(error) = roles_error(&entry, index, name) {
                errors.push(error);
            }
        } else if entry.kind() != Kind::Null && entry.kind() != kind {
            let expected = match kind {
                Kind::Str => "string",
                Kind::List => "list",
                _ => "dict",
            };
            errors.push(format!(
                "{name}: value at index {index} for key '{key}' must be of type '{expected}' but was {}",
                entry.type_name()
            ));
        }

        fields.insert(key.into(), entry);
    }

    fields.insert("details".into(), value.clone());
    (fields, errors)
}

pub fn validate_rule<V: WireValue>(value: &V, rule: Rule, index: usize, name: &str) -> Vec<String> {
    let error = match rule {
        Rule::Id => validate_id(value, index, name),
        Rule::Int => validate_type(value, Kind::Int, index, name),
        Rule::Str => validate_type(value, Kind::Str, index, name),
        Rule::Dict | Rule::OptionalDict => validate_type(value, Kind::Dict, index, name),
        Rule::OptionalList => validate_type(value, Kind::List, index, name),
        Rule::Hello | Rule::Welcome => return detail_fields(value, index, name, matches!(rule, Rule::Welcome)).1,
    };
    error.into_iter().collect()
}

impl Schema {
    pub fn parse<V: WireValue>(&self, value: &V) -> Result<BTreeMap<String, V>, Vec<String>> {
        sanity_check(value, self.min, self.max, self.code, self.name).map_err(|error| vec![error])?;
        let (fields, errors) = self.read_fields(value);

        if errors.is_empty() { Ok(fields) } else { Err(errors) }
    }

    /// Read fields from an outgoing message without validating permissive constructors.
    pub fn extract_fields<V: WireValue>(&self, value: &V) -> BTreeMap<String, V> {
        self.read_fields(value).0
    }

    fn read_fields<V: WireValue>(&self, value: &V) -> (BTreeMap<String, V>, Vec<String>) {
        let data = value.sequence().unwrap_or_default();
        let mut fields = BTreeMap::new();
        let mut errors = Vec::new();

        for field in self.fields {
            if let Some(value) = data.get(field.index) {
                if matches!(field.rule, Rule::Hello | Rule::Welcome) {
                    let (details, detail_errors) =
                        detail_fields(value, field.index, self.name, matches!(field.rule, Rule::Welcome));
                    fields.extend(details);
                    errors.extend(detail_errors);
                } else {
                    errors.extend(validate_rule(value, field.rule, field.index, self.name));
                    fields.insert(field.name.into(), value.clone());
                }
            } else {
                fields.insert(field.name.into(), value.null());
            }
        }

        (fields, errors)
    }

    /// Construct a wire list without validating application-provided fields.
    /// Parsing is the validation boundary; constructors remain permissive.
    pub fn marshal<V: WireValue>(&self, context: &V, fields: &BTreeMap<String, V>) -> V {
        let get = |name: &str| fields.get(name).cloned().unwrap_or_else(|| context.null());
        let mut wire = vec![context.int(self.code)];

        for field in self.fields {
            match field.rule {
                Rule::Hello | Rule::Welcome => {
                    let mut details = BTreeMap::new();
                    details.insert("roles".into(), get("roles"));

                    for &key in self.constructor_fields {
                        if matches!(key, "authid" | "authrole" | "authmethods" | "authmethod" | "authextra") {
                            let value = get(key);

                            if value.kind() != Kind::Null {
                                details.insert(key.into(), value);
                            }
                        }
                    }

                    wire.push(context.dict(details));
                }
                Rule::OptionalList => {
                    let args = get(field.name);

                    if args.kind() != Kind::Null {
                        wire.push(args);
                    }
                }
                Rule::OptionalDict => {
                    let kwargs = get(field.name);

                    if kwargs.kind() != Kind::Null {
                        if get("args").kind() == Kind::Null {
                            wire.push(context.list(Vec::new()));
                        }

                        wire.push(kwargs);
                    }
                }
                _ => wire.push(get(field.name)),
            }
        }

        context.list(wire)
    }
}

impl WireValue for super::types::Value {
    fn kind(&self) -> Kind {
        use super::types::Value;

        match self {
            Value::Null => Kind::Null,
            Value::Int(_) | Value::UInt(_) => Kind::Int,
            Value::Float(_) => Kind::Float,
            Value::Bool(_) => Kind::Bool,
            Value::Str(_) => Kind::Str,
            Value::List(_) => Kind::List,
            Value::Dict(_) => Kind::Dict,
            Value::Bytes(_) => Kind::Bytes,
        }
    }

    fn type_name(&self) -> String {
        match self.kind() {
            Kind::Null => "NoneType",
            Kind::Int => "int",
            Kind::Float => "float",
            Kind::Bool => "bool",
            Kind::Str => "str",
            Kind::List => "list",
            Kind::Dict => "dict",
            Kind::Bytes => "bytes",
            Kind::Other => "object",
        }
        .into()
    }

    fn display(&self) -> String {
        use super::types::Value;

        match self {
            Value::Str(value) => value.clone(),
            Value::Int(value) => value.to_string(),
            Value::UInt(value) => value.to_string(),
            Value::Null => "None".into(),
            _ => format!("{self:?}"),
        }
    }

    fn integer(&self) -> Option<i64> {
        use super::types::Value;

        match self {
            Value::Int(value) => Some(*value),
            Value::UInt(value) => (*value).try_into().ok(),
            _ => None,
        }
    }

    fn sequence(&self) -> Option<Vec<Self>> {
        if let Self::List(values) = self {
            Some(values.clone())
        } else {
            None
        }
    }

    fn member(&self, key: &str) -> Option<Self> {
        if let Self::Dict(values) = self {
            values.get(key).cloned()
        } else {
            None
        }
    }

    fn keys(&self) -> Vec<Self> {
        if let Self::Dict(values) = self {
            values.keys().map(|key| Self::Str(key.clone())).collect()
        } else {
            Vec::new()
        }
    }

    fn null(&self) -> Self {
        Self::Null
    }

    fn int(&self, value: i64) -> Self {
        Self::Int(value)
    }

    fn list(&self, values: Vec<Self>) -> Self {
        Self::List(values)
    }

    fn dict(&self, values: BTreeMap<String, Self>) -> Self {
        Self::Dict(values.into_iter().collect())
    }
}

pub const CODES: &[i64] = &[
    1, 2, 3, 4, 5, 6, 8, 16, 17, 32, 33, 34, 35, 36, 48, 49, 50, 64, 65, 66, 67, 68, 69, 70,
];

pub fn named(name: &str) -> Option<Schema> {
    CODES
        .iter()
        .filter_map(|&code| schema(code))
        .find(|schema| schema.name == name)
}

pub fn marshal_values(
    code: i64,
    fields: impl IntoIterator<Item = (&'static str, super::types::Value)>,
) -> Vec<super::types::Value> {
    let fields = fields.into_iter().map(|(key, value)| (key.into(), value)).collect();
    let Some(schema) = schema(code) else {
        unreachable!("unknown built-in message type")
    };
    let super::types::Value::List(values) = schema.marshal(&super::types::Value::Null, &fields) else {
        unreachable!()
    };
    values
}

/// Compatibility-facing scalar validation uses the same kinds and ID domain.
/// The category is translated into a host language's public exception type.
pub fn scalar<V: WireValue>(
    kind: &str,
    value: &V,
    message: &str,
    field: Option<&str>,
) -> Result<(), (&'static str, String)> {
    match kind {
        "realm" | "uri" | "procedure" => {
            let category = if kind == "realm" {
                "InvalidRealmError"
            } else {
                "InvalidUriError"
            };

            if value.kind() == Kind::Null {
                return Err((category, format!("{kind} cannot be null for {message}")));
            }

            if value.kind() != Kind::Str {
                return Err((category, format!("{kind} must be of type string for {message}")));
            }
        }
        "details" => {
            if value.kind() != Kind::Dict {
                return Err((
                    "InvalidDetailsError",
                    format!(
                        "{} must be of type dictionary for {message}",
                        field.unwrap_or("details")
                    ),
                ));
            }

            for key in value.keys() {
                if key.kind() != Kind::Str {
                    return Err((
                        "InvalidDetailsError",
                        format!(
                            "invalid type for key '{}' in extra details for {message}",
                            key.display()
                        ),
                    ));
                }
            }
        }
        "session_id" => {
            if value.kind() != Kind::Int {
                return Err(("ProtocolError", format!("session ID must be an integer for {message}")));
            }

            if !value.integer().is_some_and(|id| (0..=MAX_ID).contains(&id)) {
                return Err((
                    "ProtocolError",
                    format!("invalid {} value for {message}", field.unwrap_or("Session ID")),
                ));
            }
        }
        _ => return Err(("ValueError", "unknown scalar validation rule".into())),
    }

    Ok(())
}

pub fn named_rule(name: &str) -> Option<Rule> {
    Some(match name {
        "request_id" | "session_id" | "subscription_id" | "publication_id" | "registration_id" => Rule::Id,
        "message_type" | "int_or_raise" => Rule::Int,
        "options" | "details" | "extra" | "dict_or_raise" => Rule::Dict,
        "args" | "list_or_raise" => Rule::OptionalList,
        "kwargs" => Rule::OptionalDict,
        "hello_details" => Rule::Hello,
        "welcome_details" => Rule::Welcome,
        "uri" | "procedure" | "realm" | "authmethod" | "signature" | "reason" | "topic" | "string_or_raise" => {
            Rule::Str
        }
        _ => return None,
    })
}

pub fn validate_named_field<V: WireValue>(
    field: &str,
    input: &V,
    index: usize,
    name: &str,
) -> (BTreeMap<String, V>, Vec<String>) {
    let mut fields = BTreeMap::new();

    if matches!(
        field,
        "authid" | "authrole" | "authmethods" | "authextra" | "roles" | "welcome_authmethod"
    ) {
        let key = if field == "welcome_authmethod" {
            "authmethod"
        } else {
            field
        };
        let value = input.member(key).unwrap_or_else(|| input.null());
        let kind = match field {
            "authmethods" => Kind::List,
            "authextra" | "roles" => Kind::Dict,
            _ => Kind::Str,
        };
        let error = if field == "roles" {
            roles_error(&value, index, name)
        } else if value.kind() == Kind::Null || value.kind() == kind {
            None
        } else {
            let expected = match kind {
                Kind::List => "list",
                Kind::Dict => "dict",
                _ => "string",
            };
            Some(format!(
                "{name}: value at index {index} for key '{key}' must be of type '{expected}' but was {}",
                value.type_name()
            ))
        };
        fields.insert(key.into(), value);
        return (fields, error.into_iter().collect());
    }

    let Some(rule) = named_rule(field) else {
        return (fields, vec!["unknown field validation rule".into()]);
    };
    let data = input.sequence().unwrap_or_default();
    let Some(value) = data.get(index) else {
        return (fields, Vec::new());
    };

    if matches!(rule, Rule::Hello | Rule::Welcome) {
        return detail_fields(value, index, name, matches!(rule, Rule::Welcome));
    }

    let errors = validate_rule(value, rule, index, name);

    if errors.is_empty() {
        fields.insert(field.into(), value.clone());
    }

    (fields, errors)
}
