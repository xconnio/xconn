//! Pub/sub routing. The host sends the returned deliveries; this module does no I/O.
use crate::{
    idgen::SessionScopeIDGenerator,
    messages::{
        message::Message,
        publish::Publish,
        types::{Error, Value},
    },
    routing::{self, MatchPolicy, RoutedMessage},
    types::SessionDetails,
};
use std::collections::{BTreeMap, BTreeSet, HashMap};

#[derive(Clone, Debug)]
pub struct Subscription {
    pub id: i64,
    pub topic: String,
    pub match_policy: MatchPolicy,
    pub subscribers: BTreeSet<i64>,
}

pub struct EventDelivery {
    pub event: Box<dyn Message>,
    pub recipients: Vec<i64>,
}

pub struct Publication {
    pub events: Vec<EventDelivery>,
    pub ack: Option<RoutedMessage>,
}

#[derive(Default)]
pub struct Broker {
    sessions: HashMap<i64, SessionDetails>,
    subscriptions: BTreeMap<i64, Subscription>,
    ids: SessionScopeIDGenerator,
    disclose: bool,
}

impl Broker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_session(&mut self, details: SessionDetails) -> Result<(), Error> {
        if self.sessions.contains_key(&details.id) {
            return Err(Error::new("broker: session already exists"));
        }

        self.sessions.insert(details.id, details);
        Ok(())
    }

    pub fn remove_session(&mut self, id: i64) -> Result<(), Error> {
        if self.sessions.remove(&id).is_none() {
            return Err(Error::new("broker: session does not exist"));
        }

        self.subscriptions.retain(|_, s| {
            s.subscribers.remove(&id);
            !s.subscribers.is_empty()
        });
        Ok(())
    }

    pub fn has_subscription(&self, topic: &str) -> bool {
        self.subscriptions.values().any(|s| s.topic == topic)
    }

    pub fn auto_disclose_publisher(&mut self, disclose: bool) {
        self.disclose = disclose;
    }

    pub fn subscriptions(&self) -> impl Iterator<Item = &Subscription> {
        self.subscriptions.values()
    }

    pub fn receive_message(&mut self, session: i64, message: &dyn Message) -> Result<RoutedMessage, Error> {
        if !self.sessions.contains_key(&session) {
            return Err(Error::new("broker: unknown session"));
        }

        let (code, f) = routing::fields(message)?;

        if !matches!(code, 32 | 34) {
            return Err(Error::new("broker: unexpected message"));
        };
        let request = routing::id(&f, "request_id");

        match code {
            32 => {
                let topic = routing::string(&f, "topic");
                let opts = routing::options(&f, "options");
                let policy = match MatchPolicy::parse(routing::option(&opts, "match", "exact")) {
                    Ok(p) => p,
                    Err(_) => return routing::error(session, 32, request, "wamp.error.invalid_argument"),
                };
                let existing = self
                    .subscriptions
                    .values()
                    .find(|s| s.topic == topic && s.match_policy == policy)
                    .map(|s| s.id);
                let id = existing.unwrap_or_else(|| self.ids.next_id());
                let s = self.subscriptions.entry(id).or_insert_with(|| Subscription {
                    id,
                    topic: topic.into(),
                    match_policy: policy,
                    subscribers: BTreeSet::new(),
                });
                s.subscribers.insert(session);
                Ok(RoutedMessage::new(
                    session,
                    routing::make(
                        33,
                        BTreeMap::from([
                            ("request_id".into(), Value::Int(request)),
                            ("subscription_id".into(), Value::Int(id)),
                        ]),
                    )?,
                ))
            }
            34 => {
                let id = routing::id(&f, "subscription_id");
                let Some(sub) = self.subscriptions.get_mut(&id) else {
                    return routing::error(session, 34, request, "wamp.error.no_such_subscription");
                };

                if !sub.subscribers.remove(&session) {
                    return routing::error(session, 34, request, "wamp.error.no_such_subscription");
                }

                if sub.subscribers.is_empty() {
                    self.subscriptions.remove(&id);
                }

                Ok(RoutedMessage::new(
                    session,
                    routing::make(35, BTreeMap::from([("request_id".into(), Value::Int(request))]))?,
                ))
            }
            _ => Err(Error::new("broker: unexpected message")),
        }
    }

    pub fn receive_publish(&mut self, session: i64, publish: &Publish) -> Result<Publication, Error> {
        let sender = self
            .sessions
            .get(&session)
            .ok_or_else(|| Error::new("broker: unknown publisher"))?;
        let (_, f) = routing::fields(publish)?;
        let publication = self.ids.next_id();
        let opts = &publish.options;
        let mut events = Vec::new();

        for s in self
            .subscriptions
            .values()
            .filter(|s| s.match_policy.matches(&s.topic, &publish.topic))
        {
            let recipients = s
                .subscribers
                .iter()
                .copied()
                .filter(|id| {
                    if opts.get("exclude_me") != Some(&Value::Bool(false)) && *id == session {
                        return false;
                    }

                    if let Some(Value::List(exclude)) = opts.get("exclude")
                        && exclude
                            .iter()
                            .any(|v| crate::messages::schema::WireValue::integer(v) == Some(*id))
                    {
                        return false;
                    }

                    if let Some(Value::List(eligible)) = opts.get("eligible")
                        && !eligible
                            .iter()
                            .any(|v| crate::messages::schema::WireValue::integer(v) == Some(*id))
                    {
                        return false;
                    }

                    let Some(subscriber) = self.sessions.get(id) else {
                        return false;
                    };

                    for (excluded, eligible, identity) in [
                        ("exclude_authid", "eligible_authid", &subscriber.authid),
                        ("exclude_authrole", "eligible_authrole", &subscriber.auth_role),
                    ] {
                        let contains = |values: &[Value]| {
                            values
                                .iter()
                                .any(|value| matches!(value, Value::Str(value) if value == identity))
                        };

                        if let Some(Value::List(values)) = opts.get(excluded)
                            && contains(values)
                        {
                            return false;
                        }

                        if let Some(Value::List(values)) = opts.get(eligible)
                            && !contains(values)
                        {
                            return false;
                        }
                    }

                    true
                })
                .collect::<Vec<_>>();
            if recipients.is_empty() {
                continue;
            }

            let mut details = HashMap::new();

            if s.match_policy != MatchPolicy::Exact || self.disclose {
                details.insert("topic".into(), Value::Str(publish.topic.clone()));
            }

            if self.disclose || routing::flag(opts, "disclose_me") {
                details.insert("publisher".into(), Value::UInt(session as u64));
                details.insert("publisher_authid".into(), Value::Str(sender.authid.clone()));
                details.insert("publisher_authrole".into(), Value::Str(sender.auth_role.clone()));
            }

            let mut out = BTreeMap::from([
                ("subscription_id".into(), Value::Int(s.id)),
                ("publication_id".into(), Value::Int(publication)),
                ("details".into(), Value::Dict(details)),
            ]);
            routing::payload(&f, &mut out);
            events.push(EventDelivery {
                event: routing::make(36, out)?,
                recipients,
            });
        }

        let ack = if routing::flag(opts, "acknowledge") {
            Some(RoutedMessage::new(
                session,
                routing::make(
                    17,
                    BTreeMap::from([
                        ("request_id".into(), Value::Int(publish.request_id)),
                        ("publication_id".into(), Value::Int(publication)),
                    ]),
                )?,
            ))
        } else {
            None
        };
        Ok(Publication { events, ack })
    }
}
