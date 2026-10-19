//! Sans-I/O RPC registration, dispatch, progress, and cancellation.
use crate::{
    idgen::SessionScopeIDGenerator,
    messages::{
        cancel::Cancel,
        message::Message,
        types::{Error, Value},
    },
    routing::{self, MatchPolicy, RoutedMessage},
    types::SessionDetails,
};
use std::collections::{BTreeMap, HashMap};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InvocationPolicy {
    Single,
    First,
    Last,
    RoundRobin,
    Random,
}

impl InvocationPolicy {
    pub fn parse(s: &str) -> Result<Self, Error> {
        match s {
            "" | "single" => Ok(Self::Single),
            "first" => Ok(Self::First),
            "last" => Ok(Self::Last),
            "roundrobin" => Ok(Self::RoundRobin),
            "random" => Ok(Self::Random),
            _ => Err(Error::new("invalid invocation policy")),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Registration {
    pub id: i64,
    pub procedure: String,
    pub match_policy: MatchPolicy,
    pub invocation_policy: InvocationPolicy,
    pub callees: Vec<i64>,
    next: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CancelMode {
    Skip,
    Kill,
    KillNoWait,
}

#[derive(Clone, Debug)]
pub struct PendingInvocation {
    pub request_id: i64,
    pub caller_id: i64,
    pub callee_id: i64,
    pub registration_id: i64,
    pub progress: bool,
    pub receive_progress: bool,
    pub cancel_mode: Option<CancelMode>,
    procedure: String,
}

#[derive(Default)]
struct CallerRequests {
    highest: i64,
    progressive: bool,
}

#[derive(Default)]
pub struct Dealer {
    sessions: HashMap<i64, SessionDetails>,
    registrations: BTreeMap<i64, Registration>,
    pending: HashMap<i64, PendingInvocation>,
    calls: HashMap<(i64, i64), i64>,
    // Once a caller uses progressive input, older inactive IDs are continuations.
    // One watermark per session avoids unbounded or expiring call tombstones.
    caller_requests: HashMap<i64, CallerRequests>,
    ids: SessionScopeIDGenerator,
    disclose: bool,
}

impl Dealer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_session(&mut self, details: SessionDetails) -> Result<(), Error> {
        if self.sessions.contains_key(&details.id) {
            return Err(Error::new("dealer: session already exists"));
        };
        self.sessions.insert(details.id, details);
        Ok(())
    }

    /// Returns errors/interrupts for affected peers so the host can send them.
    pub fn remove_session(&mut self, session: i64) -> Result<Vec<RoutedMessage>, Error> {
        if self.sessions.remove(&session).is_none() {
            return Err(Error::new("dealer: unknown session"));
        }

        self.caller_requests.remove(&session);
        self.registrations.retain(|_, r| {
            r.callees.retain(|id| *id != session);
            r.next = 0;
            !r.callees.is_empty()
        });
        let affected: Vec<_> = self
            .pending
            .iter()
            .filter(|(_, p)| p.caller_id == session || p.callee_id == session)
            .map(|(id, _)| *id)
            .collect();
        let mut out = Vec::new();

        for id in affected {
            let p = self.finish(id).unwrap();

            if p.caller_id == session && self.sessions.contains_key(&p.callee_id) {
                out.push(self.interrupt(id, p.callee_id, "killnowait")?);
            } else if p.callee_id == session
                && self.sessions.contains_key(&p.caller_id)
                && !matches!(p.cancel_mode, Some(CancelMode::Skip | CancelMode::KillNoWait))
            {
                out.push(routing::error(p.caller_id, 48, p.request_id, "wamp.error.canceled")?);
            }
        }

        Ok(out)
    }

    pub fn has_procedure(&self, procedure: &str) -> bool {
        self.registrations.values().any(|r| r.procedure == procedure)
    }

    pub fn auto_disclose_caller(&mut self, disclose: bool) {
        self.disclose = disclose;
    }

    pub fn registrations(&self) -> impl Iterator<Item = &Registration> {
        self.registrations.values()
    }

    pub fn pending_invocations(&self) -> impl Iterator<Item = (&i64, &PendingInvocation)> {
        self.pending.iter()
    }

    fn finish(&mut self, id: i64) -> Option<PendingInvocation> {
        let p = self.pending.remove(&id)?;
        let key = (p.caller_id, p.request_id);

        if self.calls.get(&key) == Some(&id) {
            self.calls.remove(&key);
        }

        Some(p)
    }

    fn interrupt(&self, id: i64, callee: i64, mode: &str) -> Result<RoutedMessage, Error> {
        Ok(RoutedMessage::new(
            callee,
            routing::make(
                69,
                BTreeMap::from([
                    ("request_id".into(), Value::Int(id)),
                    (
                        "options".into(),
                        Value::Dict(HashMap::from([
                            ("mode".into(), Value::Str(mode.into())),
                            ("reason".into(), Value::Str("wamp.error.canceled".into())),
                        ])),
                    ),
                ]),
            )?,
        ))
    }

    fn matching(&self, procedure: &str) -> Option<i64> {
        self.registrations
            .values()
            .filter(|r| r.match_policy.matches(&r.procedure, procedure))
            .min_by_key(|r| match r.match_policy {
                MatchPolicy::Exact => (0, 0, r.id),
                MatchPolicy::Prefix => (1, usize::MAX - r.procedure.len(), r.id),
                MatchPolicy::Wildcard => (2, 0, r.id),
            })
            .map(|r| r.id)
    }

    pub fn receive_message(&mut self, session: i64, message: &dyn Message) -> Result<Option<RoutedMessage>, Error> {
        if !self.sessions.contains_key(&session) {
            return Err(Error::new("dealer: unknown session"));
        }

        let (code, f) = routing::fields(message)?;

        if !matches!(code, 48 | 64 | 66 | 70 | 8) {
            return Err(Error::new("dealer: unexpected message"));
        }

        let request = routing::id(&f, "request_id");

        match code {
            64 => {
                let procedure = routing::string(&f, "procedure");
                let opts = routing::options(&f, "options");
                let Ok(policy) = InvocationPolicy::parse(routing::option(&opts, "invoke", "single")) else {
                    return routing::error(session, 64, request, "wamp.error.invalid_argument").map(Some);
                };
                let Ok(matching) = MatchPolicy::parse(routing::option(&opts, "match", "exact")) else {
                    return routing::error(session, 64, request, "wamp.error.invalid_argument").map(Some);
                };
                let existing = self
                    .registrations
                    .values()
                    .find(|r| r.procedure == procedure && r.match_policy == matching)
                    .map(|r| r.id);
                let id = existing.unwrap_or_else(|| self.ids.next_id());

                if let Some(r) = self.registrations.get_mut(&id) {
                    if r.invocation_policy == InvocationPolicy::Single
                        || r.invocation_policy != policy
                        || r.callees.contains(&session)
                    {
                        return routing::error(session, 64, request, "wamp.error.procedure_already_exists").map(Some);
                    };
                    r.callees.push(session);
                } else {
                    self.registrations.insert(
                        id,
                        Registration {
                            id,
                            procedure: procedure.into(),
                            match_policy: matching,
                            invocation_policy: policy,
                            callees: vec![session],
                            next: 0,
                        },
                    );
                }

                Ok(Some(RoutedMessage::new(
                    session,
                    routing::make(
                        65,
                        BTreeMap::from([
                            ("request_id".into(), Value::Int(request)),
                            ("registration_id".into(), Value::Int(id)),
                        ]),
                    )?,
                )))
            }
            66 => {
                let id = routing::id(&f, "registration_id");
                let Some(r) = self.registrations.get_mut(&id) else {
                    return routing::error(session, 66, request, "wamp.error.no_such_registration").map(Some);
                };

                if !r.callees.contains(&session) {
                    return routing::error(session, 66, request, "wamp.error.no_such_registration").map(Some);
                }

                r.callees.retain(|id| *id != session);
                r.next = 0;

                if r.callees.is_empty() {
                    self.registrations.remove(&id);
                }

                Ok(Some(RoutedMessage::new(
                    session,
                    routing::make(67, BTreeMap::from([("request_id".into(), Value::Int(request))]))?,
                )))
            }
            48 => {
                let procedure = routing::string(&f, "procedure");
                let opts = routing::options(&f, "options");
                let progress = routing::flag(&opts, "progress");
                let (invocation, p) = if let Some(id) = self.calls.get(&(session, request)).copied() {
                    let p = self.pending.get_mut(&id).unwrap();

                    if !p.progress || p.procedure != procedure || p.cancel_mode.is_some() {
                        return routing::error(session, 48, request, "wamp.error.invalid_argument").map(Some);
                    }

                    p.progress = progress;
                    (id, p.clone())
                } else {
                    let caller = self.caller_requests.entry(session).or_default();
                    if caller.progressive && request <= caller.highest {
                        return Ok(None);
                    }
                    caller.highest = caller.highest.max(request);
                    caller.progressive |= progress;

                    let Some(id) = self.matching(procedure) else {
                        return routing::error(session, 48, request, "wamp.error.no_such_procedure").map(Some);
                    };
                    let r = self.registrations.get_mut(&id).unwrap();
                    let index = match r.invocation_policy {
                        InvocationPolicy::Single | InvocationPolicy::First => 0,
                        InvocationPolicy::Last => r.callees.len() - 1,
                        InvocationPolicy::RoundRobin => {
                            let n = r.next % r.callees.len();
                            r.next = (n + 1) % r.callees.len();
                            n
                        }
                        InvocationPolicy::Random => random_index(r.callees.len())?,
                    };
                    let invocation = self.ids.next_id();
                    let p = PendingInvocation {
                        request_id: request,
                        caller_id: session,
                        callee_id: r.callees[index],
                        registration_id: id,
                        progress,
                        receive_progress: routing::flag(&opts, "receive_progress"),
                        cancel_mode: None,
                        procedure: procedure.into(),
                    };
                    self.calls.insert((session, request), invocation);
                    self.pending.insert(invocation, p.clone());
                    (invocation, p)
                };
                let mut details = HashMap::new();

                if p.receive_progress {
                    details.insert("receive_progress".into(), Value::Bool(true));
                }

                if progress {
                    details.insert("progress".into(), Value::Bool(true));
                }

                let reg = self.registrations.get(&p.registration_id);

                if self.disclose || reg.is_some_and(|r| r.match_policy != MatchPolicy::Exact) {
                    details.insert("procedure".into(), Value::Str(procedure.into()));
                }

                if self.disclose || routing::flag(&opts, "disclose_me") {
                    let caller = &self.sessions[&session];
                    details.insert("caller".into(), Value::UInt(session as u64));
                    details.insert("caller_authid".into(), Value::Str(caller.authid.clone()));
                    details.insert("caller_authrole".into(), Value::Str(caller.auth_role.clone()));
                }

                let mut out = BTreeMap::from([
                    ("request_id".into(), Value::Int(invocation)),
                    ("registration_id".into(), Value::Int(p.registration_id)),
                    ("details".into(), Value::Dict(details)),
                ]);
                routing::payload(&f, &mut out);
                Ok(Some(RoutedMessage::new(
                    p.callee_id,
                    routing::forward(68, out, message, self.sessions[&p.callee_id].static_serializer)?,
                )))
            }
            70 | 8 => {
                if code == 8 && routing::id(&f, "message_type") != 68 {
                    return Err(Error::new("dealer: expected invocation error"));
                }
                // Replies can race with caller departure or cancellation. The
                // invocation is already finished; discard its late response.
                let Some(p) = self.pending.get(&request).cloned() else {
                    return Ok(None);
                };

                if p.callee_id != session {
                    return Err(Error::new("dealer: invocation belongs to another callee"));
                }

                let progress = code == 70 && routing::flag(&routing::options(&f, "options"), "progress");

                if matches!(p.cancel_mode, Some(CancelMode::Skip | CancelMode::KillNoWait)) {
                    if !progress {
                        self.finish(request);
                    }

                    return Ok(None);
                }

                if p.cancel_mode == Some(CancelMode::Kill) {
                    self.finish(request);
                    return routing::error(p.caller_id, 48, p.request_id, "wamp.error.canceled").map(Some);
                }

                if progress && !p.receive_progress {
                    return Err(Error::new("dealer: progressive result was not requested"));
                }

                let mut out = BTreeMap::from([("request_id".into(), Value::Int(p.request_id))]);
                routing::payload(&f, &mut out);
                let result = if code == 8 {
                    out.insert("message_type".into(), Value::Int(48));
                    out.insert("details".into(), f["details"].clone());
                    out.insert("uri".into(), f["uri"].clone());
                    routing::make(8, out)?
                } else {
                    let mut details = HashMap::new();

                    if progress {
                        details.insert("progress".into(), Value::Bool(true));
                    }

                    out.insert("details".into(), Value::Dict(details));
                    routing::forward(50, out, message, self.sessions[&p.caller_id].static_serializer)?
                };

                if !progress {
                    self.finish(request);
                }

                Ok(Some(RoutedMessage::new(p.caller_id, result)))
            }
            _ => unreachable!(),
        }
    }

    pub fn receive_cancel(&mut self, session: i64, cancel: &Cancel) -> Result<Vec<RoutedMessage>, Error> {
        if !self.sessions.contains_key(&session) {
            return Err(Error::new("dealer: unknown session"));
        }

        routing::fields(cancel)?;
        let (mode, name) = match routing::option(&cancel.options, "mode", "killnowait") {
            "skip" => (CancelMode::Skip, "skip"),
            "kill" => (CancelMode::Kill, "kill"),
            "" | "killnowait" => (CancelMode::KillNoWait, "killnowait"),
            _ => {
                return Ok(vec![routing::error(
                    session,
                    48,
                    cancel.request_id,
                    "wamp.error.invalid_argument",
                )?]);
            }
        };
        let Some(id) = self.calls.get(&(session, cancel.request_id)).copied() else {
            return Ok(vec![routing::error(
                session,
                48,
                cancel.request_id,
                "wamp.error.invalid_argument",
            )?]);
        };
        let p = self.pending.get_mut(&id).unwrap();

        if p.cancel_mode.is_some() {
            return Ok(vec![]);
        }

        p.cancel_mode = Some(mode);
        let callee = p.callee_id;
        let mut out = Vec::new();

        if mode != CancelMode::Skip {
            out.push(self.interrupt(id, callee, name)?);
        }

        if mode != CancelMode::Kill {
            out.push(routing::error(session, 48, cancel.request_id, "wamp.error.canceled")?);
            // The caller's request is complete, even while a late callee reply is pending.
            self.calls.remove(&(session, cancel.request_id));
        }

        Ok(out)
    }
}

fn random_index(length: usize) -> Result<usize, Error> {
    use rand_core::RngCore;
    let length = length as u64;
    let limit = u64::MAX - u64::MAX % length;

    loop {
        let mut bytes = [0; 8];
        rand_core::OsRng
            .try_fill_bytes(&mut bytes)
            .map_err(|e| Error::new(format!("failed to select callee: {e}")))?;
        let value = u64::from_le_bytes(bytes);

        if value < limit {
            return Ok((value % length) as usize);
        }
    }
}
