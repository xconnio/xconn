//! WAMP option, matching, invocation, cancellation, and feature names.

pub const OPTION_RECEIVE_PROGRESS: &str = "receive_progress";
pub const OPTION_PROGRESS: &str = "progress";
pub const OPTION_MATCH: &str = "match";
pub const OPTION_INVOKE: &str = "invoke";
pub const OPTION_MODE: &str = "mode";
pub const OPTION_REASON: &str = "reason";
pub const CANCEL_MODE_KILL: &str = "kill";
pub const CANCEL_MODE_KILL_NO_WAIT: &str = "killnowait";
pub const CANCEL_MODE_SKIP: &str = "skip";
pub const MATCH_EXACT: &str = "exact";
pub const MATCH_PREFIX: &str = "prefix";
pub const MATCH_WILDCARD: &str = "wildcard";
pub const INVOKE_SINGLE: &str = "single";
pub const INVOKE_FIRST: &str = "first";
pub const INVOKE_LAST: &str = "last";
pub const INVOKE_ROUND_ROBIN: &str = "roundrobin";
pub const INVOKE_RANDOM: &str = "random";
pub const FEATURE_PROGRESSIVE_CALL_INVOCATIONS: &str = "progressive_call_invocations";
pub const FEATURE_PROGRESSIVE_CALL_RESULTS: &str = "progressive_call_results";
pub const FEATURE_CALL_CANCELING: &str = "call_canceling";
pub const FEATURE_PUBLISHER_EXCLUSION: &str = "publisher_exclusion";
pub const OPTION_ACKNOWLEDGE: &str = "acknowledge";
