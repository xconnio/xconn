#[derive(Clone, Debug)]
pub struct SessionDetails {
    pub id: i64,
    pub realm: String,
    pub authid: String,
    pub auth_role: String,
    pub static_serializer: bool,
    pub auth_method: String,
    pub auth_extra: std::collections::HashMap<String, crate::messages::types::Value>,
    pub roles: std::collections::HashMap<String, crate::messages::types::Value>,
    pub created_at: String,
}

impl SessionDetails {
    pub fn new(id: i64, realm: String, authid: String, auth_role: String, static_serializer: bool) -> Self {
        SessionDetails {
            id,
            realm,
            authid,
            auth_role,
            static_serializer,
            auth_method: String::new(),
            auth_extra: Default::default(),
            roles: Default::default(),
            created_at: String::new(),
        }
    }
}

impl SessionDetails {
    /// The host supplies creation time so constructing metadata needs no clock.
    pub fn with_authentication(
        mut self,
        method: String,
        extra: std::collections::HashMap<String, crate::messages::types::Value>,
        roles: std::collections::HashMap<String, crate::messages::types::Value>,
        created_at: String,
    ) -> Self {
        self.auth_method = method;
        self.auth_extra = extra;
        self.roles = roles;
        self.created_at = created_at;
        self
    }
}
