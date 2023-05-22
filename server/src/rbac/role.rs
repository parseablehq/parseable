// Represents actions that corresponds to an api
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum Action {
    Ingest,
    Query,
    CreateStream,
    ListStream,
    GetSchema,
    GetStats,
    DeleteStream,
    GetRetention,
    PutRetention,
    PutAlert,
    GetAlert,
    PutUser,
    DeleteUser,
    PutRoles,
    All,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Permission {
    Unit(Action),
    Stream(Action, String),
}

// Currently Roles are tied to one stream
#[derive(Debug, Default)]
pub struct RoleBuilder {
    actions: Vec<Action>,
    stream: Option<String>,
    tag: Option<String>,
}

// R x P
impl RoleBuilder {
    pub fn with_stream(mut self, stream: String) -> Self {
        self.stream = Some(stream);
        self
    }

    pub fn with_tag(mut self, tag: String) -> Self {
        self.tag = Some(tag);
        self
    }

    pub fn build(self) -> Vec<Permission> {
        let mut perms = Vec::new();
        for action in self.actions {
            let perm = match action {
                Action::Ingest => Permission::Stream(action, self.stream.clone().unwrap()),
                Action::Query => Permission::Stream(action, self.stream.clone().unwrap()),
                Action::CreateStream => Permission::Unit(action),
                Action::ListStream => Permission::Unit(action),
                Action::GetSchema => Permission::Stream(action, self.stream.clone().unwrap()),
                Action::GetStats => Permission::Stream(action, self.stream.clone().unwrap()),
                Action::DeleteStream => Permission::Stream(action, self.stream.clone().unwrap()),
                Action::GetRetention => Permission::Stream(action, self.stream.clone().unwrap()),
                Action::PutRetention => Permission::Stream(action, self.stream.clone().unwrap()),
                Action::PutAlert => Permission::Stream(action, self.stream.clone().unwrap()),
                Action::GetAlert => Permission::Stream(action, self.stream.clone().unwrap()),
                Action::PutUser => Permission::Unit(action),
                Action::PutRoles => Permission::Unit(action),
                Action::DeleteUser => Permission::Unit(action),
                Action::All => Permission::Stream(action, self.stream.clone().unwrap()),
            };
            perms.push(perm);
        }
        perms
    }
}

// use facing model for /user/roles
// we can put same model in the backend
// user -> Vec<DefaultRoles>
pub mod model {
    use super::{Action, RoleBuilder};

    #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    #[serde(tag = "role", content = "resource", rename_all = "lowercase")]
    pub enum DefaultRole {
        Admin,
        Editor,
        Writer { stream: String },
        Reader { stream: String, tag: String },
    }

    impl From<&DefaultRole> for RoleBuilder {
        fn from(value: &DefaultRole) -> Self {
            match value {
                DefaultRole::Admin => admin_role_builder(),
                DefaultRole::Editor => editor_role_builder(),
                DefaultRole::Writer { stream } => {
                    writer_role_builder().with_stream(stream.to_owned())
                }
                DefaultRole::Reader { stream, tag } => reader_role_builder()
                    .with_stream(stream.to_owned())
                    .with_tag(tag.to_owned()),
            }
        }
    }

    fn admin_role_builder() -> RoleBuilder {
        RoleBuilder {
            actions: vec![Action::All],
            stream: Some("*".to_string()),
            tag: None,
        }
    }

    fn editor_role_builder() -> RoleBuilder {
        RoleBuilder {
            actions: vec![
                Action::Ingest,
                Action::Query,
                Action::CreateStream,
                Action::ListStream,
                Action::GetSchema,
                Action::GetStats,
                Action::GetRetention,
                Action::PutRetention,
                Action::PutAlert,
                Action::GetAlert,
            ],
            stream: Some("*".to_string()),
            tag: None,
        }
    }

    fn writer_role_builder() -> RoleBuilder {
        RoleBuilder {
            actions: vec![
                Action::Ingest,
                Action::Query,
                Action::GetSchema,
                Action::GetStats,
                Action::GetRetention,
                Action::PutAlert,
                Action::GetAlert,
            ],
            stream: None,
            tag: None,
        }
    }

    fn reader_role_builder() -> RoleBuilder {
        RoleBuilder {
            actions: vec![
                Action::Query,
                Action::GetSchema,
                Action::GetStats,
                Action::GetRetention,
                Action::GetAlert,
            ],
            stream: None,
            tag: None,
        }
    }
}
