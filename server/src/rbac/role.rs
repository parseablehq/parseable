/*
* Parseable Server (C) 2022 - 2024 Parseable, Inc.
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License as
* published by the Free Software Foundation, either version 3 of the
* License, or (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU Affero General Public License for more details.
*
* You should have received a copy of the GNU Affero General Public License
* along with this program.  If not, see <http://www.gnu.org/licenses/>.
*
*
*/

// Represents actions that corresponds to an api
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum Action {
    Ingest,
    Query,
    CreateStream,
    ListStream,
    GetStream,
    GetSchema,
    GetStats,
    DeleteStream,
    GetRetention,
    PutRetention,
    GetCacheEnabled,
    PutCacheEnabled,
    PutHotTierEnabled,
    GetHotTierEnabled,
    DeleteHotTierEnabled,
    PutAlert,
    GetAlert,
    PutUser,
    ListUser,
    DeleteUser,
    PutUserRoles,
    GetUserRoles,
    PutRole,
    GetRole,
    DeleteRole,
    ListRole,
    GetAbout,
    QueryLLM,
    ListCluster,
    ListClusterMetrics,
    Deleteingestor,
    All,
    GetAnalytics,
    ListDashboard,
    GetDashboard,
    CreateDashboard,
    DeleteDashboard,
    ListFilter,
    GetFilter,
    CreateFilter,
    DeleteFilter,
    ListCache,
    RemoveCache,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Permission {
    Unit(Action),
    Stream(Action, String),
    StreamWithTag(Action, String, Option<String>),
    SelfUser,
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
                Action::Query => Permission::StreamWithTag(
                    action,
                    self.stream.clone().unwrap(),
                    self.tag.clone(),
                ),
                Action::PutUser
                | Action::ListUser
                | Action::PutUserRoles
                | Action::GetUserRoles
                | Action::DeleteUser
                | Action::GetAbout
                | Action::QueryLLM
                | Action::PutRole
                | Action::GetRole
                | Action::DeleteRole
                | Action::ListRole
                | Action::CreateStream
                | Action::DeleteStream
                | Action::GetStream
                | Action::ListStream
                | Action::ListCluster
                | Action::ListClusterMetrics
                | Action::Deleteingestor
                | Action::PutHotTierEnabled
                | Action::GetHotTierEnabled
                | Action::DeleteHotTierEnabled
                | Action::ListDashboard
                | Action::GetDashboard
                | Action::CreateDashboard
                | Action::DeleteDashboard
                | Action::GetFilter
                | Action::ListFilter
                | Action::CreateFilter
                | Action::DeleteFilter
                | Action::ListCache
                | Action::RemoveCache
                | Action::GetAnalytics => Permission::Unit(action),
                Action::Ingest
                | Action::GetSchema
                | Action::GetStats
                | Action::GetRetention
                | Action::PutRetention
                | Action::GetCacheEnabled
                | Action::PutCacheEnabled
                | Action::PutAlert
                | Action::GetAlert
                | Action::All => Permission::Stream(action, self.stream.clone().unwrap()),
            };
            perms.push(perm);
        }
        perms.push(Permission::SelfUser);
        perms
    }
}

// use facing model for /user/roles
// we can put same model in the backend
// user -> Vec<DefaultRoles>
pub mod model {
    use super::{Action, RoleBuilder};

    #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Hash)]
    #[serde(tag = "privilege", content = "resource", rename_all = "lowercase")]
    pub enum DefaultPrivilege {
        Admin,
        Editor,
        Writer { stream: String },
        Ingestor { stream: String },
        Reader { stream: String, tag: Option<String> },
    }

    impl From<&DefaultPrivilege> for RoleBuilder {
        fn from(value: &DefaultPrivilege) -> Self {
            match value {
                DefaultPrivilege::Admin => admin_perm_builder(),
                DefaultPrivilege::Editor => editor_perm_builder(),
                DefaultPrivilege::Writer { stream } => {
                    writer_perm_builder().with_stream(stream.to_owned())
                }
                DefaultPrivilege::Reader { stream, tag } => {
                    let mut reader = reader_perm_builder().with_stream(stream.to_owned());
                    if let Some(tag) = tag {
                        reader = reader.with_tag(tag.to_owned())
                    }
                    reader
                }
                DefaultPrivilege::Ingestor { stream } => {
                    ingest_perm_builder().with_stream(stream.to_owned())
                }
            }
        }
    }

    fn admin_perm_builder() -> RoleBuilder {
        RoleBuilder {
            actions: vec![Action::All],
            stream: Some("*".to_string()),
            tag: None,
        }
    }

    fn editor_perm_builder() -> RoleBuilder {
        RoleBuilder {
            actions: vec![
                Action::Ingest,
                Action::Query,
                Action::CreateStream,
                Action::ListStream,
                Action::GetStream,
                Action::GetSchema,
                Action::GetStats,
                Action::GetRetention,
                Action::PutRetention,
                Action::PutCacheEnabled,
                Action::GetCacheEnabled,
                Action::PutHotTierEnabled,
                Action::GetHotTierEnabled,
                Action::DeleteHotTierEnabled,
                Action::PutAlert,
                Action::GetAlert,
                Action::GetAbout,
                Action::QueryLLM,
            ],
            stream: Some("*".to_string()),
            tag: None,
        }
    }

    fn writer_perm_builder() -> RoleBuilder {
        RoleBuilder {
            actions: vec![
                Action::Ingest,
                Action::Query,
                Action::ListStream,
                Action::GetStream,
                Action::GetSchema,
                Action::GetStats,
                Action::GetRetention,
                Action::PutAlert,
                Action::GetAlert,
                Action::GetAbout,
                Action::QueryLLM,
            ],
            stream: None,
            tag: None,
        }
    }

    fn reader_perm_builder() -> RoleBuilder {
        RoleBuilder {
            actions: vec![
                Action::Query,
                Action::ListStream,
                Action::GetStream,
                Action::GetSchema,
                Action::GetStats,
                Action::GetRetention,
                Action::GetAlert,
                Action::GetAbout,
                Action::QueryLLM,
                Action::ListCluster,
                Action::GetHotTierEnabled,
            ],
            stream: None,
            tag: None,
        }
    }

    fn ingest_perm_builder() -> RoleBuilder {
        RoleBuilder {
            actions: vec![Action::Ingest],
            stream: None,
            tag: None,
        }
    }
}
