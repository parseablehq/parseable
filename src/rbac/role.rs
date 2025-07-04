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
    CreateUserGroup,
    GetUserGroup,
    ModifyUserGroup,
    DeleteUserGroup,
    Ingest,
    Query,
    CreateStream,
    ListStream,
    GetStreamInfo,
    DetectSchema,
    GetSchema,
    GetStats,
    DeleteStream,
    GetRetention,
    PutRetention,
    PutHotTierEnabled,
    GetHotTierEnabled,
    DeleteHotTierEnabled,
    PutAlert,
    GetAlert,
    DeleteAlert,
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
    AddLLM,
    DeleteLLM,
    GetLLM,
    QueryLLM,
    ListLLM,
    ListCluster,
    ListClusterMetrics,
    DeleteNode,
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
    Login,
    Metrics,
    GetCorrelation,
    CreateCorrelation,
    DeleteCorrelation,
    PutCorrelation,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Permission {
    Unit(Action),
    Stream(Action, String),
    StreamWithTag(Action, String, Option<String>),
    Resource(Action, Option<String>, Option<String>),
    SelfUser,
}

// Currently Roles are tied to one stream
#[derive(Debug, Default)]
pub struct RoleBuilder {
    actions: Vec<Action>,
    stream: Option<String>,
    tag: Option<String>,
    resource_id: Option<String>,
    resource_type: Option<String>,
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

    pub fn with_resource(mut self, resource_id: String, resource_type: String) -> Self {
        self.resource_id = Some(resource_id);
        self.resource_type = Some(resource_type);
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
                Action::Login
                | Action::Metrics
                | Action::PutUser
                | Action::ListUser
                | Action::PutUserRoles
                | Action::GetUserRoles
                | Action::DeleteUser
                | Action::GetAbout
                | Action::PutRole
                | Action::GetRole
                | Action::DeleteRole
                | Action::ListRole
                | Action::CreateStream
                | Action::DeleteStream
                | Action::GetStreamInfo
                | Action::ListCluster
                | Action::ListClusterMetrics
                | Action::CreateCorrelation
                | Action::DeleteCorrelation
                | Action::GetCorrelation
                | Action::PutCorrelation
                | Action::DeleteNode
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
                | Action::PutAlert
                | Action::GetAlert
                | Action::DeleteAlert
                | Action::CreateUserGroup
                | Action::GetUserGroup
                | Action::DeleteUserGroup
                | Action::ModifyUserGroup
                | Action::GetAnalytics => Permission::Unit(action),
                Action::QueryLLM
                | Action::AddLLM
                | Action::DeleteLLM
                | Action::GetLLM
                | Action::ListLLM => Permission::Resource(
                    action,
                    self.resource_type.clone(),
                    self.resource_id.clone(),
                ),
                Action::Ingest
                | Action::ListStream
                | Action::GetSchema
                | Action::DetectSchema
                | Action::GetStats
                | Action::GetRetention
                | Action::PutRetention
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
        Writer {
            stream: String,
        },
        Ingestor {
            stream: String,
        },
        Reader {
            stream: String,
            tag: Option<String>,
        },
        Resource {
            resource_id: String,
            resource_type: String,
        },
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
                DefaultPrivilege::Resource {
                    resource_id,
                    resource_type,
                } => resource_perm_builder()
                    .with_resource(resource_id.clone(), resource_type.clone()),
            }
        }
    }

    fn admin_perm_builder() -> RoleBuilder {
        RoleBuilder {
            actions: vec![Action::All],
            stream: Some("*".to_string()),
            tag: None,
            resource_type: Some("*".to_string()),
            resource_id: Some("*".to_string()),
        }
    }

    fn editor_perm_builder() -> RoleBuilder {
        RoleBuilder {
            actions: vec![
                Action::Login,
                Action::Metrics,
                Action::GetAbout,
                Action::Ingest,
                Action::Query,
                Action::CreateStream,
                Action::DeleteStream,
                Action::ListStream,
                Action::GetStreamInfo,
                Action::CreateCorrelation,
                Action::DeleteCorrelation,
                Action::GetCorrelation,
                Action::PutCorrelation,
                Action::DetectSchema,
                Action::GetSchema,
                Action::GetStats,
                Action::GetRetention,
                Action::PutRetention,
                Action::PutHotTierEnabled,
                Action::GetHotTierEnabled,
                Action::DeleteHotTierEnabled,
                Action::PutAlert,
                Action::GetAlert,
                Action::DeleteAlert,
                Action::AddLLM,
                Action::DeleteLLM,
                Action::GetLLM,
                Action::QueryLLM,
                Action::ListLLM,
                Action::CreateFilter,
                Action::ListFilter,
                Action::GetFilter,
                Action::DeleteFilter,
                Action::ListDashboard,
                Action::GetDashboard,
                Action::CreateDashboard,
                Action::DeleteDashboard,
                Action::GetUserRoles,
            ],
            stream: Some("*".to_string()),
            tag: None,
            resource_id: None,
            resource_type: None,
        }
    }

    fn writer_perm_builder() -> RoleBuilder {
        RoleBuilder {
            actions: vec![
                Action::Login,
                Action::GetAbout,
                Action::Query,
                Action::ListStream,
                Action::GetSchema,
                Action::GetStats,
                Action::PutRetention,
                Action::PutAlert,
                Action::GetAlert,
                Action::DeleteAlert,
                Action::GetRetention,
                Action::PutHotTierEnabled,
                Action::GetHotTierEnabled,
                Action::DeleteHotTierEnabled,
                Action::CreateCorrelation,
                Action::DeleteCorrelation,
                Action::GetCorrelation,
                Action::PutCorrelation,
                Action::ListDashboard,
                Action::GetDashboard,
                Action::CreateDashboard,
                Action::DeleteDashboard,
                Action::Ingest,
                Action::GetLLM,
                Action::QueryLLM,
                Action::ListLLM,
                Action::GetStreamInfo,
                Action::GetFilter,
                Action::ListFilter,
                Action::CreateFilter,
                Action::DeleteFilter,
                Action::GetUserRoles,
            ],
            stream: None,
            tag: None,
            resource_id: None,
            resource_type: None,
        }
    }

    fn reader_perm_builder() -> RoleBuilder {
        RoleBuilder {
            actions: vec![
                Action::Login,
                Action::GetAbout,
                Action::Query,
                Action::ListStream,
                Action::GetSchema,
                Action::GetStats,
                Action::GetLLM,
                Action::QueryLLM,
                Action::ListLLM,
                Action::ListFilter,
                Action::GetFilter,
                Action::CreateFilter,
                Action::DeleteFilter,
                Action::CreateCorrelation,
                Action::DeleteCorrelation,
                Action::GetCorrelation,
                Action::PutCorrelation,
                Action::ListDashboard,
                Action::GetDashboard,
                Action::CreateDashboard,
                Action::DeleteDashboard,
                Action::GetRetention,
                Action::GetStreamInfo,
                Action::GetUserRoles,
                Action::GetAlert,
            ],
            stream: None,
            tag: None,
            resource_id: None,
            resource_type: None,
        }
    }

    fn resource_perm_builder() -> RoleBuilder {
        RoleBuilder {
            actions: vec![Action::GetLLM, Action::ListLLM, Action::QueryLLM],
            stream: None,
            tag: None,
            resource_id: None,
            resource_type: None,
        }
    }

    fn ingest_perm_builder() -> RoleBuilder {
        RoleBuilder {
            actions: vec![Action::Ingest],
            stream: None,
            tag: None,
            resource_id: None,
            resource_type: None,
        }
    }
}
