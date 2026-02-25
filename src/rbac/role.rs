/*
* Parseable Server (C) 2022 - 2025 Parseable, Inc.
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
    SuperAdmin,
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum ParseableResourceType {
    #[serde(rename = "stream")]
    Stream(String),
    #[serde(rename = "llmKey")]
    Llm(String),
    #[serde(rename = "all")]
    All,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Permission {
    Unit(Action),
    Resource(Action, Option<ParseableResourceType>),
    SelfUser,
}

// Currently Roles are tied to one stream
#[derive(Debug, Default)]
pub struct RoleBuilder {
    actions: Vec<Action>,
    resource_type: Option<ParseableResourceType>,
}

// R x P
impl RoleBuilder {
    pub fn with_resource(mut self, resource_type: ParseableResourceType) -> Self {
        self.resource_type = Some(resource_type);
        self
    }

    pub fn build(self) -> Vec<Permission> {
        let mut perms = Vec::new();
        for action in self.actions {
            let perm = match action {
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
                Action::Query
                | Action::QueryLLM
                | Action::AddLLM
                | Action::DeleteLLM
                | Action::GetLLM
                | Action::ListLLM
                | Action::Ingest
                | Action::ListStream
                | Action::GetSchema
                | Action::DetectSchema
                | Action::GetStats
                | Action::GetRetention
                | Action::PutRetention
                | Action::All
                | Action::SuperAdmin => Permission::Resource(action, self.resource_type.clone()),
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
    use serde::{Deserialize, Serialize, Serializer, de};
    use serde_json::Value;

    use crate::rbac::role::ParseableResourceType;

    use super::{Action, RoleBuilder};

    #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Hash)]
    #[serde(tag = "privilege", rename_all = "lowercase")]
    pub enum DefaultPrivilege {
        SuperAdmin,
        Admin,
        Editor,
        Writer {
            resource: ParseableResourceType,
        },
        Ingestor {
            resource: Option<ParseableResourceType>,
        },
        Reader {
            resource: Option<ParseableResourceType>,
        },
    }

    #[derive(Debug, Clone)]
    pub struct RoleUI(pub Role);
    impl Serialize for RoleUI {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            self.0.actions.serialize(serializer)
        }
    }

    #[derive(Debug, Default, serde::Serialize, serde::Deserialize, Clone, PartialEq, Eq)]
    #[serde(rename_all = "camelCase")]
    pub enum RoleType {
        #[default]
        User,
        Internal,
    }

    #[derive(Debug, serde::Serialize, Clone, PartialEq, Eq, Default)]
    #[serde(rename_all = "camelCase")]
    pub struct Role {
        actions: Vec<DefaultPrivilege>,
        role_type: RoleType,
    }

    impl Role {
        pub fn create_user_role(actions: Vec<DefaultPrivilege>) -> Self {
            Self {
                actions,
                role_type: RoleType::User,
            }
        }

        pub fn create_internal_role(actions: Vec<DefaultPrivilege>) -> Self {
            Self {
                actions,
                role_type: RoleType::Internal,
            }
        }

        pub fn privileges(&self) -> &Vec<DefaultPrivilege> {
            &self.actions
        }

        pub fn role_type(&self) -> &RoleType {
            &self.role_type
        }

        pub fn deny_super_admin(&self) -> bool {
            self.actions
                .iter()
                .any(|p| p.eq(&DefaultPrivilege::SuperAdmin))
        }
    }

    impl<'de> Deserialize<'de> for Role {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            let value = Value::deserialize(deserializer)?;

            match value {
                Value::Array(_) => {
                    let actions =
                        Vec::<DefaultPrivilege>::deserialize(value).map_err(de::Error::custom)?;
                    Ok(Role {
                        actions,
                        role_type: RoleType::User,
                    })
                }
                Value::Object(ref obj) => {
                    let actions = obj
                        .get("actions")
                        .ok_or_else(|| de::Error::missing_field("actions"))?;
                    let actions =
                        Vec::<DefaultPrivilege>::deserialize(actions).map_err(de::Error::custom)?;

                    let role_type = obj
                        .get("roleType")
                        .ok_or_else(|| de::Error::missing_field("roleType"))?;
                    let role_type = RoleType::deserialize(role_type).map_err(de::Error::custom)?;
                    Ok(Role { actions, role_type })
                }
                _ => Err(de::Error::custom("expected an array or an object")),
            }
        }
    }

    impl From<&DefaultPrivilege> for RoleBuilder {
        fn from(value: &DefaultPrivilege) -> Self {
            match value {
                DefaultPrivilege::SuperAdmin => super_admin_perm_builder(),
                DefaultPrivilege::Admin => admin_perm_builder(),
                DefaultPrivilege::Editor => editor_perm_builder(),
                DefaultPrivilege::Writer { resource } => {
                    writer_perm_builder().with_resource(resource.to_owned())
                }
                DefaultPrivilege::Reader { resource } => {
                    if let Some(resource) = resource.as_ref() {
                        reader_perm_builder().with_resource(resource.to_owned())
                    } else {
                        reader_perm_builder()
                    }
                }
                DefaultPrivilege::Ingestor { resource } => {
                    if let Some(resource) = resource.as_ref() {
                        ingest_perm_builder().with_resource(resource.to_owned())
                    } else {
                        ingest_perm_builder()
                    }
                }
            }
        }
    }

    fn super_admin_perm_builder() -> RoleBuilder {
        RoleBuilder {
            actions: vec![Action::SuperAdmin, Action::All],
            resource_type: Some(ParseableResourceType::All),
        }
    }

    fn admin_perm_builder() -> RoleBuilder {
        RoleBuilder {
            actions: vec![Action::All],
            resource_type: Some(ParseableResourceType::All),
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
            resource_type: Some(ParseableResourceType::All),
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
            resource_type: None,
        }
    }

    fn ingest_perm_builder() -> RoleBuilder {
        RoleBuilder {
            actions: vec![Action::Ingest],
            resource_type: None,
        }
    }
}
