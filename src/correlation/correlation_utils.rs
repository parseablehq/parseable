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
 */

use itertools::Itertools;

use crate::rbac::{
    map::SessionKey,
    role::{Action, Permission},
    Users,
};

use super::{CorrelationError, TableConfig};

pub async fn user_auth_for_query(
    session_key: &SessionKey,
    table_configs: &[TableConfig],
) -> Result<(), CorrelationError> {
    let tables = table_configs.iter().map(|t| &t.table_name).collect_vec();
    let permissions = Users.get_permissions(session_key);

    for table_name in tables {
        let mut authorized = false;

        // in permission check if user can run query on the stream.
        // also while iterating add any filter tags for this stream
        for permission in permissions.iter() {
            match permission {
                Permission::Stream(Action::All, _) => {
                    authorized = true;
                    break;
                }
                Permission::StreamWithTag(Action::Query, ref stream, _)
                    if stream == table_name || stream == "*" =>
                {
                    authorized = true;
                }
                _ => (),
            }
        }

        if !authorized {
            return Err(CorrelationError::Unauthorized);
        }
    }

    Ok(())
}
