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
 */

use http::StatusCode;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DetailedError {
    pub operation: String,
    pub message: String,
    pub stream_name: Option<String>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub metadata: Option<std::collections::HashMap<String, String>>,
    pub status_code: u16,
}

impl DetailedError {
    pub fn status_code(&self) -> StatusCode {
        StatusCode::from_u16(self.status_code).unwrap()
    }
}
