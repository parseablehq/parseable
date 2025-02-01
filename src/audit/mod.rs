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

mod builder;
mod logger;
mod types;

pub use builder::AuditLogBuilder;
pub use logger::AuditLogger;
pub use types::*;

use once_cell::sync::OnceCell;
use tokio::sync::mpsc::Sender;

// Shared audit logger instance to batch and send audit logs
static AUDIT_LOG_TX: OnceCell<Sender<AuditLog>> = OnceCell::new();
