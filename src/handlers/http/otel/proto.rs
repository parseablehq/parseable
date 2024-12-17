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

/// Common types used across all event types.
pub mod common {
    pub mod v1 {
        include!("opentelemetry.proto.common.v1.rs");
    }
}

/// Generated types used for logs.
pub mod logs {
    pub mod v1 {
        include!("opentelemetry.proto.logs.v1.rs");
    }
}

/// Generated types used in resources.
pub mod resource {
    pub mod v1 {
        include!("opentelemetry.proto.resource.v1.rs");
    }
}

/// Generated types used in metrics.
pub mod metrics {
    pub mod v1 {
        include!("opentelemetry.proto.metrics.v1.rs");
    }
}

/// Generated types used in traces.
pub mod trace {
    pub mod v1 {
        include!("opentelemetry.proto.trace.v1.rs");
    }
}
