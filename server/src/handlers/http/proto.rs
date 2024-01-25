

/// Common types used across all event types.
pub mod common {
    pub mod v1 {
        include!("otel/common.rs");
    }
}

/// Generated types used for logs.
pub mod logs {
    pub mod v1 {
        include!("otel/log.rs");
    }
}

/// Generated types used in resources.
pub mod resource {
    pub mod v1 {
        include!("otel/resource.rs");
    }
}
