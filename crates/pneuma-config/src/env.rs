//! What a configuration failure is.
//!
//! Split out of `lib.rs` so that file stays declarations-only, which is what
//! the workspace coverage gate assumes when it excludes it. The reading itself
//! lives in [`crate::source`] -- this module held free functions over
//! `std::env` until they were replaced, because a free function over a process
//! global is testable only by mutating that global.

/// Why reading a configuration value failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConfigError {
    /// The variable is not set.
    #[error("{key} is required")]
    Missing {
        /// The variable that was looked up.
        key: String,
    },
    /// The variable is set but is not valid UTF-8.
    ///
    /// Distinguished from [`ConfigError::Missing`] on purpose — see the module
    /// docs.
    #[error("{key} is set but is not valid UTF-8")]
    NotUnicode {
        /// The variable that was looked up.
        key: String,
    },
    /// The variable is set but is blank, where a value is required.
    #[error("{key} is required and must not be empty")]
    Empty {
        /// The variable that was looked up.
        key: String,
    },
    /// The variable is set but could not be parsed as the requested type.
    #[error("{key} must be a valid {expected}")]
    Invalid {
        /// The variable that was looked up.
        key: String,
        /// The Rust type name that was being parsed.
        expected: &'static str,
    },
}

/// The bare type name an operator should see, without its module path.
///
/// `std::any::type_name` returns the fully-qualified internal path, so an
/// operator reading a startup failure would be told `BIND_ADDR must be a valid
/// core::net::socket_addr::SocketAddr`. `u16`, `u64` and `bool` are the types
/// most often parsed and the two forms coincide for all three, so this wart is
/// invisible unless pinned deliberately -- [`crate::source`] does, with `u32`.
pub(crate) fn expected_type<T>() -> &'static str {
    let full = std::any::type_name::<T>();
    match full.rsplit_once("::") {
        Some((_, bare)) => bare,
        None => full,
    }
}
