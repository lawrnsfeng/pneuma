//! Newtype identifiers used throughout the domain model.
//!
//! Every id is a thin wrapper over [`compact_str::CompactString`] — cheap to
//! clone, inline-stored for short strings, and `serde`-transparent so the wire
//! format is a bare JSON string, matching the original exactly.
//!
//! `RunId` and `JobId` are kept as **distinct** types even though a run's id
//! and its owning job's id are always equal today — see [`RunId::from_job`],
//! the one place that assumption lives. If multi-run jobs are ever needed,
//! the compiler enumerates every call site that assumed otherwise.

use std::fmt;

use compact_str::CompactString;
use serde::{Deserialize, Serialize};

macro_rules! id_newtype {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(CompactString);

        impl $name {
            /// Constructs a new id from anything convertible into a
            /// [`CompactString`] (`&str`, `String`, ...).
            pub fn new(s: impl Into<CompactString>) -> Self {
                Self(s.into())
            }

            /// Borrows the id as a plain string slice.
            pub fn as_str(&self) -> &str {
                self.0.as_str()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.0.as_str())
            }
        }

        impl From<&str> for $name {
            fn from(s: &str) -> Self {
                Self::new(s)
            }
        }

        impl From<String> for $name {
            fn from(s: String) -> Self {
                Self::new(s)
            }
        }
    };
}

id_newtype!(
    /// Identifies a single node within a pipeline definition, unique within
    /// that pipeline (globally unique across a resolved, flattened graph).
    NodeId
);
id_newtype!(
    /// Identifies a pipeline definition.
    PipelineId
);
id_newtype!(
    /// Identifies a single run (one execution of a pipeline).
    RunId
);
id_newtype!(
    /// Identifies the job a run belongs to. Distinct from [`RunId`] on
    /// purpose — see the module-level docs.
    JobId
);
id_newtype!(
    /// Identifies a tenant for routing and fairness purposes.
    TenantId
);
id_newtype!(
    /// A broker subject/topic/queue name.
    SubjectName
);

impl RunId {
    /// Derives a run id from a job id. Today a run's id and its owning job's
    /// id are always equal — this is the one place that assumption lives.
    pub fn from_job(job: &JobId) -> Self {
        RunId::new(job.as_str())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    /// Every macro-generated method (`new`, `as_str`, `Display`, both `From`
    /// impls) is separately-generated code per type, so each type needs its
    /// own exercise of every method to reach 100% line coverage — not just
    /// one type standing in for the rest.
    macro_rules! test_id_newtype {
        ($mod_name:ident, $ty:ident) => {
            mod $mod_name {
                use super::*;

                #[test]
                fn new_from_str_and_string_agree() {
                    let from_str = $ty::new("abc");
                    let from_string = $ty::new(String::from("abc"));
                    assert_eq!(from_str, from_string);
                    assert_eq!(from_str.as_str(), "abc");
                }

                #[test]
                fn display_matches_as_str() {
                    let id = $ty::new("hello");
                    assert_eq!(id.to_string(), "hello");
                    assert_eq!(id.to_string(), id.as_str());
                }

                #[test]
                fn from_str_ref() {
                    let id: $ty = "x".into();
                    assert_eq!(id.as_str(), "x");
                }

                #[test]
                fn from_owned_string() {
                    let id: $ty = String::from("y").into();
                    assert_eq!(id.as_str(), "y");
                }

                #[test]
                fn eq_and_hash_agree_for_equal_content() {
                    let a = $ty::new("dup");
                    let b = $ty::new("dup");
                    assert_eq!(a, b);
                    let mut set = HashSet::new();
                    set.insert(a);
                    assert!(set.contains(&b));
                }

                #[test]
                fn ord_sorts_lexicographically() {
                    let mut v = vec![$ty::new("b"), $ty::new("a"), $ty::new("c")];
                    v.sort();
                    assert_eq!(v, vec![$ty::new("a"), $ty::new("b"), $ty::new("c")]);
                }

                #[test]
                fn serde_round_trips() -> Result<(), serde_json::Error> {
                    let id = $ty::new("round-trip");
                    let json = serde_json::to_string(&id)?;
                    assert_eq!(json, "\"round-trip\"");
                    let back: $ty = serde_json::from_str(&json)?;
                    assert_eq!(back, id);
                    Ok(())
                }
            }
        };
    }

    test_id_newtype!(node_id, NodeId);
    test_id_newtype!(pipeline_id, PipelineId);
    test_id_newtype!(run_id, RunId);
    test_id_newtype!(job_id, JobId);
    test_id_newtype!(tenant_id, TenantId);
    test_id_newtype!(subject_name, SubjectName);

    #[test]
    fn run_id_from_job_carries_the_same_string() {
        let job = JobId::new("j1");
        let run = RunId::from_job(&job);
        assert_eq!(run.as_str(), "j1");
    }

    proptest::proptest! {
        /// Cheap identity check, not load-bearing the way the slug and
        /// resolver property tests are (see `slug.rs`, `resolver.rs`) — this
        /// exists to catch a macro-expansion typo (e.g. an accidentally
        /// truncated `CompactString`), not to prove a security-relevant
        /// invariant.
        #[test]
        fn new_then_as_str_round_trips(s in ".*") {
            let id = NodeId::new(s.clone());
            proptest::prop_assert_eq!(id.as_str(), s.as_str());
        }
    }
}
