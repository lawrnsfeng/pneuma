//! [`FlowKey`] — the identity work is made fair *across*.
//!
//! A flow is whatever combination of dimensions a deployment wants to be fair
//! between: `(tenant_id, workflow_type)` by default, optionally a priority
//! tier, cost class, or SLA class. `FRAMEWORK-FOUNDATIONS.md` §5 puts that
//! composition in configuration rather than scattered through dispatch code,
//! so this type is built from a list of dimensions rather than fixed fields.
//!
//! # Why the encoding is the interesting part
//!
//! The key becomes a `PARTITION BY` column, so **two distinct dimension sets
//! must never render to the same string**. If they do, two tenants share one
//! quota — one starves the other, and the isolation the whole subsystem exists
//! to provide is gone. It fails silently and looks like a capacity problem.
//!
//! This is not a hypothetical failure mode for this codebase. `pneuma-core`'s
//! `slug` module exists because the original used `str.lstrip` as a
//! prefix strip and aliased distinct coordination keys together, and a later
//! revision of that same module reintroduced the aliasing on the writer side by
//! rendering `NodeId("B:7")` identically to node `B` at index 7. The lesson
//! transfers directly: a separator-joined identifier is unsafe unless the
//! separator cannot occur in the parts.
//!
//! So values are escaped rather than trusted: `\` becomes `\\` and `|` becomes
//! `\|`. Escaping the escape is not decoration — without it,
//! `["a\\", "b"]` and `["a|d1=b"]` render identically.
//!
//! **What guards that, precisely.** Named tests pin each specific collision,
//! and each was confirmed by breaking the encoder and watching exactly those
//! tests fail. The property test is a regression net over ordinary values and
//! **does not prove injectivity**: it draws its two inputs independently, so
//! finding a collision would mean randomly producing both halves of a colliding
//! pair in one sample. Against a deliberately broken encoder it passes — at
//! 200,000 cases, with an alphabet that can generate the colliding value.
//!
//! This paragraph previously claimed the property test proved injectivity. It
//! does not, and the distinction is load-bearing: a reader trusting the earlier
//! wording could delete a named collision test as a redundant special case,
//! watch the property test stay green, and ship an encoder whose ambiguity is
//! unguarded — reintroducing exactly the two-tenants-one-quota failure this
//! module exists to prevent.
//!
//! Today's tenant ids would not have collided: the real sample carries
//! `tenant_id: "default"`, and the original broker already interpolates it
//! into a NATS subject as `"%s.tenant_%s"`, so a `.` would break routing before
//! it reached here. That is an argument about current data, not about the
//! encoding, and it is exactly the kind of argument the `slug` defect survived
//! on until it did not.

use compact_str::CompactString;

/// Separator between rendered dimensions. Escaped inside values.
const SEPARATOR: char = '|';

/// Separator between a dimension's name and its value.
const ASSIGN: char = '=';

/// The escape character. Escaped inside values.
const ESCAPE: char = '\\';

/// One classifying dimension of a flow.
///
/// The name is developer-supplied — it comes from configuration, not from a
/// message — so it is validated rather than escaped. The value is external and
/// is escaped.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Dimension {
    name: CompactString,
    value: CompactString,
}

impl Dimension {
    /// Builds a dimension, rejecting a name that would make the key ambiguous.
    ///
    /// Names are restricted to `[a-z0-9_]` so they can never contain the
    /// separator, the assignment character, or the escape.
    pub fn new(
        name: impl Into<CompactString>,
        value: impl Into<CompactString>,
    ) -> Result<Self, FlowKeyError> {
        let name = name.into();
        if name.is_empty() {
            return Err(FlowKeyError::EmptyDimensionName);
        }
        // Built in one expression rather than a multi-line struct literal,
        // whose field lines the coverage tool attributes unreliably.
        let invalid = name
            .chars()
            .find(|c| !c.is_ascii_lowercase() && !c.is_ascii_digit() && *c != '_');
        if let Some(character) = invalid {
            let name = name.to_string();
            return Err(FlowKeyError::InvalidDimensionName { name, character });
        }
        Ok(Dimension {
            name,
            value: value.into(),
        })
    }

    /// The dimension's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The dimension's unescaped value.
    pub fn value(&self) -> &str {
        &self.value
    }
}

/// The rendered identity of a flow.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FlowKey(CompactString);

impl FlowKey {
    /// Composes a key from an ordered list of dimensions.
    ///
    /// Order is significant and is the caller's: a deployment that classifies
    /// by `(tenant, workflow)` and one that classifies by `(workflow, tenant)`
    /// produce different keys, which is correct — they are different
    /// partitionings, and silently sorting them would merge two configurations
    /// that a reader can see are distinct.
    pub fn new(dimensions: &[Dimension]) -> Result<Self, FlowKeyError> {
        if dimensions.is_empty() {
            return Err(FlowKeyError::NoDimensions);
        }

        let mut rendered = String::new();
        for (index, dimension) in dimensions.iter().enumerate() {
            if index > 0 {
                rendered.push(SEPARATOR);
            }
            rendered.push_str(&dimension.name);
            rendered.push(ASSIGN);
            for character in dimension.value.chars() {
                if character == ESCAPE || character == SEPARATOR {
                    rendered.push(ESCAPE);
                }
                rendered.push(character);
            }
        }
        Ok(FlowKey(CompactString::from(rendered)))
    }

    /// The key as it is stored and grouped by.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for FlowKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Why a flow key could not be built.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FlowKeyError {
    /// A key with no dimensions would put every tenant in one flow.
    #[error("a flow key needs at least one dimension")]
    NoDimensions,
    /// A dimension name was blank.
    #[error("a dimension name must not be empty")]
    EmptyDimensionName,
    /// A dimension name contained something outside `[a-z0-9_]`.
    #[error("dimension name {name:?} contains {character:?}; names are limited to [a-z0-9_]")]
    InvalidDimensionName {
        /// The offending name.
        name: String,
        /// The first character that is not allowed.
        character: char,
    },
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn dim(name: &str, value: &str) -> Dimension {
        match Dimension::new(name, value) {
            Ok(dimension) => dimension,
            Err(err) => panic!("{name}={value} should be a valid dimension: {err}"),
        }
    }

    fn key(dimensions: &[Dimension]) -> String {
        match FlowKey::new(dimensions) {
            Ok(key) => key.as_str().to_owned(),
            Err(err) => panic!("should build a key: {err}"),
        }
    }

    #[test]
    fn the_default_composition_is_tenant_then_workflow() {
        // FRAMEWORK-FOUNDATIONS.md 5: default (tenant_id, workflow_type).
        let rendered = key(&[
            dim("tenant_id", "acme"),
            dim("workflow_type", "invoice.page.default"),
        ]);
        assert_eq!(
            rendered,
            "tenant_id=acme|workflow_type=invoice.page.default"
        );
    }

    #[test]
    fn dimension_order_is_the_callers_and_is_significant() {
        // Two different partitionings, not two spellings of one. Sorting them
        // would silently merge configurations a reader can see are different.
        let forward = key(&[dim("tenant_id", "a"), dim("workflow_type", "b")]);
        let reverse = key(&[dim("workflow_type", "b"), dim("tenant_id", "a")]);
        assert_ne!(forward, reverse);
    }

    #[test]
    fn a_separator_in_a_value_cannot_forge_another_dimension() {
        // The collision that would put two tenants in one quota. Without
        // escaping, tenant "a|workflow_type=b" would render identically to the
        // two-dimension key for tenant "a", workflow "b".
        let forged = key(&[dim("tenant_id", "a|workflow_type=b")]);
        let genuine = key(&[dim("tenant_id", "a"), dim("workflow_type", "b")]);
        assert_ne!(
            forged, genuine,
            "a value must not be able to impersonate a dimension boundary"
        );
    }

    #[test]
    fn an_escape_in_a_value_cannot_forge_a_boundary_either() {
        // The second-order attempt: end a value with a backslash so it eats the
        // escape that protects the following separator.
        let sneaky = key(&[dim("tenant_id", "a\\"), dim("workflow_type", "b")]);
        let other = key(&[dim("tenant_id", "a\\|workflow_type=b")]);
        assert_ne!(sneaky, other);
    }

    #[test]
    fn a_value_ending_in_the_escape_cannot_swallow_a_boundary() {
        // The ambiguity that escaping ONLY the separator would introduce, and
        // the reason the escape character is itself escaped.
        //
        // Under separator-only escaping both of these render to
        // `d0=a\\|d1=b`: the first because the trailing backslash is emitted
        // raw and the real boundary follows it, the second because the literal
        // `|` is escaped into exactly that pair. Two different partitionings,
        // one key -- two tenants sharing a quota.
        //
        // Found by mutating the encoder, not by reading the code, and NOT by
        // the property test below -- see its docs for why.
        let boundary = key(&[dim("d0", "a\\"), dim("d1", "b")]);
        let forged = key(&[dim("d0", "a|d1=b")]);
        assert_ne!(boundary, forged);
    }

    #[test]
    fn distinct_values_give_distinct_keys() {
        let mut seen = std::collections::HashSet::new();
        for value in ["a", "a|", "|a", "a\\", "\\a", "a=b", "", "a|b", "a\\|b"] {
            assert!(
                seen.insert(key(&[dim("tenant_id", value)])),
                "value {value:?} collided with an earlier one"
            );
        }
    }

    #[test]
    fn an_empty_value_is_allowed_and_distinguishable() {
        // An absent tenant is still a flow, and must not merge with any other.
        assert_eq!(key(&[dim("tenant_id", "")]), "tenant_id=");
        assert_ne!(key(&[dim("tenant_id", "")]), key(&[dim("tenant_id", " ")]));
    }

    #[test]
    fn a_key_needs_at_least_one_dimension() {
        // Zero dimensions would put every tenant into a single flow -- the
        // exact opposite of the subsystem's purpose, and silent.
        assert_eq!(FlowKey::new(&[]), Err(FlowKeyError::NoDimensions));
        assert_eq!(
            FlowKeyError::NoDimensions.to_string(),
            "a flow key needs at least one dimension"
        );
    }

    #[test]
    fn dimension_names_are_validated_not_escaped() {
        // Names come from configuration, so a bad one is a deployment error to
        // surface, not an input to sanitise.
        assert_eq!(
            Dimension::new("", "v"),
            Err(FlowKeyError::EmptyDimensionName)
        );
        for (name, bad) in [
            ("tenant|id", '|'),
            ("tenant=id", '='),
            ("tenant\\id", '\\'),
            ("TenantId", 'T'),
            ("tenant id", ' '),
            ("tenant.id", '.'),
        ] {
            assert_eq!(
                Dimension::new(name, "v"),
                Err(FlowKeyError::InvalidDimensionName {
                    name: name.to_owned(),
                    character: bad,
                }),
                "{name} should be rejected"
            );
        }
        // The permitted alphabet.
        assert!(Dimension::new("tenant_id_2", "v").is_ok());
    }

    #[test]
    fn errors_display_usefully() {
        assert_eq!(
            FlowKeyError::EmptyDimensionName.to_string(),
            "a dimension name must not be empty"
        );
        assert_eq!(
            FlowKeyError::InvalidDimensionName {
                name: "Tenant".to_owned(),
                character: 'T',
            }
            .to_string(),
            "dimension name \"Tenant\" contains 'T'; names are limited to [a-z0-9_]"
        );
    }

    #[test]
    fn accessors_and_derives() {
        let dimension = dim("tenant_id", "acme");
        assert_eq!(dimension.name(), "tenant_id");
        assert_eq!(dimension.value(), "acme");
        assert_eq!(dimension.clone(), dimension);

        let Ok(built) = FlowKey::new(&[dimension]) else {
            panic!("should build");
        };
        assert_eq!(built.to_string(), built.as_str());
        assert_eq!(built.clone(), built);
        assert!(format!("{built:?}").contains("acme"));

        use std::collections::HashSet;
        let set: HashSet<_> = [built.clone(), built].into();
        assert_eq!(set.len(), 1);
    }

    proptest! {
        /// The property the quota logic depends on: the encoding is injective.
        ///
        /// A biased alphabet, heavy in the separator and escape.
        ///
        /// **What this cannot do**, stated because it would otherwise be
        /// mistaken for a proof: `left` and `right` are drawn independently, so
        /// finding a collision requires randomly hitting *both* halves of a
        /// colliding pair in one sample. For the real ambiguity in this
        /// encoding — `["a\\", "b"]` against `["a|d1=b"]` — that is hopeless by
        /// search, and widening the alphabet and the length bound did not help.
        /// Verified by breaking the encoder: this test still passed.
        ///
        /// It earns its place as a regression net over the ordinary cases. The
        /// specific collisions are pinned by named tests above, which is where
        /// the actual guarantee lives.
        #[test]
        fn distinct_dimensions_never_collide(
            left in prop::collection::vec("[ab01d|\\\\=]{0,8}", 1..4),
            right in prop::collection::vec("[ab01d|\\\\=]{0,8}", 1..4),
        ) {
            let to_key = |values: &[String]| {
                let dimensions: Vec<_> = values
                    .iter()
                    .enumerate()
                    .map(|(index, value)| dim(&format!("d{index}"), value))
                    .collect();
                key(&dimensions)
            };
            let left_key = to_key(&left);
            let right_key = to_key(&right);
            prop_assert_eq!(left == right, left_key == right_key);
        }
    }
}
