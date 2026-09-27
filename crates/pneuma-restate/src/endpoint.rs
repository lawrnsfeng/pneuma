//! Turning a component name into the URL to call.
//!
//! `spikes/restate/VERDICT.md` §8 is the requirement: a component participates
//! over plain HTTP without adopting the Restate SDK, and durability comes from
//! the call being wrapped in `ctx.run`. So something has to decide *where* a
//! named component lives, and that decision is deployment-specific — the
//! original addresses components by publishing to a subject named after
//! `StepCommon::name`, and an HTTP deployment has no equivalent convention to
//! inherit.
//!
//! Rather than invent one, this takes a template and substitutes. The template
//! is configuration; the substitution is the only thing with a rule.

/// Why an endpoint could not be built or resolved.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EndpointError {
    /// The template does not mention the component.
    ///
    /// Refused rather than accepted as a fixed URL. A template without the
    /// placeholder sends every component to the same address, which in a
    /// deployment with one component looks exactly like working — and stops
    /// looking like it the moment a second exists.
    #[error(
        "the endpoint template {template:?} does not contain {}",
        Endpoint::PLACEHOLDER
    )]
    NoPlaceholder {
        /// What was given.
        template: String,
    },

    /// A step's component name is empty.
    #[error("a component name is empty, so there is no address to build")]
    EmptyComponent,

    /// A component name would change the meaning of the URL.
    ///
    /// Refused rather than percent-encoded, the same choice
    /// `pneuma_store::refpath` makes for a dotted node id: inventing an
    /// encoding means the port and the original address different things, and
    /// during any period of coexistence that is silent.
    #[error("the component name {component:?} contains {character:?}, which would change the URL")]
    UnsafeComponent {
        /// The offending name.
        component: String,
        /// The character that made it unusable.
        character: char,
    },
}

/// Where components live, as a template.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    template: String,
}

impl Endpoint {
    /// The placeholder a template must contain.
    pub const PLACEHOLDER: &'static str = "{component}";

    /// Characters refused in a component name.
    ///
    /// Everything that gives a URL structure. A name containing `/` would add
    /// a path segment, `?` a query, `#` a fragment, and a space or control
    /// character is not valid in a URL at all. Dots and dashes are deliberately
    /// fine: every name in the corpus is dotted
    /// (`freeform.page.commercial-invoice.preprocessing`), and refusing those
    /// would refuse production.
    ///
    /// `&`, `=`, `;` and `+` are here because a template may put the component
    /// in a *query string* — `…/predict?c={component}` is a reasonable thing to
    /// configure — and there a name like `a&admin=1` adds or overrides
    /// parameters on the component call. Exactly the "changes what the URL
    /// means" this list exists for, and the first version missed it by thinking
    /// only about path position.
    ///
    /// An earlier draft of this comment cited the integration test as using
    /// that shape. It did, and was rewritten to path position in the same
    /// commit — so the rule stands on the configuration being possible, not on
    /// a test that exercises it. Nothing in the repository does any more.
    ///
    /// `+` and `=` are a **behaviour change**: a component name containing
    /// either resolved before and is refused now. No name in the corpus
    /// contains one, and a name that does cannot be addressed safely in query
    /// position — but a deployment carrying one now fails loudly rather than
    /// quietly building a different URL, which is the intended direction.
    const REFUSED: &'static [char] = &['/', '?', '#', '\\', ' ', '%', '@', ':', '&', '=', ';', '+'];

    /// Builds an endpoint from a template such as
    /// `http://components.svc/{component}/predict`.
    pub fn new(template: impl Into<String>) -> Result<Self, EndpointError> {
        let template = template.into();
        if !template.contains(Self::PLACEHOLDER) {
            return Err(EndpointError::NoPlaceholder { template });
        }
        Ok(Endpoint { template })
    }

    /// The URL for one component.
    pub fn url_for(&self, component: &str) -> Result<String, EndpointError> {
        if component.is_empty() {
            return Err(EndpointError::EmptyComponent);
        }
        if let Some(character) = component
            .chars()
            .find(|c| Self::REFUSED.contains(c) || c.is_control())
        {
            return Err(EndpointError::UnsafeComponent {
                component: component.to_owned(),
                character,
            });
        }
        Ok(self.template.replace(Self::PLACEHOLDER, component))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn endpoint() -> Endpoint {
        match Endpoint::new("http://components.svc/{component}/predict") {
            Ok(endpoint) => endpoint,
            Err(error) => panic!("a template with the placeholder is valid: {error}"),
        }
    }

    #[test]
    fn a_dotted_corpus_name_resolves() {
        let Ok(url) = endpoint().url_for("freeform.page.commercial-invoice.preprocessing") else {
            panic!("every name in the corpus is dotted, so this must work");
        };
        assert_eq!(
            url,
            "http://components.svc/freeform.page.commercial-invoice.preprocessing/predict"
        );
    }

    #[test]
    fn a_template_appearing_twice_substitutes_both() {
        // Not a supported shape so much as a defined one: `replace` does all
        // occurrences, and a template mentioning the component in a host *and*
        // a path is a reasonable thing to write.
        let Ok(endpoint) = Endpoint::new("http://{component}.svc/{component}") else {
            panic!("valid");
        };
        let Ok(url) = endpoint.url_for("ocr") else {
            panic!("valid");
        };
        assert_eq!(url, "http://ocr.svc/ocr");
    }

    #[test]
    fn a_template_without_the_placeholder_is_refused() {
        // The failure this exists for: it would send every component to one
        // address, which looks like working until a second component exists.
        let Err(error) = Endpoint::new("http://components.svc/predict") else {
            panic!("a fixed URL is not a template");
        };
        let EndpointError::NoPlaceholder { template } = &error else {
            panic!("wrong variant: {error:?}");
        };
        assert_eq!(template, "http://components.svc/predict");
        // The placeholder was a field once, always set to the one constant it
        // could be. Carried no information, and its struct-literal line was a
        // move tarpaulin could never see run. It is rendered from the constant
        // instead, so the message still tells an operator what is missing.
        assert!(error.to_string().contains("{component}"), "{error}");
    }

    #[test]
    fn an_empty_component_name_is_refused() {
        let Err(error) = endpoint().url_for("") else {
            panic!("there is no address for nothing");
        };
        assert_eq!(error, EndpointError::EmptyComponent);
    }

    #[test]
    fn a_name_that_would_restructure_the_url_is_refused_not_encoded() {
        // Each of these changes what the URL *means* rather than merely how it
        // looks, which is why they are refused rather than percent-encoded.
        for (component, character) in [
            ("a/../admin", '/'),
            ("a?x=1", '?'),
            ("a#frag", '#'),
            ("a b", ' '),
            ("a%2f", '%'),
            ("host:9000", ':'),
            ("user@host", '@'),
            ("a\\b", '\\'),
            // Query position: the template may put the component after a `?`,
            // where these inject or override parameters.
            ("a&admin=1", '&'),
            ("a=b", '='),
            ("a;b", ';'),
            ("a+b", '+'),
        ] {
            let Err(error) = endpoint().url_for(component) else {
                panic!("{component} must not resolve");
            };
            let EndpointError::UnsafeComponent {
                character: found, ..
            } = &error
            else {
                panic!("wrong variant for {component}: {error:?}");
            };
            assert_eq!(*found, character, "for {component}");
        }
    }

    #[test]
    fn a_control_character_is_refused_even_though_it_is_not_in_the_list() {
        // Listing every control character would be a list that goes stale;
        // `is_control` is the rule. A newline in a name is how a header
        // injection starts.
        let Err(error) = endpoint().url_for("ocr\nHost: evil") else {
            panic!("a control character must not resolve");
        };
        let EndpointError::UnsafeComponent { character, .. } = &error else {
            panic!("wrong variant: {error:?}");
        };
        assert_eq!(*character, '\n');
    }
}
