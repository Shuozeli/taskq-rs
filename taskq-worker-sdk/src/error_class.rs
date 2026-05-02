//! Typed `error_class` surface backed by the namespace's runtime registry.
//!
//! Reference: `design.md` Sec 9.4 ("typed `error_class` enum at SDK init"),
//! `problems/08-retry-storms.md` ("worker reports `retryable: bool` +
//! `error_class`").
//!
//! The SDK fetches the namespace's registered error classes at
//! `Register` time and caches them in an [`ErrorClassRegistry`]. Constructing
//! an [`ErrorClass`] goes through [`ErrorClassRegistry::error_class`], which
//! validates the requested name against the cache and returns an
//! [`UnknownErrorClass`] otherwise. [`crate::HandlerOutcome`]'s failure
//! variants take [`ErrorClass`] (not raw `String`), so the only way to land an
//! invalid class on the wire is to skip the registry -- which the public
//! surface does not allow.
//!
//! ## Why not a true compile-time enum?
//!
//! True codegen would require a build-time step reading the namespace's
//! `error_classes` from the control plane (or a static config file), which
//! couples the worker binary to the namespace boundary at compile time. v1
//! ships the runtime newtype; a v1.1 enhancement may add an opt-in build
//! script that reads a config file and emits a `pub enum` matching the
//! registry.

use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use thiserror::Error;

/// Validated `error_class` string, paired with a back-reference to the
/// registry it was minted from.
///
/// The wire surface in `taskq_proto::Failure::error_class` is just a string;
/// the type's purpose is to enforce that every value the SDK ships originated
/// from a namespace registry lookup. Cloning is `O(1)` (the inner string is
/// `Arc`-shared).
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ErrorClass {
    name: Arc<str>,
}

impl ErrorClass {
    /// Borrow the validated class name. Equivalent to the `Display` form.
    pub fn as_str(&self) -> &str {
        &self.name
    }

    /// Consume the wrapper and return the inner `String`.
    pub fn into_string(self) -> String {
        self.name.to_string()
    }
}

impl fmt::Debug for ErrorClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ErrorClass").field(&self.name).finish()
    }
}

impl fmt::Display for ErrorClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.name)
    }
}

/// Returned by [`ErrorClassRegistry::error_class`] when the requested name is
/// not in the namespace's registered set.
#[derive(Debug, Error)]
#[error("error_class {name:?} is not registered for this namespace; known classes: {known:?}")]
pub struct UnknownErrorClass {
    /// The name the caller attempted to look up.
    pub name: String,
    /// The namespace's registered set at the time of lookup.
    pub known: Vec<String>,
}

/// Cached snapshot of the namespace's `error_class` set, returned by the
/// `Register` RPC.
///
/// Cheap to clone (`Arc`-backed); the harness shares it between the heartbeat
/// task, the acquire loop, and any user code that wants to mint
/// [`ErrorClass`] values for [`crate::HandlerOutcome`]s.
#[derive(Clone)]
pub struct ErrorClassRegistry {
    inner: Arc<RegistryInner>,
}

struct RegistryInner {
    classes: HashSet<Arc<str>>,
}

impl ErrorClassRegistry {
    /// Build a new registry from the `error_classes` field of
    /// `RegisterWorkerResponse`.
    pub fn new<I, S>(classes: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let set: HashSet<Arc<str>> = classes
            .into_iter()
            .map(|s| Arc::<str>::from(s.into()))
            .collect();
        Self {
            inner: Arc::new(RegistryInner { classes: set }),
        }
    }

    /// Validate `name` against the registry and return a typed
    /// [`ErrorClass`].
    ///
    /// Use this when a handler wants to emit a typed failure. If the
    /// namespace's registry has shifted since `Register` (e.g. the operator
    /// added a new class), the SDK MAY refresh by re-registering.
    ///
    /// # Errors
    ///
    /// Returns [`UnknownErrorClass`] when `name` is not in the cached set.
    pub fn error_class(&self, name: &str) -> Result<ErrorClass, UnknownErrorClass> {
        if let Some(found) = self.inner.classes.get(name) {
            Ok(ErrorClass {
                name: Arc::clone(found),
            })
        } else {
            let mut known: Vec<String> = self.inner.classes.iter().map(|c| c.to_string()).collect();
            known.sort();
            Err(UnknownErrorClass {
                name: name.to_owned(),
                known,
            })
        }
    }

    /// Iterate the registered classes (sorted, lexicographic).
    pub fn known(&self) -> Vec<String> {
        let mut classes: Vec<String> = self.inner.classes.iter().map(|c| c.to_string()).collect();
        classes.sort();
        classes
    }

    /// Number of registered classes.
    pub fn len(&self) -> usize {
        self.inner.classes.len()
    }

    /// True when the namespace registered no error classes.
    pub fn is_empty(&self) -> bool {
        self.inner.classes.is_empty()
    }
}

impl fmt::Debug for ErrorClassRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ErrorClassRegistry")
            .field("known", &self.known())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_returns_typed_class_for_known_name() {
        // Arrange
        let registry = ErrorClassRegistry::new(["Timeout", "BadInput"]);

        // Act
        let class = registry.error_class("Timeout").expect("Timeout is known");

        // Assert
        assert_eq!(class.as_str(), "Timeout");
    }

    #[test]
    fn registry_rejects_unknown_class_with_known_list() {
        // Arrange
        let registry = ErrorClassRegistry::new(["Timeout", "BadInput"]);

        // Act
        let err = registry
            .error_class("MissingThing")
            .expect_err("unknown class must be rejected");

        // Assert
        assert_eq!(err.name, "MissingThing");
        assert_eq!(err.known, vec!["BadInput".to_owned(), "Timeout".to_owned()]);
    }

    #[test]
    fn registry_known_returns_sorted_set() {
        // Arrange
        let registry = ErrorClassRegistry::new(["Zeta", "Alpha", "Beta"]);

        // Act
        let known = registry.known();

        // Assert
        assert_eq!(
            known,
            vec!["Alpha".to_owned(), "Beta".to_owned(), "Zeta".to_owned()]
        );
    }

    #[test]
    fn registry_with_no_classes_reports_empty() {
        // Arrange
        let registry: ErrorClassRegistry = ErrorClassRegistry::new::<[&str; 0], &str>([]);

        // Act + Assert
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
    }
}
