//! Strategy registry — runtime selection of compile-time-linked
//! `Admitter` and `Dispatcher` implementations.
//!
//! `design.md` §7, §9.3 / `problems/05`. Strategies are compile-time linked
//! into the CP binary; per-namespace selection happens at CP startup based
//! on the operator's `namespace_quota` row. Strategy mismatch (operator
//! configured a strategy not present in this build) marks the namespace as
//! degraded, surfaces it via `/readyz`, and continues serving the rest.

use std::collections::HashMap;
use std::sync::Arc;

use taskq_storage::Namespace;

use self::admitter::{Admitter, AlwaysAdmitter, CoDelAdmitter, MaxPendingAdmitter};
use self::dispatcher::{
    AgePromotedDispatcher, Dispatcher, PriorityFifoDispatcher, RandomNamespaceDispatcher,
};

pub mod admitter;
pub mod dispatcher;

/// Resolved strategy choice for one namespace.
pub struct NamespaceStrategy {
    pub admitter: Arc<dyn Admitter>,
    pub dispatcher: Arc<dyn Dispatcher>,
}

/// Reason a namespace failed to load. Surfaced through `/readyz`.
#[derive(Debug, Clone)]
pub struct DegradedNamespace {
    pub namespace: Namespace,
    pub reason: String,
}

/// Compile-time set of strategies linked into this binary, plus the
/// per-namespace resolution computed at startup.
///
/// ## Implicit default chain
///
/// Namespaces without an explicit `(admitter, dispatcher)` row in the
/// registry fall through to a built-in default
/// `(MaxPendingAdmitter { limit: u64::MAX }, PriorityFifoDispatcher)`.
/// The `MaxPendingAdmitter` issues a transactional
/// `check_capacity_quota(MaxPending)` against the namespace's quota row at
/// admit time, so the **persisted** `namespace_quota.max_pending` is
/// authoritative. When the quota is unset, the backend returns
/// `UnderLimit { limit: u64::MAX }` and the admitter accepts. When set,
/// the admitter rejects with `MAX_PENDING_EXCEEDED` once the count tips
/// past the persisted limit. The strategy's own `limit` field is a
/// fallback only; never consulted in this default.
///
/// `design.md` §9.1 + `problems/06`: capacity quota reads are inline
/// transactional, no cache. The implicit default exists so quota
/// enforcement holds even when no operator-configured strategy row is
/// present — capacity is part of the namespace contract, not an
/// opt-in feature.
pub struct StrategyRegistry {
    namespaces: HashMap<Namespace, NamespaceStrategy>,
    degraded: Vec<DegradedNamespace>,
    default: NamespaceStrategy,
}

impl StrategyRegistry {
    /// Construct an empty registry. `main` populates it via
    /// `resolve_from_quota` for every namespace returned by
    /// `Storage::list_namespaces` (Phase 5b will add that storage method).
    pub fn empty() -> Self {
        Self {
            namespaces: HashMap::new(),
            degraded: Vec::new(),
            default: default_strategy(),
        }
    }

    /// Look up the strategy for one namespace. Returns the operator-
    /// configured strategy when one is installed; otherwise returns the
    /// built-in default chain so capacity quotas read inline from the
    /// namespace's `namespace_quota.max_pending` are enforced even when
    /// no explicit strategy is registered.
    ///
    /// Always `Some` — the implicit default is part of the contract.
    /// `Option` is preserved for API stability so call sites keep their
    /// existing `if let Some(strategy)` shape. A future refactor may
    /// collapse the return type once all callers move off the
    /// no-strategy fast path.
    pub fn for_namespace(&self, ns: &Namespace) -> Option<&NamespaceStrategy> {
        Some(self.namespaces.get(ns).unwrap_or(&self.default))
    }

    /// Insert a fully-resolved strategy choice for a namespace.
    pub fn insert(&mut self, ns: Namespace, strategy: NamespaceStrategy) {
        self.namespaces.insert(ns, strategy);
    }

    /// Record a namespace whose strategy choice could not be resolved (e.g.
    /// because the binary did not link the requested strategy). The
    /// namespace is left absent from `namespaces` and surfaced through
    /// `/readyz` so operators can see the gap.
    pub fn mark_degraded(&mut self, namespace: Namespace, reason: impl Into<String>) {
        self.degraded.push(DegradedNamespace {
            namespace,
            reason: reason.into(),
        });
    }

    /// Snapshot of all degraded namespaces for the `/readyz` body.
    pub fn degraded(&self) -> &[DegradedNamespace] {
        &self.degraded
    }

    /// Total number of healthy namespaces. Used for the `/readyz` summary.
    pub fn healthy_count(&self) -> usize {
        self.namespaces.len()
    }

    /// Snapshot of every loaded namespace's identifier. Used by Phase 5d's
    /// metrics refresher and audit pruner to enumerate the set of
    /// namespaces the CP knows about. Degraded namespaces are NOT included
    /// — they have no strategy slot, but the audit-pruner walks them
    /// separately via [`Self::degraded`].
    pub fn loaded_namespaces(&self) -> Vec<Namespace> {
        self.namespaces.keys().cloned().collect()
    }
}

impl Default for StrategyRegistry {
    fn default() -> Self {
        Self::empty()
    }
}

/// Build the implicit default `(MaxPendingAdmitter, PriorityFifoDispatcher)`
/// chain used by `StrategyRegistry::for_namespace` when no operator-
/// configured strategy row exists for the namespace.
///
/// `MaxPendingAdmitter::admit` issues a transactional
/// `check_capacity_quota(MaxPending)` and trusts the backend to read the
/// namespace's persisted `max_pending`; the strategy's own `limit` field
/// is a fallback, set to `u64::MAX` so the strategy never short-circuits
/// without consulting storage.
fn default_strategy() -> NamespaceStrategy {
    NamespaceStrategy {
        admitter: Arc::new(MaxPendingAdmitter { limit: u64::MAX }),
        dispatcher: Arc::new(PriorityFifoDispatcher),
    }
}

/// Resolve an `admitter_kind` string + opaque params into a concrete
/// `Arc<dyn Admitter>`. Unknown kinds return `Err` so the caller can mark
/// the namespace as degraded.
///
/// Phase 5a only handles the three v1 admitters; Phase 5b will pull `limit`
/// / `target_latency_ms` out of `params` (FlatBuffers-encoded today, but
/// the registry doesn't care — strategies parse what they need).
pub fn build_admitter(
    kind: &str,
    params: &AdmitterParams,
) -> Result<Arc<dyn Admitter>, StrategyMismatch> {
    match kind {
        "Always" => Ok(Arc::new(AlwaysAdmitter)),
        "MaxPending" => Ok(Arc::new(MaxPendingAdmitter {
            limit: params.max_pending_limit.unwrap_or(0),
        })),
        "CoDel" => Ok(Arc::new(CoDelAdmitter {
            target_latency_ms: params.codel_target_latency_ms.unwrap_or(0),
        })),
        other => Err(StrategyMismatch {
            kind: other.to_owned(),
            slot: StrategySlot::Admitter,
        }),
    }
}

/// Resolve a `dispatcher_kind` string + opaque params into a concrete
/// `Arc<dyn Dispatcher>`. Unknown kinds return `Err` for degradation
/// reporting.
pub fn build_dispatcher(
    kind: &str,
    params: &DispatcherParams,
) -> Result<Arc<dyn Dispatcher>, StrategyMismatch> {
    match kind {
        "PriorityFifo" => Ok(Arc::new(PriorityFifoDispatcher)),
        "AgePromoted" => Ok(Arc::new(AgePromotedDispatcher {
            age_weight: params.age_weight.unwrap_or(0.0),
        })),
        "RandomNamespace" => Ok(Arc::new(RandomNamespaceDispatcher {
            sample_attempts: params.sample_attempts.unwrap_or(4),
        })),
        other => Err(StrategyMismatch {
            kind: other.to_owned(),
            slot: StrategySlot::Dispatcher,
        }),
    }
}

/// Parsed admitter parameters. Phase 5b will populate this from the
/// FlatBuffers-serialized `NamespaceQuota.admitter_params` blob; Phase 5a
/// keeps the struct shape so callers can already write code against it.
#[derive(Debug, Default, Clone)]
pub struct AdmitterParams {
    pub max_pending_limit: Option<u64>,
    pub codel_target_latency_ms: Option<u64>,
}

/// Parsed dispatcher parameters. Same Phase 5b note as above.
#[derive(Debug, Default, Clone)]
pub struct DispatcherParams {
    pub age_weight: Option<f64>,
    pub sample_attempts: Option<u32>,
}

/// Reason a strategy lookup failed. Lifts `kind` (operator-supplied
/// string) into a structured error so the caller can build a clear
/// `DegradedNamespace` reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrategyMismatch {
    pub kind: String,
    pub slot: StrategySlot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StrategySlot {
    Admitter,
    Dispatcher,
}

impl std::fmt::Display for StrategyMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let slot = match self.slot {
            StrategySlot::Admitter => "admitter",
            StrategySlot::Dispatcher => "dispatcher",
        };
        write!(
            f,
            "configured {slot} strategy '{kind}' is not linked into this binary",
            slot = slot,
            kind = self.kind
        )
    }
}

impl std::error::Error for StrategyMismatch {}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper that pattern-matches a build_* result to a concrete strategy
    /// or panics with context. Avoids requiring `Debug` on `dyn Admitter` /
    /// `dyn Dispatcher`, which is impractical for trait objects.
    fn unwrap_admitter(
        result: std::result::Result<Arc<dyn Admitter>, StrategyMismatch>,
    ) -> Arc<dyn Admitter> {
        match result {
            Ok(admitter) => admitter,
            Err(err) => panic!("expected admitter, got mismatch: {err}"),
        }
    }

    fn unwrap_dispatcher(
        result: std::result::Result<Arc<dyn Dispatcher>, StrategyMismatch>,
    ) -> Arc<dyn Dispatcher> {
        match result {
            Ok(dispatcher) => dispatcher,
            Err(err) => panic!("expected dispatcher, got mismatch: {err}"),
        }
    }

    #[test]
    fn build_admitter_resolves_known_kinds() {
        // Arrange
        let params = AdmitterParams {
            max_pending_limit: Some(100),
            codel_target_latency_ms: Some(5_000),
        };

        // Act
        let always = unwrap_admitter(build_admitter("Always", &AdmitterParams::default()));
        let max_pending = unwrap_admitter(build_admitter("MaxPending", &params));
        let codel = unwrap_admitter(build_admitter("CoDel", &params));

        // Assert
        assert_eq!(always.name(), "Always");
        assert_eq!(max_pending.name(), "MaxPending");
        assert_eq!(codel.name(), "CoDel");
    }

    #[test]
    fn build_admitter_rejects_unknown_kind() {
        // Arrange
        let params = AdmitterParams::default();

        // Act
        let result = build_admitter("WeightedFair", &params);

        // Assert
        match result {
            Ok(_) => panic!("expected mismatch for 'WeightedFair'"),
            Err(err) => {
                assert_eq!(err.kind, "WeightedFair");
                assert_eq!(err.slot, StrategySlot::Admitter);
            }
        }
    }

    #[test]
    fn build_dispatcher_resolves_known_kinds() {
        // Arrange
        let params = DispatcherParams {
            age_weight: Some(2.5),
            sample_attempts: Some(8),
        };

        // Act
        let pfifo = unwrap_dispatcher(build_dispatcher(
            "PriorityFifo",
            &DispatcherParams::default(),
        ));
        let aged = unwrap_dispatcher(build_dispatcher("AgePromoted", &params));
        let rnd = unwrap_dispatcher(build_dispatcher("RandomNamespace", &params));

        // Assert
        assert_eq!(pfifo.name(), "PriorityFifo");
        assert_eq!(aged.name(), "AgePromoted");
        assert_eq!(rnd.name(), "RandomNamespace");
    }

    #[test]
    fn for_namespace_returns_default_strategy_when_no_explicit_entry() {
        // Arrange: empty registry. Per `default_strategy`, the
        // implicit fallback is (MaxPendingAdmitter limit=u64::MAX,
        // PriorityFifoDispatcher) so capacity quotas still get
        // enforced via the backend's transactional read even when no
        // operator-configured row exists.
        let registry = StrategyRegistry::empty();

        // Act
        let strategy = registry
            .for_namespace(&Namespace::new("never-configured"))
            .expect("default chain must always resolve");

        // Assert
        assert_eq!(strategy.admitter.name(), "MaxPending");
        assert_eq!(strategy.dispatcher.name(), "PriorityFifo");
    }

    #[test]
    fn for_namespace_returns_inserted_strategy_over_default() {
        // Arrange: install an explicit chain.
        let mut registry = StrategyRegistry::empty();
        let ns = Namespace::new("explicit");
        let custom = NamespaceStrategy {
            admitter: unwrap_admitter(build_admitter("Always", &AdmitterParams::default())),
            dispatcher: unwrap_dispatcher(build_dispatcher(
                "AgePromoted",
                &DispatcherParams {
                    age_weight: Some(1.5),
                    sample_attempts: Some(8),
                },
            )),
        };
        registry.insert(ns.clone(), custom);

        // Act
        let strategy = registry
            .for_namespace(&ns)
            .expect("inserted chain must resolve");

        // Assert
        assert_eq!(strategy.admitter.name(), "Always");
        assert_eq!(strategy.dispatcher.name(), "AgePromoted");
    }

    #[test]
    fn loaded_namespaces_excludes_implicit_default() {
        // Arrange: empty registry has the implicit default but no
        // explicit entries.
        let registry = StrategyRegistry::empty();

        // Act / Assert
        assert!(registry.loaded_namespaces().is_empty());
        assert_eq!(registry.healthy_count(), 0);
    }

    #[test]
    fn registry_tracks_degraded_namespaces() {
        // Arrange
        let mut registry = StrategyRegistry::empty();

        // Act
        registry.mark_degraded(
            Namespace::new("broken"),
            "configured admitter 'WeightedFair' not in this binary",
        );

        // Assert
        let degraded = registry.degraded();
        assert_eq!(degraded.len(), 1);
        assert_eq!(degraded[0].namespace.as_str(), "broken");
        assert_eq!(registry.healthy_count(), 0);
    }
}
