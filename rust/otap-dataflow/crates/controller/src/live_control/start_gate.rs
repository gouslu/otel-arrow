// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Build-then-run gate for pipeline instances created by one reconcile.
//!
//! Topic receivers subscribe during build, and publication without subscribers
//! is dropped. Hold every new instance until the entire batch has built, even
//! when its node-acyclic topic graph has a pipeline-level cycle. Already running
//! instances are never held. No node may publish during build.

use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

const INTERRUPT_POLL_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Debug)]
enum GateDecision {
    Closed,
    Open,
    Aborted(String),
}

#[derive(Debug)]
struct GateState {
    built: usize,
    decision: GateDecision,
}

/// Cross-thread coordination is required because each pipeline builds on its own OS thread.
#[derive(Debug)]
pub(crate) struct StartGate {
    state: Mutex<GateState>,
    changed: Condvar,
}

impl StartGate {
    /// Creates a closed gate.
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(GateState {
                built: 0,
                decision: GateDecision::Closed,
            }),
            changed: Condvar::new(),
        })
    }

    /// Returns a ticket for one instance that will be held by this gate.
    pub(crate) fn ticket(self: &Arc<Self>, label: String) -> StartGateTicket {
        StartGateTicket {
            gate: Some(Arc::clone(self)),
            label,
        }
    }

    /// Opens the gate unless it was already decided.
    pub(crate) fn open(&self) {
        self.decide(GateDecision::Open);
    }

    /// Aborts the gate unless it was already decided.
    pub(crate) fn abort(&self, reason: String) {
        self.decide(GateDecision::Aborted(reason));
    }

    fn decide(&self, decision: GateDecision) {
        let mut state = self.lock();
        if matches!(state.decision, GateDecision::Closed) {
            state.decision = decision;
            self.changed.notify_all();
        }
    }

    /// Waits for every build, an abort, the deadline, or an interruption such as shutdown.
    /// Interruption is polled without the gate lock, including after all builds finish.
    pub(crate) fn wait_until_built(
        &self,
        expected: usize,
        deadline: Instant,
        interrupted: impl Fn() -> Option<String>,
    ) -> Result<(), String> {
        let mut state = self.lock();
        loop {
            match &state.decision {
                GateDecision::Aborted(reason) => return Err(reason.clone()),
                GateDecision::Open => return Ok(()),
                GateDecision::Closed => {}
            }
            drop(state);
            if let Some(reason) = interrupted() {
                return Err(reason);
            }
            state = self.lock();
            if !matches!(state.decision, GateDecision::Closed) {
                continue;
            }
            if state.built >= expected {
                return Ok(());
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(format!(
                    "timed out waiting for new pipeline instances to build ({} of {expected} built)",
                    state.built
                ));
            }
            let wait = (deadline - now).min(INTERRUPT_POLL_INTERVAL);
            state = self
                .changed
                .wait_timeout(state, wait)
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .0;
        }
    }

    /// Blocks until the gate is opened or aborted.
    pub(crate) fn wait_decision(&self) -> Result<(), String> {
        let mut state = self.lock();
        loop {
            match &state.decision {
                GateDecision::Open => return Ok(()),
                GateDecision::Aborted(reason) => return Err(reason.clone()),
                GateDecision::Closed => {
                    state = self
                        .changed
                        .wait(state)
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                }
            }
        }
    }

    fn report_built(&self) {
        let mut state = self.lock();
        state.built += 1;
        self.changed.notify_all();
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, GateState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Ensures an early reconcile return or panic cannot strand waiting instances.
pub(crate) struct StartGateAbortOnDrop(pub(crate) Arc<StartGate>);

impl Drop for StartGateAbortOnDrop {
    fn drop(&mut self) {
        self.0
            .abort("reconcile ended before new pipelines were released".to_owned());
    }
}

/// Dropping an unbuilt ticket aborts its batch, including after a build panic or spawn error.
#[derive(Debug)]
pub(crate) struct StartGateTicket {
    gate: Option<Arc<StartGate>>,
    label: String,
}

impl StartGateTicket {
    /// Reports this instance as built and waits for permission to run.
    pub(crate) fn built_and_wait(mut self) -> Result<(), String> {
        let gate = self
            .gate
            .take()
            .expect("ticket gate is present until consumed");
        gate.report_built();
        gate.wait_decision()
    }

    /// Aborts the gate because this instance failed to build.
    pub(crate) fn fail(mut self, error: &str) {
        if let Some(gate) = self.gate.take() {
            gate.abort(format!("{} failed to build: {error}", self.label));
        }
    }
}

impl Drop for StartGateTicket {
    fn drop(&mut self) {
        if let Some(gate) = self.gate.take() {
            gate.abort(format!("{} stopped before it was built", self.label));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    /// Scenario: the first ticket builds while the second has not yet built.
    /// Guarantees: no built instance runs until all instances build and the gate opens.
    #[test]
    fn gate_releases_only_after_all_built_and_open() {
        let gate = StartGate::new();
        let first = gate.ticket("a".to_owned());
        let second = gate.ticket("b".to_owned());
        let waiter = thread::spawn(move || first.built_and_wait());
        gate.wait_until_built(1, Instant::now() + Duration::from_secs(5), || None)
            .expect("first ticket should report built");
        assert!(gate.wait_until_built(2, Instant::now(), || None).is_err());
        assert!(!waiter.is_finished());

        let second_waiter = thread::spawn(move || second.built_and_wait());
        gate.wait_until_built(2, Instant::now() + Duration::from_secs(5), || None)
            .expect("both tickets should report built");
        assert!(!waiter.is_finished());
        gate.open();
        assert_eq!(waiter.join().expect("waiter"), Ok(()));
        assert_eq!(second_waiter.join().expect("waiter"), Ok(()));
    }

    /// Scenario: one instance fails to build after another has built and is waiting.
    /// Guarantees: the failure releases the built instance with an abort reason, never permission to run.
    #[test]
    fn build_failure_aborts_waiting_instances() {
        let gate = StartGate::new();
        let built = gate.ticket("a".to_owned());
        let failing = gate.ticket("b".to_owned());
        let waiter = thread::spawn(move || built.built_and_wait());
        gate.wait_until_built(1, Instant::now() + Duration::from_secs(5), || None)
            .expect("first ticket should report built");
        failing.fail("boom");
        let err = gate
            .wait_until_built(2, Instant::now() + Duration::from_secs(5), || None)
            .expect_err("gate should be aborted");
        assert_eq!(err, "b failed to build: boom");
        assert_eq!(waiter.join().expect("waiter"), Err(err));
    }

    /// Scenario: all instances have built when shutdown interrupts reconciliation.
    /// Guarantees: shutdown wins over build completion and releases instances without running them.
    #[test]
    fn interruption_wins_over_all_built() {
        let gate = StartGate::new();
        let ticket = gate.ticket("a".to_owned());
        let waiter = thread::spawn(move || ticket.built_and_wait());
        gate.wait_until_built(1, Instant::now() + Duration::from_secs(5), || None)
            .expect("ticket should report built");
        let err = gate
            .wait_until_built(1, Instant::now() + Duration::from_secs(5), || {
                Some("shutdown".to_owned())
            })
            .expect_err("shutdown must interrupt even a fully built batch");
        gate.abort(err.clone());
        assert_eq!(waiter.join().expect("waiter"), Err(err));
    }

    /// Scenario: a build ticket is dropped before reporting built, as after a panic.
    /// Guarantees: the batch aborts instead of waiting forever for that instance.
    #[test]
    fn dropped_ticket_aborts_gate() {
        let gate = StartGate::new();
        drop(gate.ticket("a".to_owned()));
        assert_eq!(
            gate.wait_decision(),
            Err("a stopped before it was built".to_owned())
        );
    }

    /// Scenario: reconciliation leaves its scope while a built instance is waiting.
    /// Guarantees: the abort guard releases the instance without starting it.
    #[test]
    fn reconcile_drop_aborts_waiting_instances() {
        let gate = StartGate::new();
        let guard = StartGateAbortOnDrop(Arc::clone(&gate));
        let ticket = gate.ticket("a".to_owned());
        let waiter = thread::spawn(move || ticket.built_and_wait());
        gate.wait_until_built(1, Instant::now() + Duration::from_secs(5), || None)
            .expect("ticket should report built");
        drop(guard);
        assert_eq!(
            waiter.join().expect("waiter"),
            Err("reconcile ended before new pipelines were released".to_owned())
        );
    }
}
