// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Trait for local (!Send) extensions.

use crate::error::Error;
use crate::extension::{ControlChannel, EffectHandler};
use crate::terminal_state::TerminalState;
use async_trait::async_trait;
use std::rc::Rc;

/// A trait for pipeline extensions (!Send variant).
///
/// Extensions are long-lived components that run alongside the pipeline and
/// expose functionality (e.g., authentication, service discovery) to other
/// components through the [`CapabilityRegistry`](crate::capability::registry::CapabilityRegistry).
///
/// Unlike receivers, processors, and exporters, extensions are NOT generic over
/// PData — they never process pipeline data.
///
/// # Thread Safety
///
/// The local `Extension` trait does NOT require the `Send` bound, allowing
/// use of `Rc`, `RefCell`, and other !Send types within a single-threaded
/// `LocalSet`.
///
/// # Ownership
///
/// `start` takes `Rc<Self>` so the same instance can serve both the background
/// task and capability consumers without cloning internal state. The `Rc`
/// wrapper is managed by the engine — extension authors just implement the trait.
#[async_trait(?Send)]
pub trait Extension {
    /// Starts the extension.
    ///
    /// The pipeline engine calls this to start the extension in a dedicated task.
    /// Extensions are started BEFORE receivers, processors, and exporters so that
    /// their capabilities are available when data-path components initialize.
    ///
    /// Takes `Rc<Self>` so the running extension and its capability trait objects
    /// share the same instance via refcounting — no data cloning needed.
    ///
    /// **Passive extensions** (those that only expose capabilities without
    /// running background work) can omit this method — the default
    /// implementation waits for shutdown and returns cleanly.
    ///
    /// **Active extensions** (those that run background tasks like token
    /// refresh or periodic polling) should override this method.
    ///
    /// # Parameters
    ///
    /// - `ctrl_chan`: A channel to receive control messages.
    /// - `effect_handler`: A handler to perform side effects.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if an unrecoverable error occurs.
    async fn start(
        self: Rc<Self>,
        ctrl_chan: ControlChannel,
        effect_handler: EffectHandler,
    ) -> Result<TerminalState, Error>;
}
