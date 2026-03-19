// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Trait for shared (Send) extensions.

use crate::error::Error;
use crate::extension::{ControlChannel, EffectHandler};
use crate::terminal_state::TerminalState;
use async_trait::async_trait;

/// A trait for pipeline extensions (Send variant).
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
/// The shared `Extension` trait requires the `Send` bound, enabling use in both
/// single-threaded and multi-threaded runtime contexts.
#[async_trait]
pub trait Extension: Send {
    /// Starts the extension.
    ///
    /// The pipeline engine calls this to start the extension in a dedicated task.
    /// Extensions are started BEFORE receivers, processors, and exporters so that
    /// their capabilities are available when data-path components initialize.
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
        self: Box<Self>,
        ctrl_chan: ControlChannel,
        effect_handler: EffectHandler,
    ) -> Result<TerminalState, Error>;
}
