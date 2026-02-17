// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier

use async_trait::async_trait;

use crate::error::Error;
use crate::terminal_state::TerminalState;

/// Trait representing an extension that can be started by the engine.
#[async_trait]
pub trait Extension {

    /// Starts the extension. The extension should run until completion, returning a `TerminalState` indicating the result.
    async fn start(
        self: Box<Self>
    ) -> Result<TerminalState, Error>;
}
