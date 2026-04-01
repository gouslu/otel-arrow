// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Sample shared-only key-value store extension.
//!
//! Demonstrates the `with_shared()` only pattern — a single shared type
//! serves both local and shared consumers via the registry fallback.
//!
//! This models a storage backend where the underlying store (file, database,
//! network) is inherently `Send`, making a local `Rc`-based variant pointless.
//! For this sample, the store is in-memory (`Arc<RwLock<HashMap>>`), but the
//! pattern applies equally to real I/O-backed stores.

use linkme::distributed_slice;
use otap_df_config::node::NodeUserConfig;
use otap_df_engine::ExtensionFactory;
use otap_df_engine::capability::key_value_store::KeyValueStore;
use otap_df_engine::config::ExtensionConfig;
use otap_df_engine::context::PipelineContext;
use otap_df_engine::extension::{ExtensionWrapper, Passive};
use otap_df_engine::node::NodeId;
use std::sync::Arc;

use otap_df_otap::OTAP_EXTENSION_FACTORIES;

mod extension;

pub use extension::SampleSharedKeyValueStore;

/// URN identifying this extension in pipeline configuration.
pub const SAMPLE_SHARED_KV_STORE_URN: &str = "urn:otap:extension:sample_shared_key_value_store";

/// Register the sample shared key-value store extension factory.
#[allow(unsafe_code)]
#[distributed_slice(OTAP_EXTENSION_FACTORIES)]
pub static SAMPLE_SHARED_KV_STORE: ExtensionFactory = ExtensionFactory {
    name: SAMPLE_SHARED_KV_STORE_URN,
    description: "Sample shared-only in-memory key-value store",
    documentation_url: "",
    capabilities: otap_df_engine::extension_capabilities!(
        shared: SampleSharedKeyValueStore => KeyValueStore
    ),
    create: |_: PipelineContext,
             node: NodeId,
             node_config: Arc<NodeUserConfig>,
             extension_config: &ExtensionConfig| {
        let ext = SampleSharedKeyValueStore::new();

        Ok(
            ExtensionWrapper::builder(node, node_config, extension_config)
                .with_shared(Passive(ext))
                .build(),
        )
    },
    validate_config: otap_df_config::validation::no_config,
};
