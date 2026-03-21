// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Sample optimized key-value store extension with local/shared variants.
//!
//! Demonstrates the dual-type pattern — `with_local()` + `with_shared()` with
//! different concrete types. The local variant avoids locking overhead on
//! single-threaded runtimes, while the shared variant uses `Arc<RwLock>` for
//! thread safety.
//!
//! The builder's TypeId guard ensures these are recognized as independent
//! types, each getting its own lifecycle and control channel.

use linkme::distributed_slice;
use otap_df_config::node::NodeUserConfig;
use otap_df_engine::ExtensionFactory;
use otap_df_engine::capability::key_value_store::KeyValueStore;
use otap_df_engine::config::ExtensionConfig;
use otap_df_engine::context::PipelineContext;
use otap_df_engine::extension::{ExtensionWrapper, Passive};
use otap_df_engine::node::NodeId;
use std::rc::Rc;
use std::sync::Arc;

use otap_df_otap::OTAP_EXTENSION_FACTORIES;

/// Local (!Send) variant of the extension.
pub mod local;
/// Shared (Send) variant of the extension.
pub mod shared;

/// URN identifying this extension in pipeline configuration.
pub const SAMPLE_OPTIMIZED_KV_STORE_URN: &str =
    "urn:otap:extension:sample_optimized_key_value_store";

/// Register the sample optimized key-value store extension factory.
#[allow(unsafe_code)]
#[distributed_slice(OTAP_EXTENSION_FACTORIES)]
pub static SAMPLE_OPTIMIZED_KV_STORE: ExtensionFactory = ExtensionFactory {
    name: SAMPLE_OPTIMIZED_KV_STORE_URN,
    description: "Sample optimized in-memory key-value store with local/shared variants",
    documentation_url: "",
    capabilities: otap_df_engine::extension_capabilities!(
        shared: shared::SampleOptimizedKeyValueStore,
        local: local::SampleOptimizedKeyValueStore
        => KeyValueStore
    ),
    create: |_: PipelineContext,
             node: NodeId,
             node_config: Arc<NodeUserConfig>,
             extension_config: &ExtensionConfig| {
        let local_ext = local::SampleOptimizedKeyValueStore::new();
        let shared_ext = shared::SampleOptimizedKeyValueStore::new();

        Ok(
            ExtensionWrapper::builder(node, node_config, extension_config)
                .with_local(Passive(Rc::new(local_ext)))
                .with_shared(Passive(shared_ext))
                .build(),
        )
    },
    validate_config: otap_df_config::validation::no_config,
};
