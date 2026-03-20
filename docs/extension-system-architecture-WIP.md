# Extension System Architecture (v2 — Local/Shared)

## Overview

This document describes the current architecture of the
extension system in the OTAP dataflow engine. It covers
extensions, capabilities, the local/shared split, the
Active/Passive lifecycle model, and how to implement new
extensions and capabilities.

## What Are Extensions?

Extensions are standalone pipeline components that provide
**shared, cross-cutting capabilities** — such as
authentication, storage, or service discovery — to
data-path nodes (receivers, processors, exporters). They
are configured as siblings to `nodes`, not as nodes
themselves, and they never touch pipeline data directly.

## Architecture Overview

```text
+----------------------------------------------------------+
|                     Pipeline Engine                      |
|                                                          |
|  +-------------------+  +-------------------+            |
|  | Extension A       |  | Extension B       |  ...       |
|  | Active(auth)      |  | Passive(kv store) |            |
|  | local + shared    |  | shared only       |            |
|  | lifecycle         |  | no task spawned   |            |
|  +---------+---------+  +---------+---------+            |
|            | register_capability!() macro                |
|            | + extension_capabilities!() macro           |
|            v                                             |
|  +----------------------------+                          |
|  |    CapabilityRegistry      |  (built once per         |
|  |  local_handles HashMap     |   pipeline)              |
|  |  shared_handles HashMap    |                          |
|  +----+-----------------+-----+                          |
|       | resolve_bindings|                                |
|       v                 v                                |
|  +-----------+  +-----------+                            |
|  | Receiver  |  | Exporter  |                            |
|  | require() |  | require() |                            |
|  | -> Handle |  | -> Handle |                            |
|  +-----------+  +-----------+                            |
|                                                          |
|  Handles dispatch to Local(Rc<dyn Trait>) or             |
|  Shared(Box<dyn Trait>) based on ConsumerType            |
+----------------------------------------------------------+
```

## Key Design Decisions

1. **Extensions start first, shut down last.** Active
   extensions are spawned before data-path nodes. At
   shutdown, extensions terminate only after all data-path
   nodes have drained. Passive extensions (no lifecycle)
   skip spawning entirely.

2. **PData-free.** Extensions are completely decoupled from
   the pipeline data type. They use `ExtensionControlMsg`
   through a dedicated control channel.

3. **Active vs Passive.** Extensions signal their lifecycle
   intent at build time via `Active(ext)` or `Passive(ext)`
   newtype wrappers. Active extensions get a task and control
   channel. Passive extensions only provide capabilities —
   no task is spawned, no control channel is allocated, no
   messages are sent. This is enforced at the type level:
   `Active<E>` requires `E: Extension`, `Passive<E>` does
   not.

4. **Local/Shared split.** Extensions support both local
   (`!Send`, `Rc`-based) and shared (`Send`, `Clone`-based)
   variants. A single extension can provide one or both:

   - **Shared-only (piggyback):** `with_shared(Active(ext))`
     — the shared type serves both local and shared consumers
     via registry fallback.
   - **Dual-type:** `with_local(Active(Rc::new(l)))` +
     `with_shared(Active(s))` — separate types with
     independent lifecycles. The builder enforces different
     `TypeId`s via a runtime assertion.
   - **Passive:** `with_shared(Passive(ext))` — no lifecycle,
     capabilities only.

5. **Handle-based capability dispatch.** Capabilities use
   a handle enum (e.g., `BearerTokenProvider`) that wraps
   either `Local(Rc<dyn Trait>)` or `Shared(Box<dyn Trait>)`.
   Consumers call `capabilities.require::<Handle>(ConsumerType)`
   — the registry selects the right variant. Local consumers
   prefer local, fall back to shared.

6. **Capability co-location.** Each capability file
   (e.g., `capability/bearer_token_provider.rs`) contains
   the shared data types, inline `local::`/`shared::` trait
   mods, and the dispatch handle — all in one place. This
   eliminates cross-folder dependencies. Root-level
   `local::capability::` and `shared::capability::` modules
   re-export the traits for ergonomic imports.

## Module Layout

```text
engine/src/
  lib.rs                    → ExtensionFactory, engine build logic
  extension.rs              → ExtensionWrapper, builder, Active, Passive,
                              ControlChannel, EffectHandler, provider traits
  capability/
    mod.rs                  → module root
    registry.rs             → CapabilityRegistry, Capabilities, macros,
                              CapabilityHandle trait, Error type
    bearer_token_provider.rs → BearerToken, Secret, local::trait,
                              shared::trait, handle enum
    key_value_store.rs      → local::trait, shared::trait, handle enum

  local/
    extension.rs            → Extension trait (!Send, Rc<Self>)
    capability.rs           → re-exports: local::capability::BearerTokenProvider etc.
    exporter.rs, receiver.rs, processor.rs  (unchanged)

  shared/
    extension.rs            → Extension trait (Send, Box<Self>)
    capability.rs           → re-exports: shared::capability::BearerTokenProvider etc.
    exporter.rs, receiver.rs, processor.rs  (unchanged)
```

### Import Paths

Extension authors (local):

```rust
use otap_df_engine::local::capability::BearerTokenProvider;
use otap_df_engine::local::extension::Extension;
```

Extension authors (shared):

```rust
use otap_df_engine::shared::capability::BearerTokenProvider;
use otap_df_engine::shared::extension::Extension;
```

Consumers (handle — mode-agnostic):

```rust
use otap_df_engine::capability::bearer_token_provider::BearerTokenProvider;
```

### Dependency Flow

```text
capability/registry.rs        → Error type, macros (no deps on local/shared)
capability/bearer_token_provider.rs → types + inline local/shared traits + handle
    ↑                                    (self-contained, no cross-folder deps)
local::capability   → re-exports from capability/bearer_token_provider::local
shared::capability  → re-exports from capability/bearer_token_provider::shared
```

All arrows point one way. No circular dependencies.

## Core Types

### Active and Passive Wrappers

Extensions signal their lifecycle intent at the builder call
site using newtype wrappers:

```rust
/// Active — has an event loop, gets a task + control channel.
pub struct Active<E>(pub E);

/// Passive — capabilities only, no task, no control channel.
pub struct Passive<E>(pub E);
```

These implement sealed `SharedProvider` / `LocalProvider`
traits that decompose the wrapped value into type-erased
components:

- `Active<E>` where `E: shared::Extension + Clone + Send` →
  stores both `shared_any` (capabilities) and
  `shared_extension` (lifecycle)
- `Passive<E>` where `E: Clone + Send` → stores only
  `shared_any` (capabilities), no `Extension` bound needed

This means:

- A passive extension **cannot** have a `start()` method
  silently ignored — it doesn't implement `Extension` at all.
- An active extension **must** implement `Extension` — the
  compiler enforces this.
- The engine skips task spawning for passive extensions —
  no control channel, no messages, zero overhead.

### ExtensionWrapper

Engine-internal struct that manages an extension's
lifecycle(s) and capability registrations:

```rust
pub struct ExtensionWrapper {
    node_id: NodeId,
    user_config: Arc<NodeUserConfig>,
    runtime_config: ExtensionConfig,

    // Lifecycle — None for passive
    shared_extension: Option<Box<dyn shared::Extension>>,
    local_extension: Option<Rc<dyn local::Extension>>,

    // Capabilities — always present
    shared_any: Option<Box<dyn CloneAnySend>>,
    local_any: Option<Rc<dyn Any>>,
    capabilities: ExtensionCapabilities,

    // Control channels — None for passive
    control_sender: Option<SharedSender<ExtensionControlMsg>>,
    control_receiver: Option<SharedReceiver<ExtensionControlMsg>>,
    shared_control_sender: Option<SharedSender<ExtensionControlMsg>>,
    shared_control_receiver: Option<SharedReceiver<ExtensionControlMsg>>,

    telemetry: Option<NodeTelemetryGuard>,
}
```

#### Builder Pattern

```rust
// Active shared-only (piggyback)
ExtensionWrapper::builder(node, config, ext_config)
    .with_shared(Active(ext))
    .build()

// Passive shared-only
ExtensionWrapper::builder(node, config, ext_config)
    .with_shared(Passive(ext))
    .build()

// Dual-type active (independent lifecycles)
ExtensionWrapper::builder(node, config, ext_config)
    .with_local(Active(Rc::new(local_ext)))
    .with_shared(Active(shared_ext))
    .build()
```

#### TypeId Guard

When both `with_local` and `with_shared` are called, the
builder asserts at `build()` that the inner types have
different `TypeId`s. Same-type means the developer should
use `with_shared()` alone (piggyback pattern). This
prevents accidentally creating two independent instances
of the same type with disconnected state.

#### Dual Control Channels

When both local and shared lifecycles are present (always
different types per the TypeId guard), the builder creates
two control channels. At `start()`, the shared lifecycle is
spawned on `tokio::spawn` (Send) and the local lifecycle
runs on the current `LocalSet` thread. Both receive
independent shutdown messages.

### Capability System

#### register_capability! Macro

A single macro registers a capability — sealing, metadata,
link-time registration, and coercion glue:

```rust
crate::register_capability!(
    BearerTokenProvider,                 // handle type
    local::BearerTokenProvider,          // local trait
    shared::BearerTokenProvider,         // shared trait
    "bearer_token_provider",             // config name
    "Provides bearer tokens for HTTP",   // description
);
```

The macro generates:

- `Sealed` / `HandleSealed` / `ExtensionCapability` impls
- A `KNOWN_CAPABILITIES` static entry (via `paste!` and
  `distributed_slice`)
- `shared_capabilities()` / `local_capabilities()` methods
  for type-erased coercion

#### CapabilityHandle Trait

Each capability defines a handle enum that dispatches to
the right variant:

```rust
pub trait CapabilityHandle: HandleSealed + Sized {
    const CAPABILITY_NAME: &'static str;
    type Local: ?Sized + 'static;
    type Shared: ?Sized + 'static;

    fn from_local(local: Rc<Self::Local>) -> Self;
    fn from_shared(shared: Box<Self::Shared>) -> Self;
}
```

Consumers resolve capabilities via the `Capabilities`
struct:

```rust
let auth = capabilities.require::<BearerTokenProvider>(
    ConsumerType::Local,
)?;
auth.get_token().await?;
```

`ConsumerType::Local` tries the local variant first, falls
back to shared. `ConsumerType::Shared` uses shared only.

#### Consuming Capabilities: require() and optional()

The `Capabilities` struct (produced by `resolve_bindings()`)
is passed to every node factory. It provides two methods
for resolving capability handles:

**`require()`** — The capability must be configured. If not
bound, returns a clear error with instructions:

```rust
let auth = capabilities.require::<BearerTokenProvider>(
    ConsumerType::Local,
)?;
// Error if not bound:
// "Missing required capability 'bearer_token_provider'.
//  Add to your node config:
//    capabilities:
//      bearer_token_provider: <extension_instance_name>"
```

**`optional()`** — The capability may or may not be
configured. Returns `None` if not bound:

```rust
if let Some(store) = capabilities.optional::<KeyValueStore>(
    ConsumerType::Local,
) {
    store.set("offset", offset_bytes).await?;
}
```

Both methods take a `ConsumerType` that determines variant
selection:

- **`ConsumerType::Local`** — Prefers the local (`Rc`-based)
  variant for zero-overhead on single-threaded runtimes.
  Falls back to the shared variant if no local variant is
  registered (piggyback mode).
- **`ConsumerType::Shared`** — Uses the shared (`Box`-based)
  variant only. Local-only extensions are not visible to
  shared consumers.

Both methods also track which variants were consumed
(`consumed_local()` / `consumed_shared()`). After all
nodes are built, the engine uses this to drop unused
extension variants — if no consumer asked for the local
variant, `drop_local()` is called, freeing the `Rc`
and preventing an orphaned lifecycle from starting.

### Extension Traits

Two lifecycle traits — local and shared:

**Local** (`local/extension.rs`):

```rust
#[async_trait(?Send)]
pub trait Extension {
    async fn start(
        self: Rc<Self>,
        ctrl_chan: ControlChannel,
        effect_handler: EffectHandler,
    ) -> Result<TerminalState, Error>;
}
```

**Shared** (`shared/extension.rs`):

```rust
#[async_trait]
pub trait Extension: Send {
    async fn start(
        self: Box<Self>,
        ctrl_chan: ControlChannel,
        effect_handler: EffectHandler,
    ) -> Result<TerminalState, Error>;
}
```

Key difference: local takes `Rc<Self>` (true single-instance
sharing with capability trait objects), shared takes
`Box<Self>` (ownership transfer).

Only active extensions implement these traits. Passive
extensions do not implement `Extension` at all.

### ExtensionFactory

```rust
pub struct ExtensionFactory {
    pub name: &'static str,
    pub description: &'static str,
    pub documentation_url: &'static str,
    pub capabilities: ExtensionCapabilities,
    pub create: fn(
        PipelineContext, NodeId, Arc<NodeUserConfig>,
        &ExtensionConfig,
    ) -> Result<ExtensionWrapper, Error>,
    pub validate_config: fn(&Value) -> Result<(), Error>,
}
```

The `capabilities` field carries the registration functions
produced by `extension_capabilities!`. The engine calls
these during build to populate the `CapabilityRegistry`.

### Built-in Capabilities

#### BearerTokenProvider

```rust
pub enum BearerTokenProvider {
    Local(Rc<dyn local::BearerTokenProvider>),
    Shared(Box<dyn shared::BearerTokenProvider>),
}
```

Provides `get_token()` and `subscribe_token_refresh()`.
Data types `BearerToken` and `Secret` are co-located in
the same file.

#### KeyValueStore

```rust
pub enum KeyValueStore {
    Local(Rc<dyn local::KeyValueStore>),
    Shared(Box<dyn shared::KeyValueStore>),
}
```

Provides `get()`, `set()`, `delete()`. Mirrors Go's
`storage.Client` interface.

## Implementing Extensions

### Active Extension (Azure Identity Auth)

```rust
// Factory
ExtensionWrapper::builder(node, node_config, ext_config)
    .with_local(Active(Rc::new(local_ext)))
    .with_shared(Active(shared_ext))
    .build()
```

Both variants implement `Extension` with their own event
loop. The builder detects different `TypeId`s, creates
dual control channels, and `start()` spawns both.

### Passive Extension (In-Memory KV Store)

```rust
// Factory — no Extension trait impl needed
ExtensionWrapper::builder(node, node_config, ext_config)
    .with_shared(Passive(ext))
    .build()
```

No task spawned, no control channel. The extension only
registers capabilities.

### Dual-Type Passive Extension (Optimized KV Store)

```rust
// Factory — local uses Rc<RefCell<HashMap>> (no locks),
//           shared uses Arc<RwLock<HashMap>> (thread-safe)
ExtensionWrapper::builder(node, node_config, ext_config)
    .with_local(Passive(Rc::new(local_ext)))
    .with_shared(Passive(shared_ext))
    .build()
```

Different types → different `TypeId`s → accepted by
builder. Local consumers get the lock-free variant,
shared consumers get the thread-safe variant.

### Adding a New Capability

**1.** Create `capability/<name>.rs` with inline
`local::`/`shared::` trait mods and handle enum:

```rust
crate::register_capability!(
    MyCapability,
    local::MyCapability,
    shared::MyCapability,
    "my_capability",
    "Does something useful",
);

#[doc(hidden)]
pub mod local { /* trait */ }
#[doc(hidden)]
pub mod shared { /* trait */ }

pub enum MyCapability {
    Local(Rc<dyn local::MyCapability>),
    Shared(Box<dyn shared::MyCapability>),
}
// + impl CapabilityHandle
```

**2.** Add re-exports in `local/capability.rs` and
`shared/capability.rs`:

```rust
pub use crate::capability::my_capability::local::MyCapability;
```

**3.** Register the module in `capability/mod.rs`.

## Configuration

### Pipeline YAML

Extensions are configured as siblings to `nodes` in the
pipeline config. Each extension has a `type` (URN) and
optional `config`. Consumers reference extensions by name
in their `capabilities` section:

```yaml
groups:
  default:
    pipelines:
      main:
        extensions:
          azure-auth:
            type: "urn:microsoft:extension:azure_identity_auth"
            config:
              method: "managed_identity"
              client_id: "your-client-id"
              scope: "https://monitor.azure.com/.default"

          kv-store:
            type: "urn:otap:extension:sample_shared_key_value_store"
            # no config needed — uses no_config validator

        nodes:
          azure-monitor-exporter:
            type: "urn:microsoft:exporter:azure_monitor"
            config:
              # ... exporter-specific config
            capabilities:
              bearer_token_provider: azure-auth
```

The `capabilities` section maps capability names to
extension instance names. This is how consumers declare
their dependencies — the engine resolves them at build
time via `resolve_bindings()`.

### Config Validation

Each `ExtensionFactory` carries a `validate_config`
function pointer that performs static validation during
config parsing — before any extension is created:

```rust
pub struct ExtensionFactory {
    // ...
    pub validate_config: fn(
        config: &serde_json::Value,
    ) -> Result<(), Error>,
}
```

Two built-in validators:

- **`validate_typed_config::<T>`** — Deserializes the JSON
  config into type `T`. If deserialization fails, the error
  surfaces immediately at config parse time with a clear
  message. This is the most common validator:

  ```rust
  validate_config: validate_typed_config::<Config>,
  ```

- **`no_config`** — Accepts `null` or `{}` only. Rejects
  any other value, catching typos or misplaced config
  blocks early:

  ```rust
  validate_config: no_config,
  ```

### Capability Binding Validation

During `resolve_bindings()`, the engine validates each
capability binding with four checks:

1. **Extension exists** — The named extension instance must
   be registered. Error: "no extension with that name exists."

2. **Known capability type** — The capability name must be
   in `KNOWN_CAPABILITIES` (registered at link time via
   `register_capability!`). Error: "Unknown capability"
   with a list of known types.

3. **Capability provided** — Some loaded extension must
   actually provide the requested capability. Error:
   "no loaded extension provides it."

4. **Specific extension provides it** — The specific named
   extension must expose the requested capability. Error:
   "does not provide capability" with a list of what it
   does provide.

After all nodes are built, the engine also detects
**unused bindings** — capabilities that were configured
but never consumed by any `require()` or `optional()`
call. These are reported as warnings for configuration
hygiene.

## Pipeline Lifecycle

```text
1. Config parsing
   ├─ Extensions parsed from `extensions` section
   └─ ExtensionInNodesSection error if misplaced

2. Pipeline build
   ├─ Create extensions (factories return ExtensionWrapper)
   ├─ register_traits() → populate CapabilityRegistry
   ├─ resolve_bindings() → per-node Capabilities
   ├─ Create data-path nodes (receive &Capabilities)
   ├─ Track consumption (consumed_local/consumed_shared)
   └─ Drop unused variants (drop_local/drop_shared)

3. Pipeline start (RuntimePipeline::run)
   ├─ Passive extensions: skip (is_passive() == true)
   ├─ Active extensions: spawn tasks, track control senders
   ├─ Spawn exporters, processors, receivers
   └─ Extension control senders stored separately

4. Steady state
   ├─ Active extensions run event loops
   ├─ Passive extensions exist only as registered capabilities
   └─ ExtensionControlMsg flows to active extensions only

5. Shutdown
   ├─ Data-path nodes drain
   ├─ shutdown_extensions() sends Shutdown to active only
   └─ Extensions terminate after data-path is fully drained
```
