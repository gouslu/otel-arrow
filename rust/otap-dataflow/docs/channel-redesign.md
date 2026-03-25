# Channel Redesign Proposal

## Status

Draft — design exploration, not yet implemented.

## Problem Statement

The current `MessageChannel` in `message.rs` is a ~200-line async state machine
that multiplexes control and pdata channels with built-in shutdown draining,
deadline enforcement, synthetic shutdown generation, backpressure gating, and
`received_at_node` stamping. This complexity exists because the engine pushes
shutdown and drain responsibility into every node's hot path rather than
handling it centrally.

Additionally, the current shutdown sequence is **lossy under load**: receivers
get explicit Shutdown, but processors and exporters receive synthetic Shutdown
when their pdata channel closes. This creates a race where ack/nack messages
can be lost if upstream nodes exit before downstream ack/nack arrives — the
order of node exit is timing-dependent.

## Goals

1. **Simplify or eliminate `MessageChannel`** — reduce it to a trivial biased
   select, or remove it entirely by exposing raw channels to nodes.
2. **Deterministic graceful shutdown** — no data loss if there is time, with
   proper ack/nack delivery back to origin.
3. **Drain handle API** — compile-time enforcement that nodes handle remaining
   buffered pdata on shutdown.
4. **Fair scheduling** — control messages have priority, but pdata and ack/nack
   get fair alternation (neither starves the other).
5. **Implicit `received_at_node` stamping via `PdataReceiver`** — a thin
   wrapper around the pdata receiver that stamps automatically on `recv()`,
   keeping the stamping invisible to node implementations.

## Current Architecture

### Message flow

``` md
Receiver → [pdata channel] → Processor → [pdata channel] → Exporter
                                                              ↓
                                                         External System
                                                              ↓
Receiver ← [control channel] ← Processor ← [control channel] ← Ack/nack
           (via PipelineCtrlMsgManager unwinding)
```

Each `→` is a separate MPSC/MPMC channel between adjacent nodes. Pdata flows
forward, ack/nack flows backward through the `PipelineCtrlMsgManager`.

### Current shutdown sequence

1. Engine sends `Shutdown` to receivers only.
2. Receivers drain and exit → drop their pdata senders.
3. Processors get synthetic Shutdown from `MessageChannel` when pdata closes →
   drain with deadline → exit.
4. Exporters get synthetic Shutdown similarly → drain → exit.
5. Ack/nack during draining is best-effort — if the upstream node already
   exited, ack/nack is silently dropped.

**Problem:** Steps 3–5 are timing-dependent. Under load or with slow external
systems, processors may exit before exporter ack/nack arrives, causing lost
acknowledgements and caller timeouts.

### Current `MessageChannel` responsibilities

| Responsibility | Lines | Complexity |
| --- | --- | --- |
| Control priority (biased select) | ~10 | Low |
| Shutdown draining state machine | ~60 | High |
| Deadline enforcement (sleep timer) | ~15 | Medium |
| Synthetic Shutdown on closed pdata | ~20 | Medium |
| `accept_pdata` gating | ~25 | Medium |
| Closed-pdata detection probe | ~15 | Medium |
| `received_at_node` stamping | ~5 | Low |
| Post-shutdown Closed error | ~5 | Low |

## Proposed Design

### Principle: Engine controls shutdown, nodes just process and exit

Nodes have a simple contract:

- Process messages until you receive `Shutdown` on the control channel.
- On `Shutdown`, use the drain handle to handle remaining pdata.
- Return `(TerminalState, DrainResult)`.

The engine orchestrates **when** each node gets Shutdown, ensuring proper
ordering so ack/nack flows correctly.

### Shutdown sequence (phased, reverse-topological)

``` md
Phase 1: Stop the inflow
  → Send "StopIngesting" to all receivers
  → Receivers stop accepting new connections/data
  → Receivers signal OK to engine
  → Receivers stay alive for ack/nack

Phase 2: Shutdown exporters
  → Send Shutdown to all exporters
  → Exporters drain pdata (drain handle), flush HTTP requests, send ack/nack
  → Exporters exit
  → Ack/nack from exporters flows through PipelineCtrlMsgManager to processors

Phase 3: Shutdown processors (reverse topological order)
  → Engine waits for exporter exits + ack/nack flush
  → Send Shutdown to leaf processors (those adjacent to exporters)
  → Processors drain pdata, forward ack/nack upstream
  → Processors exit
  → Cascade: next layer of processors gets Shutdown

Phase 4: Shutdown receivers
  → All ack/nack has been delivered
  → Send Shutdown to receivers
  → Receivers process final ack/nack, respond to callers
  → Receivers exit

Global deadline: if any phase exceeds the remaining time budget, engine
cancels all remaining tasks (force kill).
```

### Engine-driven shutdown detection

The runtime loop uses `FuturesUnordered` to react to individual node task
exits, and delegates the actual Shutdown delivery to the `PipelineCtrlMsgManager`
via a new `ShutdownNode` pipeline control message (Option C):

```rust
// Runtime loop (in runtime_pipeline.rs)
// Keeps a clone of pipeline_ctrl_msg_tx for sending ShutdownNode.
let mut exited: HashSet<usize> = HashSet::new();

while let Some((node_idx, result)) = tagged_futures.next().await {
    exited.insert(node_idx);

    for downstream_idx in topology.downstream_of(node_idx) {
        let all_upstream_exited = topology
            .upstream_of(downstream_idx)
            .all(|up| exited.contains(&up));

        if all_upstream_exited && !shutdown_sent.contains(&downstream_idx) {
            // Runtime decides WHEN; manager does the SENDING.
            pipeline_ctrl_msg_tx.send(PipelineControlMsg::ShutdownNode {
                node_id: downstream_idx,
                reason: format!("upstream node {} exited", node_idx),
            }).await;
            shutdown_sent.insert(downstream_idx);
        }
    }
}
// All nodes exited — drop our clone so the pipeline channel closes
// and the manager exits via channel closure (same behavior as today).
drop(pipeline_ctrl_msg_tx);
```

```rust
// Manager handling (in pipeline_ctrl.rs) — ~5 lines added to match block:
PipelineControlMsg::ShutdownNode { node_id, reason } => {
    let _ = self.control_senders.send(
        node_id,
        NodeControlMsg::Shutdown { deadline: shutdown_deadline, reason },
    );
}
```

**Why this split:** The runtime loop has `FuturesUnordered` (task handles) and
the pipeline topology, so it knows **when** each node should be shut down.
The `PipelineCtrlMsgManager` has `ControlSenders` (per-node senders), so it
can **deliver** the Shutdown. They communicate through the existing pipeline
control channel — no shared ownership, no new channels.

This replaces synthetic Shutdown entirely — the engine sends real Shutdown to
each node when appropriate based on topology.

### Drain API (compile-time enforcement)

```rust
pub struct DrainResult { /* private fields */ }

pub struct Drain<PData> {
    pdata: PdataReceiver<PData>,
}

impl Drain<PData> {
    /// Construct from MessageChannel or PdataReceiver.
    pub fn from_msg_chan(chan: MessageChannel<PData>) -> Self { ... }
    pub fn from_pdata(pdata: PdataReceiver<PData>) -> Self { ... }

    /// Pull next buffered pdata for custom handling.
    pub fn next_pdata(&mut self) -> Option<PData> { ... }

    /// Finalize after manual iteration. Must be called.
    pub fn finish(self) -> DrainResult { ... }

    /// Nack all remaining pdata, then finish.
    pub async fn nack_all(self, eh: &EffectHandler) -> DrainResult { ... }

    /// Drop all remaining pdata silently, then finish.
    pub fn drop_all(self) -> DrainResult { ... }
}
```

One unified type, two explicit constructors:

```rust
// Simple mode
Drain::from_msg_chan(msg_chan).nack_all(&eh).await

// Advanced mode
Drain::from_pdata(ch.pdata).nack_all(&eh).await
```

The exporter trait requires returning `DrainResult`:

```rust
async fn start(
    self: Box<Self>,
    msg_chan: MessageChannel<PData>,
    effect_handler: EffectHandler<PData>,
) -> Result<(TerminalState, DrainResult), Error>;
```

`DrainResult` has a private constructor — you can only produce it via
`Drain`. Forgetting to drain is a compile error.

### Ack/nack channel separation

Ack/nack is currently delivered via the per-node control channel
(`NodeControlMsg::Ack` / `NodeControlMsg::Nack`), routed through the
`PipelineCtrlMsgManager`. This mixes rare, critical control messages (Shutdown,
Config) with potentially bursty ack/nack traffic.

**The problem:** If 50 ack/nack messages are queued in the control channel when
Shutdown arrives, the node must process all 50 before it sees Shutdown — even
with `biased select!`, because the channel delivers messages in order. Bursty
ack/nack delays critical control messages.

**Solution:** Separate ack/nack into its own per-node channel, delivered by the
`PipelineCtrlMsgManager` on a different sender. Control channel stays small and
fast.

**Which nodes get which channels:**

| Node type | Control | Pdata (in) | Ack/Nack (in) | Rationale |
| --- | --- | --- | --- | --- |
| Receiver | Yes | No | Yes | Receives ack/nack to respond to callers |
| Processor | Yes | Yes | Yes | Receives ack/nack from downstream, forwards upstream |
| Exporter | Yes | Yes | No | Terminal node — sends ack/nack, never receives |

**Node select pattern:**

```rust
// Processor loop with three channels
loop {
    tokio::select! {
        biased;

        // Control always has priority (Shutdown, Config, TimerTick, etc.)
        // Never blocked by ack/nack bursts — separate channel.
        ctrl = control_rx.recv() => handle_control(ctrl?),

        // Pdata and ack/nack get fair alternation
        either = async {
            tokio::select! {
                pdata = pdata_rx.recv() => Either::Pdata(pdata),
                ack = acknack_rx.recv() => Either::AckNack(ack),
            }
        } => match either {
            Either::Pdata(pdata) => handle_pdata(pdata?),
            Either::AckNack(ack) => handle_acknack(ack?),
        },
    }
}
```

```rust
// Exporter loop — only two channels, no ack/nack receiver
loop {
    tokio::select! {
        biased;
        ctrl = control_rx.recv() => handle_control(ctrl?),
        pdata = pdata_rx.recv() => handle_pdata(pdata?),
    }
}
```

Outer `biased select!` → control always wins (never behind ack/nack).
Inner unbiased `select!` → pdata and ack/nack alternate fairly.
Exporters don't participate in ack/nack reception — they only send it.

**Routing:** Ack/nack still flows through the `PipelineCtrlMsgManager` for
routing (using context stack frames). The manager simply delivers to the
ack/nack sender instead of the control sender. This preserves the centralized
routing, `try_send` buffering, and fan-in/fan-out handling the manager already
provides.

**Considered alternative — direct reverse channels:** Creating per-edge reverse
channels (exporter → processor, processor → receiver) was explored. This would
bypass the manager entirely and reduce ack/nack latency. Fan-in works because
ack/nack carries the stamped pdata with routing context inside. However, it
distributes the `try_send` buffering and deadlock avoidance logic to every
node, and the wins are marginal since the manager already handles this well.
The manager approach is retained for simplicity.

### What `message.rs` becomes

`message.rs` becomes a toolkit of simple types with no hidden behavior.

#### Before vs After — `recv()` comparison

**Current** (~150 lines, state machine with 2 modes and 4 select blocks):

```rust
pub async fn recv_when(&mut self, accept_pdata: bool) -> Result<Message<PData>, RecvError> {
    let mut sleep_until_deadline: Option<Pin<Box<Sleep>>> = None;

    loop {
        // Check if already shutdown
        if self.control_rx.is_none() || self.pdata_rx.is_none() {
            return Err(RecvError::Closed);
        }

        // Closed-pdata detection probe (15 lines)
        if !accept_pdata && self.pdata_rx.as_ref().expect("...").is_empty() {
            if let Err(RecvError::Closed) = self.pdata_rx.as_mut().expect("...").try_recv() {
                self.shutdown();
                return Ok(Message::Control(NodeControlMsg::Shutdown {
                    deadline: Instant::now().add(Duration::from_secs(1)),
                    reason: "pdata channel closed".to_owned(),
                }));
            }
        }

        // Draining mode (50+ lines)
        if let Some(dl) = self.shutting_down_deadline {
            if self.pdata_rx.as_ref().expect("...").is_empty() { /* ... */ }
            if sleep_until_deadline.is_none() { /* ... */ }
            tokio::select! {
                biased;
                _ = sleep_until_deadline.as_mut().expect("...") => { /* deadline */ }
                ctrl = self.control_rx.as_mut().expect("...").recv() => { /* ... */ }
                pdata = self.pdata_rx.as_mut().expect("...").recv(), if accept_pdata => { /* ... */ }
            }
        }

        // Normal mode (30+ lines)
        tokio::select! {
            biased;
            ctrl = self.control_rx.as_mut().expect("...").recv() => {
                // Intercept Shutdown → enter draining mode → continue
                // Or return control message
            }
            pdata = self.pdata_rx.as_mut().expect("...").recv(), if accept_pdata => {
                // Stamp received_at_node
                // Handle Err(Closed) → synthetic Shutdown
            }
        }
    }
}
```

**Proposed** (~15 lines, single select, no state):

```rust
pub async fn recv(&mut self) -> Result<Message<PData>, RecvError> {
    let ack_allowed = self.ack_streak < ACK_BURST_LIMIT;

    tokio::select! {
        biased;
        ctrl = self.control.recv() => Ok(Message::Control(ctrl?)),
        ack = self.recv_acknack(), if ack_allowed => {
            self.ack_streak += 1;
            ack
        },
        pdata = self.pdata.recv() => {
            self.ack_streak = 0;
            Ok(Message::PData(pdata?))
        },
    }
}
```

#### What moved where

| Removed from `message.rs` | Where it went |
| --- | --- |
| Draining state machine | Engine phased shutdown (`runtime_pipeline.rs`) |
| Deadline enforcement | Engine global deadline + per-phase timeout |
| Synthetic Shutdown on closed pdata | Engine sends real Shutdown via `FuturesUnordered` topology cascade |
| `accept_pdata` gating | Simplified — `recv_when(accept_pdata)` is just an `if` guard; advanced nodes use `into_channels()` |
| Closed-pdata detection probe | Not needed — engine guarantees Shutdown before pdata close (requires phased shutdown) |
| `received_at_node` stamping | `PdataReceiver` wrapper (stamps on `.recv()` transparently) |
| `node_id` / `interests` fields | Moved to `PdataReceiver` |
| `recv_when` method | Simplified — just an `if` guard on pdata branch, no complex probe or drain interaction |
| `pending_shutdown` / `shutting_down_deadline` | Not needed — Shutdown is immediate, drain is via `Drain` |

#### Simple exporter — before vs after

**Before:**

```rust
impl Exporter<OtapPdata> for NoopExporter {
    async fn start(
        self: Box<Self>,
        mut msg_chan: MessageChannel<OtapPdata>,
        effect_handler: EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        loop {
            // msg_chan.recv() hides: draining, deadline, synthetic shutdown,
            // accept_pdata gating, received_at_node stamping, closed-pdata probe
            match msg_chan.recv().await? {
                Message::Control(NodeControlMsg::Shutdown { .. }) => break,
                Message::PData(data) => println!("{:?}", data),
                _ => {}
            }
        }
        Ok(TerminalState::default())
    }
}
```

**After:**

```rust
impl Exporter<OtapPdata> for NoopExporter {
    async fn start(
        self: Box<Self>,
        mut msg_chan: MessageChannel<OtapPdata>,
        effect_handler: EffectHandler<OtapPdata>,
    ) -> Result<(TerminalState, DrainResult), Error> {
        loop {
            match msg_chan.recv().await? {
                Message::Control(NodeControlMsg::Shutdown { .. }) => {
                    return Ok((TerminalState::default(), Drain::from_msg_chan(msg_chan).nack_all(&effect_handler).await));
                }
                Message::PData(data) => println!("{:?}", data),
                _ => {}
            }
        }
    }
}
```

`return` works here because the `&mut self` borrow from `recv().await` is
temporary — it ends when the `match` resolves, so `msg_chan` is unborrowed
when `drain()` consumes it.

Difference: `DrainResult` is returned (compiler enforces drain handling).
The `recv()` behind the scenes is ~15 lines, not ~150.

#### Advanced exporter — before vs after

**Before** (Azure Monitor exporter, simplified):

```rust
// msg_chan.recv_when(accepting_pdata) hides the draining/deadline/synthetic
// logic but the exporter ALSO has its own select! around it — double layering.
loop {
    tokio::select! {
        biased;
        _ = token_refresh => { ... },
        completed = in_flight.next() => { ... },
        msg = msg_chan.recv_when(accepting_pdata) => match msg { ... },
    }
}
```

**After:**

```rust
// into_channels() gives raw typed receivers — one select!, no double layering.
let Channels { mut control, mut pdata, acknack } = msg_chan.into_channels();
loop {
    tokio::select! {
        biased;
        ctrl = control.recv() => match ctrl? {
            NodeControlMsg::Shutdown { .. } => {
                self.flush(&effect_handler).await?;
                return Ok((TerminalState::new(...),
                    Drain::from_pdata(pdata).nack_all(&effect_handler).await));
            },
            _ => {}
        },
        _ = token_refresh => { ... },
        completed = in_flight.next() => { ... },
        p = pdata.recv(), if accepting_pdata => { ... },
    }
}
```

No `recv_when` wrapper around a wrapper. Flat select with full control.

#### Keeps (simplified or new)

- `Message<PData>` enum — unified return type for `MessageChannel::recv()`
- `Sender<T>` / `Receiver<T>` enums — local/shared channel abstraction
- `MsgReceiver<T>` trait — generic receiver interface
- `MessageChannel` — simplified to ~15 lines (biased select, no state machine);
  convenience wrapper for simple nodes
- `ControlReceiver<PData>` — typed wrapper for the control channel
- `PdataReceiver<PData>` — wrapper with implicit `received_at_node` stamping
- `AckNackReceiver<PData>` — typed wrapper for the ack/nack channel
- `Channels<PData>` — named struct returned by `MessageChannel::into_channels()`
- `Drain` / `DrainResult` — drain API with compile-time enforcement

**Channel types — consistent naming:**

```rust
/// Typed control channel receiver.
pub struct ControlReceiver<PData> { inner: CtrlRx, ... }

/// Pdata channel receiver with implicit `received_at_node` stamping.
pub struct PdataReceiver<PData> { inner: DataRx, node_id: usize, interests: Interests, ... }

/// Ack/nack channel receiver.
pub struct AckNackReceiver<PData> { inner: AckNackRx, ... }

/// All channels for a node, returned by `MessageChannel::into_channels()`.
pub struct Channels<PData> {
    pub control: ControlReceiver<PData>,
    pub pdata: PdataReceiver<PData>,
    pub acknack: Option<AckNackReceiver<PData>>,
}
```

All types expose `.recv()`, `.try_recv()`. Consistent `*Receiver` suffix.

**Removes:**

- Draining state machine (`shutting_down_deadline`, `pending_shutdown`, two
  separate `select!` blocks) — ~60 lines
- Deadline enforcement (`sleep_until_deadline` timer) — ~15 lines
- Synthetic Shutdown on closed pdata — ~20 lines
- `accept_pdata` gating and closed-pdata detection probe — ~40 lines
  (simplified to a single `if` guard on the pdata branch)
- `node_id` and `interests` fields on `MessageChannel` (moved to
  `PdataReceiver`)
- `recv_when` method — simplified to a single `if` guard, no complex probe
  or drain interaction

**Simple mode** — `MessageChannel::recv()` implementation:

```rust
/// Maximum consecutive ack/nack messages before forcing a pdata turn.
const ACK_BURST_LIMIT: u8 = 32;

pub struct MessageChannel<PData> {
    control: ControlReceiver<PData>,
    pdata: PdataReceiver<PData>,
    acknack: Option<AckNackReceiver<PData>>,
    ack_streak: u8,
}

impl<PData: ReceivedAtNode> MessageChannel<PData> {
    /// Priority: control > ack/nack > pdata, with burst protection.
    ///
    /// - Control is always checked first (biased).
    /// - Ack/nack has priority over pdata because processing acks frees
    ///   backpressure capacity and processing nacks triggers retries.
    /// - After ACK_BURST_LIMIT consecutive ack/nack without a pdata turn,
    ///   the ack branch is disabled for one iteration to prevent starvation.
    ///   Processing one pdata resets the streak.
    pub async fn recv(&mut self) -> Result<Message<PData>, RecvError> {
        self.recv_when(true).await
    }

    /// Like [`recv()`](Self::recv), but with an `accept_pdata` guard.
    ///
    /// When `accept_pdata` is `false`, only control and ack/nack messages
    /// are returned. Pdata stays in the channel, providing natural
    /// backpressure to upstream nodes.
    pub async fn recv_when(&mut self, accept_pdata: bool) -> Result<Message<PData>, RecvError> {
        let ack_allowed = self.ack_streak < ACK_BURST_LIMIT;

        tokio::select! {
            biased;
            ctrl = self.control.recv() => Ok(Message::Control(ctrl?)),
            ack = self.recv_acknack(), if ack_allowed => {
                self.ack_streak += 1;
                ack
            },
            pdata = self.pdata.recv(), if accept_pdata => {
                self.ack_streak = 0;
                Ok(Message::PData(pdata?))
            },
        }
    }

    async fn recv_acknack(&mut self) -> Result<Message<PData>, RecvError> {
        match &mut self.acknack {
            Some(rx) => Ok(Message::AckNack(rx.recv().await?)),
            None => std::future::pending().await,
        }
    }

    /// Consume self, return individual channels for advanced usage.
    pub fn into_channels(self) -> Channels<PData> {
        Channels {
            control: self.control,
            pdata: self.pdata,
            acknack: self.acknack,
        }
    }
}
```

**`Channels` struct** — returned by `into_channels()`:

```rust
pub struct Channels<PData> {
    pub control: ControlReceiver<PData>,
    pub pdata: PdataReceiver<PData>,
    pub acknack: Option<AckNackReceiver<PData>>,
}
```

**`Drain`** — compile-time drain enforcement:

```rust
pub struct DrainResult {
    _private: (), // private field — can't construct outside this module
}

#[must_use = "Drain must be finalized via finish(), nack_all(), or drop_all()"]
pub struct Drain<PData> {
    pdata: PdataReceiver<PData>,
}

impl<PData> Drain<PData> {
    /// Construct from MessageChannel (simple mode).
    pub fn from_msg_chan(chan: MessageChannel<PData>) -> Self {
        Drain { pdata: chan.pdata }
    }

    /// Construct from PdataReceiver (advanced mode).
    pub fn from_pdata(pdata: PdataReceiver<PData>) -> Self {
        Drain { pdata }
    }

    /// Pull the next buffered pdata, or None if channel is empty.
    /// Use in a `while let Some(pdata) = drain.next_pdata()` loop
    /// for custom drain handling.
    pub fn next_pdata(&mut self) -> Option<PData> {
        self.pdata.try_recv().ok()
    }

    /// Finalize the drain. Must be called after manual iteration
    /// via `next_pdata()`. Consumes self, produces DrainResult.
    pub fn finish(self) -> DrainResult {
        DrainResult { _private: () }
    }

    // --- Shortcuts (call finish() internally) ---

    /// Nack all remaining pdata, then finish.
    pub async fn nack_all(mut self, eh: &EffectHandler<PData>) -> DrainResult {
        while let Some(pdata) = self.next_pdata() {
            let _ = eh.notify_nack(NackMsg::new("shutdown", pdata)).await;
        }
        self.finish()
    }

    /// Drop all remaining pdata silently, then finish.
    pub fn drop_all(mut self) -> DrainResult {
        while self.next_pdata().is_some() {}
        self.finish()
    }
}
```

**Usage patterns:**

```rust
// Simple: nack everything
Drain::from_msg_chan(msg_chan).nack_all(&eh).await

// Simple: discard everything
Drain::from_msg_chan(msg_chan).drop_all()

// Custom: process remaining pdata during drain
let mut drain = Drain::from_msg_chan(msg_chan);
while let Some(pdata) = drain.next_pdata() {
    self.process(pdata, &mut effect_handler).await?;
}
drain.finish()

// Advanced mode: same pattern, different constructor
Drain::from_pdata(pdata).nack_all(&eh).await
```

Two constructors: `Drain::from_msg_chan()` or `Drain::from_pdata()`.
Three finalization paths: `nack_all()`, `drop_all()`, or `next_pdata()` loop + `finish()`.
Compile-time enforced — `DrainResult` has a private constructor, `Drain` is `#[must_use]`.

**Typed receiver wrappers:**

```rust
/// Control messages only (Shutdown, Config, TimerTick, CollectTelemetry).
pub struct ControlReceiver<PData> {
    inner: /* MsgReceiver<ControlMsg<PData>> */,
}

/// Pdata with implicit received_at_node stamping.
pub struct PdataReceiver<PData> {
    inner: /* MsgReceiver<PData> */,
    node_id: usize,
    interests: Interests,
}

/// Ack/nack messages only.
pub struct AckNackReceiver<PData> {
    inner: /* MsgReceiver<AckNackMsg<PData>> */,
}
```

Each exposes `.recv()` and `.try_recv()`. `PdataReceiver` stamps automatically.
All are generic over the underlying local/shared receiver via `MsgReceiver<T>`.

Both modes get implicit `received_at_node` stamping via `PdataReceiver`.
Both must return `DrainResult` (compile-time enforced).

### `received_at_node` stamping via `PdataReceiver`

Stamping stays on the receive side (preserving accurate entry timestamps that
exclude channel buffering time) but moves out of `MessageChannel` into
`PdataReceiver`:

```rust
pub struct PdataReceiver<PData, DataRx> {
    inner: DataRx,
    node_id: usize,
    interests: Interests,
}

impl PdataReceiver<PData, DataRx> {
    pub async fn recv(&mut self) -> Result<PData, RecvError> {
        let mut pdata = self.inner.recv().await?;
        pdata.received_at_node(self.node_id, self.interests);
        Ok(pdata)
    }
}
```

The engine wraps the raw pdata channel in `PdataReceiver` before passing it to
the node (either directly or inside `MessageChannel`). Nodes call `.recv()` as
normal — stamping is invisible. This is critical because the entry frame
enables ack/nack unwinding through the `PipelineCtrlMsgManager`, so it must
happen before the node processes the pdata.

Moving to the send side was considered but rejected because:

- Entry timestamps would include channel buffer wait time (wrong metric)
- The sender doesn't know the destination node's `Interests` flags

## Migration Path

1. ~~Unify shared/local `MessageChannel` (generic over receiver type)~~ — Done.
2. Add `PdataReceiver` wrapper for implicit `received_at_node` stamping.
3. Add `Drain` / `DrainResult` types.
4. Implement three-channel wiring (control, pdata, ack/nack) for processors
   and receivers; two-channel (control, pdata) for exporters.
5. Simplify `MessageChannel::recv` to trivial biased select (no draining),
   backed by `PdataReceiver`.
6. Add `recv_when(accept_pdata)` with simple `if` guard.
7. Add `into_channels()` to `MessageChannel` for advanced node access.
8. Update exporter/processor traits for new signatures (return `DrainResult`).
9. Implement phased shutdown in `runtime_pipeline.rs`.
10. Remove synthetic Shutdown, deadline logic, closed-pdata probe from
    `MessageChannel`.
11. Update all node implementations.

## Ack/Nack Semantics

Two possible contracts — must be consistent per pipeline:

| Contract | Ack means | Requires |
| --- | --- | --- |
| **"Exported"** (Contract A) | Data reached external system | Full ack/nack chain, phased shutdown |
| **"Accepted"** (Contract B) | Pipeline accepted responsibility | Durable buffer processor, early ack |

Contract A is the current default. Contract B requires an explicit durable
buffer processor in the pipeline. The shutdown design supports both — Contract A
needs the full phased sequence, Contract B is more forgiving since ack goes to
the caller before export.

## Open Questions

1. **Phase timing budget** — Currently a single global deadline (default 60s,
   set via admin HTTP endpoint). Should phased shutdown keep the single global
   deadline wrapping all phases, or introduce per-phase budgets? Single global
   is simplest and matches current behavior.
2. **Processor-owned loops** — Should processors also be able to own their
   event loop (like exporters), or always be engine-managed?
3. **Connector nodes** — Not yet implemented. How do they fit into the
   topological shutdown order?

## Resolved Design Decisions

- **StopIngesting** — New `NodeControlMsg::StopIngesting` variant. Receivers
  handle it by stopping their listener, finishing in-flight requests, and
  staying alive for ack/nack until they receive `Shutdown`.
- **ControlSenders ownership** — Runtime loop decides when to shut down each
  node (topology + task handles); `PipelineCtrlMsgManager` does the sending
  (owns `ControlSenders`). Communication via new `PipelineControlMsg::ShutdownNode`
  on the existing pipeline control channel. No shared ownership needed.
- **Ack/nack channel separation** — Delivered by the manager on a separate
  per-node sender. Control channel stays small for critical messages.
- **`received_at_node` stamping** — Stays on receive side via `PdataReceiver`.
  Send-side stamping rejected (wrong timestamps, missing `Interests` flags).
