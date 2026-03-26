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

1. **Simplify `MessageChannel`** — reduce it to a trivial biased select with
   no draining state machine, no synthetic shutdown, no deadline enforcement.
2. **Deterministic graceful shutdown** — two-phase shutdown with proper
   topological ordering that drains pdata and ensures ack/nack flows back
   correctly before nodes exit. Draining is handled by the shutdown sequence,
   not by `MessageChannel`.

**Note:** This document builds on the control/completion channel separation
from [#2370](https://github.com/open-telemetry/otel-arrow/pull/2370).

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

## Proposed Design

### Core proposal: Simplify `MessageChannel`

The `MessageChannel` is simplified from a ~200-line async state machine to a
~15-line biased select. All shutdown/drain complexity moves to the engine's
shutdown orchestrator.

### Control message changes

`Shutdown` is renamed to `EndShutdown` and the `deadline` field is removed —
global deadline is enforced by the engine via `tokio::time::timeout`. A new
`BeginShutdown` variant is added. All nodes receive the same two shutdown
messages — **no node-type discrimination**:

```rust
pub enum NodeControlMsg<PData> {
    Config { config: Value },
    TimerTick {},
    CollectTelemetry { metrics_reporter: MetricsReporter },
    DelayedData { when: Instant, data: Box<PData> },

    // New
    BeginShutdown { done: oneshot::Sender<()>, reason: String },
    // Replaces Shutdown { deadline, reason }
    EndShutdown { reason: String },
}
```

Ack/Nack messages are no longer part of `NodeControlMsg` — they arrive on the
separate completion channel introduced in [#2370](https://github.com/open-telemetry/otel-arrow/pull/2370).

Each node type handles these differently:

| Node type | BeginShutdown | EndShutdown |
| --- | --- | --- |
| **Receiver** | Stop connections, finish processing incoming requests, signal done | Process final ack/nack, send responses to callers, exit |
| **Processor** | Flush internal state, forward downstream, signal done | Process final ack/nack, forward upstream, exit |
| **Exporter** | Drain pdata, flush HTTP, send ack/nack, signal done | Exit |

Exporters do all their work during `BeginShutdown` and simply exit on
`EndShutdown`. This means existing exporters need to migrate their `Shutdown`
handler logic into `BeginShutdown` rather than a 1:1 rename to `EndShutdown`.

`BeginShutdown` is where all draining and clearing of internal state happens.
Each node flushes its buffered data downstream, completes any in-progress work,
and signals done — but does not exit. This ensures the pipeline is fully
drained before `EndShutdown` triggers the exit sequence.

### Processor `ProcessResult`

```rust
pub enum ProcessResult {
    /// Normal processing — continue the loop.
    Continue,
    /// BeginShutdown handled — state flushed, oneshot signaled.
    BeginShutdownComplete,
    /// EndShutdown handled — processor is done, break the loop.
    Exit,
}
```

The processor trait changes from `Result<(), Error>` to
`Result<ProcessResult, Error>`. The engine wrapper reacts:

```rust
while let Ok(msg) = message_channel.recv_when(processor.accept_pdata()).await {
    match processor.process(msg, &mut effect_handler).await? {
        ProcessResult::Exit => break,
        _ => {}
    }
}
_ = telemetry_cancel_handle.cancel().await;
processor.process(
    Message::Control(NodeControlMsg::CollectTelemetry { metrics_reporter }),
    &mut effect_handler,
).await?;
```

### Shutdown sequence (two-phase, dependency-aware)

``` md
Phase 1 — BeginShutdown (forward topological order):

  Engine sends BeginShutdown to each node only after ALL its upstream
  nodes have signaled done via their oneshot.

  1. Receivers: stop connections, finish processing incoming requests, signal done, stay alive.
  2. Engine waits for all receiver oneshots.
  3. Processors: flush internal state, forward downstream, signal done, stay alive.
  4. Engine waits for all processor oneshots.
  5. Exporters: drain pdata, flush HTTP, send ack/nack, signal done, stay alive.
  6. Engine waits for all exporter oneshots.

  At this point: all pdata processed, all ack/nack sent by exporters.

Phase 2 — EndShutdown (reverse topological order):

  7. Exporters get EndShutdown → exit.
  8. Engine waits for exporter exits. Ack/nack flows to processors.
  9. Processors get EndShutdown (after all downstream exited) →
     process ack/nack, forward upstream, exit.
  10. Engine waits for processor exits. Ack/nack flows to receivers.
  11. Receivers get EndShutdown → process ack/nack, respond to callers, exit.

Global deadline: tokio::time::timeout wrapping the entire sequence.
If exceeded, all remaining tasks are aborted.
```

### Engine-driven shutdown orchestration

**Phase 1** uses oneshot channels:

```rust
for layer in topology.forward_layers() {
    let mut done_handles = Vec::new();
    for node_id in layer {
        let (tx, rx) = oneshot::channel();
        pipeline_ctrl_msg_tx.send(PipelineControlMsg::SendControlMsg {
            node_id,
            msg: NodeControlMsg::BeginShutdown { done: tx, reason: reason.clone() },
        }).await;
        done_handles.push(rx);
    }
    futures::future::join_all(done_handles).await;
}
```

**Phase 2** uses `FuturesUnordered` + topology cascade:

```rust
for exporter_id in topology.exporters() {
    pipeline_ctrl_msg_tx.send(PipelineControlMsg::SendControlMsg {
        node_id: exporter_id,
        msg: NodeControlMsg::EndShutdown { reason: reason.clone() },
    }).await;
}

let mut exited: HashSet<usize> = HashSet::new();
while let Some((node_idx, result)) = tagged_futures.next().await {
    exited.insert(node_idx);
    for upstream_idx in topology.upstream_of(node_idx) {
        let all_downstream_exited = topology
            .downstream_of(upstream_idx)
            .all(|down| exited.contains(&down));
        if all_downstream_exited && !shutdown_sent.contains(&upstream_idx) {
            pipeline_ctrl_msg_tx.send(PipelineControlMsg::SendControlMsg {
                node_id: upstream_idx,
                msg: NodeControlMsg::EndShutdown { reason: reason.clone() },
            }).await;
            shutdown_sent.insert(upstream_idx);
        }
    }
}
drop(pipeline_ctrl_msg_tx);
```

**Delivery** via `PipelineControlMsg::SendControlMsg { node_id, msg }`. The
manager owns `ControlSenders` and does the actual send. The runtime loop
decides **when**; the manager does the **sending**.

### What `MessageChannel` becomes

**Proposed** (~15 lines, single select, no state):

This uses the burst-limit protection from [#2370](https://github.com/open-telemetry/otel-arrow/pull/2370)
but applies it to the completion (ack/nack) channel directly by exposing
completion channel to message channel and never blocks the
node-control channel.

```rust
pub async fn recv_when(&mut self, accept_pdata: bool) -> Result<Message<PData>, RecvError> {
    let ack_allowed = self.ack_streak < ACK_BURST_LIMIT;
    tokio::select! {
        biased;
        ctrl = self.control.recv() => Ok(Message::Control(ctrl?)),
        ack = self.completion.recv(), if ack_allowed => {
            self.ack_streak += 1;
            Ok(Message::Completion(ack?))
        },
        pdata = self.pdata.recv(), if accept_pdata => {
            self.ack_streak = 0;
            Ok(Message::PData(pdata?))
        },
    }
}
```

`recv()` delegates to `recv_when(true)`.

### `received_at_node` stamping via `PdataReceiver`

Stays on receive side via `PdataReceiver` wrapper. Stamps automatically on
`.recv()`.

## Migration Path

1. ~~Unify shared/local `MessageChannel` (generic over receiver type)~~ — Done.
2. Add `PdataReceiver` wrapper for `received_at_node` stamping.
3. Rename `Shutdown` to `EndShutdown`, add `BeginShutdown`, remove `deadline`.
4. Add `ProcessResult` return type to processor `process()` trait.
5. Add `PipelineControlMsg::SendControlMsg` for targeted delivery.
6. Simplify `MessageChannel::recv` to trivial biased select.
7. Implement two-phase shutdown orchestration in `runtime_pipeline.rs`.
8. Remove synthetic Shutdown, deadline logic, closed-pdata probe.
9. Update all node implementations.

## Open Questions

1. **Phase timing budget** — Single global deadline vs per-phase budgets.
2. **Connector nodes** — How do they fit in the topological shutdown order?
3. **Node-specific shutdown messages** — The current proposal uses one
   `BeginShutdown`/`EndShutdown` pair for all node types, but each node type
   has a different contract (e.g., exporters do all work in `BeginShutdown`
   and just exit on `EndShutdown`). This makes migration error-prone since
   the compiler can't enforce what each node type should do in each phase.
   Options to address this:
   - **Fully node-specific**: distinct `NodeControlMsg` types per node type
     (e.g., `ReceiverControlMsg`, `ExporterControlMsg`) each with their own
     shutdown variants encoding exact expectations in the type system.
   - **Partially node-specific via generics**: `NodeControlMsg<ShutdownPayload>`
     where `ShutdownPayload` is a generic type parameter that varies by node
     type (e.g., `ReceiverShutdown`, `ExporterShutdown`). The enum stays small,
     but `BeginShutdown { done, payload: ShutdownPayload }` carries
     node-type-specific instructions. The engine constructs the right payload
     per node; the node destructures its own type.
   Trade-off: type safety and migration clarity vs. enum size and complexity.
4. **`into_channels()` escape hatch** — Should `MessageChannel` expose an
   `into_channels()` method that decomposes it into raw control, ack/nack,
   and pdata receivers? This would let advanced nodes build their own
   `select!` loop when the default biased select doesn't fit. Trade-offs:
   more API surface, risk of nodes bypassing burst protection, and loss of
   encapsulation — any future changes to `MessageChannel` internals (e.g.,
   channel types, priority logic, new channel additions) would break nodes
   that decomposed into raw channels, whereas nodes using `recv_when()`
   get those changes for free.
