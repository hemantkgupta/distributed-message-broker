# distributed-message-broker

A Java 17 multi-module reference implementation of a partition-replicated,
log-centric message broker. Built from scratch using JDK APIs only (no JNI)
to keep every architectural claim traceable to source.

This repo is the companion to the CSE-Raw wiki topic
[distributed-message-broker](https://github.com/hemantkgupta/CSE-Raw/tree/main/wiki/implementations/distributed-message-broker.md);
the wiki contains the narrative blogs, paper deep-dives, concept pages, and
hand-tuned SVG diagrams that this code makes concrete.

## What it implements

Following the canonical Apache Kafka model (NetDB 2011) updated with the
2026 modern envelope (KRaft, leader epochs, sparse indexes, zero-copy via
`FileChannel.transferTo`):

| Module | Purpose |
|---|---|
| `broker-common` | Records, value types, framing primitives shared across modules. |
| `broker-storage` | Append-only segmented logs, sparse offset index, page-cache-as-store writes, filesystem-backed tiered segment offload. |
| `broker-replica` | Partition replica state machine: ISR, high-watermark, leader-epoch-based truncation, follower HWM propagation. |
| `broker-network` | NIO socket server, request waiting list, `FileChannel.transferTo` zero-copy fetch. |
| `broker-coordinator` | Consumer-group coordinator: JoinGroup / SyncGroup / Heartbeat / LeaveGroup; `__consumer_offsets`; cooperative rebalance planner. |
| `broker-metadata` | Embedded KRaft-style metadata log: controller, snapshot, broker registration, rack-aware closest replica selection. |
| `broker-client` | Smart producer (batching, idempotency, retries) + pull-based consumer (offset tracking). |
| `broker-simulator` | In-process cluster harness + chaos: network partitions, broker crashes, slow followers. |

## Building

The user pins Java via `jenv local 17.0`. Build with the wrapper:

```bash
./gradlew build --no-daemon --console=plain
```

All tests run under `./gradlew test`. There is no external dependency on
Kafka, ZooKeeper, RocksDB, or any C-extension library — the entire stack
is pure JDK 17.

## Pedagogical departures from production Kafka

- **No JNI / no native libraries.** Compression (when used) is the JDK's
  `Deflater`, not Snappy / LZ4 / Zstd.
- **In-process cluster.** Brokers run as objects inside a single JVM, with
  a `ChannelMux` simulating the network. Real socket transport exists in
  `broker-network`'s tests but the simulator harness wires brokers in-memory
  for fast deterministic test runs.
- **Tiered storage is local-filesystem backed.** The code models KIP-405's
  sealed-segment copy, COPY_SEGMENT_FINISHED visibility, delete-after-copy
  invariant, and cold readback path without depending on S3/GCS SDKs.
- **Follower fetching is rack-aware but simulator-scoped.** Metadata chooses
  a closest in-sync replica by rack; the in-process endpoint routes reads to it.
  There is no real inter-broker network cost model.
- **Cooperative rebalance is a planner, not a full wire protocol.** The
  coordinator computes sticky assignment deltas (revoked/added) so tests can
  verify KIP-429's core property; it does not implement client-side two-round
  callbacks.
- **No transactions / exactly-once.** Single-partition idempotency via
  producer epochs is supported; multi-partition transactions (KIP-447) are
  out of scope.
- **No TLS / SASL.** Network layer is unencrypted.
- **Simplified KRaft.** Metadata controller is a single-process raft-like
  state machine, not a 3-node controller quorum. Captures the protocol
  shape; doesn't exercise true network consensus.

These departures preserve the load-bearing architecture (page cache,
sparse index, ISR, leader epoch, zero-copy fetch, group coordinator,
metadata log) while keeping the codebase readable and dependency-free.

## Reading order

1. `broker-common` — types
2. `broker-storage` — append + read with sparse index
3. `broker-replica` — leader/follower + ISR + HWM
4. `broker-network` — zero-copy fetch over a real socket
5. `broker-coordinator` — consumer group state machine
6. `broker-metadata` — KRaft sim
7. `broker-client` — producer batching + consumer pull loop
8. `broker-simulator` — chaos + e2e

## Co-author

Co-authored with Claude (Opus 4.7, 1M context).
