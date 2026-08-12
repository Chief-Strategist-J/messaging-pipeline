# Decision Record: 20260812-005-stream-processor-windowstore-rocksdb-eviction.md

## Status
Accepted

## Date
2026-08-12

## Context & Problem Statement
In `stream-processor` (`DedupProcessor.kt`), scheduled punctuation executed `store.all()` across the `dedup-store` RocksDB KeyValueStore every 10 minutes.
- **Fatal Flaw**: Performing `store.all()` key-range iteration across millions of deduplication keys directly on the active StreamThread blocked processing for extended durations.
- **Consequences**: Triggered Kafka Consumer Group Rebalances, partition revocation, and stream processor crashes due to missed heartbeats (`max.poll.interval.ms` exceeded).

## Decision & Implementation Details
1. **Migrated from `KeyValueStore` to `WindowStore`**:
   - Updated `DedupTransformer` to consume `WindowStore<String, Long>`.
   - Replaced `store.all()` punctuation scan loop with native RocksDB segment window eviction (`persistentWindowStore`).
2. **Windowed Key Lookup**:
   - Replaced full scans with `store.fetch(eventId, timeFrom, eventTime)` to query specific event deduplication windows in $O(\log N)$ time.
3. **Automated Window Retention**:
   - Configured `Stores.windowStoreBuilder` in `TopologyBuilder.kt` with a 1-day retention window (`Duration.ofDays(1)`). RocksDB automatically purges expired segment files in the background without blocking StreamThreads.

## Verification
- Compiled and built `stream-processor` container cleanly.
- Updated running container `event-platform-stream-processor-1`.
- Committed and pushed to all active git remotes including `repo-varun147G` via PAT authentication.
