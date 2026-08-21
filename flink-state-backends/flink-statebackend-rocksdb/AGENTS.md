<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# flink-statebackend-rocksdb

The RocksDB keyed-state backend: an embedded RocksDB instance per task that holds keyed state off-heap (one column family per registered state) and supports incremental checkpointing. This is the primary production state backend for large keyed state. It depends on `frocksdbjni` (Ververica's FRocksDB fork) for the native library and JNI bindings (`org.rocksdb.*`).

## Build Commands

```
./mvnw clean install -DskipTests -pl flink-state-backends/flink-statebackend-rocksdb -am
```

After the first `-am` build, drop `-am` for faster rebuilds when only changing code within this module. Run a single test with `./mvnw -pl flink-state-backends/flink-statebackend-rocksdb -Dtest=RocksDBResourceContainerTest test`.

Run `./mvnw spotless:apply` after edits (google-java-format, AOSP style). This module **is** checkstyle-enforced — it inherits the root `maven-checkstyle-plugin` `validate`-phase binding (`tools/maven/checkstyle.xml`) with no module-level opt-out (see root [AGENTS.md](../../AGENTS.md)). Do not suppress checkstyle; fix the code.

## Key Directory Structure

Under `flink-state-backends/flink-statebackend-rocksdb/src/main/java/org/apache/flink/state/rocksdb/`:

- (top level) — the backend entry point, builder, state primitives, config holders, and native-resource lifecycle (`EmbeddedRocksDBStateBackend`, `RocksDBKeyedStateBackend`, `RocksDB*State`, `RocksDBResourceContainer`, ...)
- `snapshot/` — checkpoint snapshot strategies: `RocksDBSnapshotStrategyBase` and the incremental / native-full implementations
- `restore/` — restore operations that rebuild the RocksDB instance from state handles (incremental, full, none)
- `iterator/` — `RocksIterator`-backed key/namespace iteration used by state queries and full snapshots
- `ttl/` — `RocksDbTtlCompactFiltersManager`, wiring state TTL into RocksDB compaction filters
- `sstmerge/` — manual SST-file compaction scheduling for small state (`RocksDBManualCompactionManager` and friends)
- `org/apache/flink/contrib/streaming/state/` — **deprecated** thin subclasses kept under the old package for backwards compatibility (e.g. `EmbeddedRocksDBStateBackend`, `RocksDBOptions`); new code lives in `org.apache.flink.state.rocksdb`

## Key Abstractions

- **`EmbeddedRocksDBStateBackend`** (`@PublicEvolving`, extends `AbstractManagedMemoryStateBackend`, implements `ConfigurableStateBackend`) — the user-facing `StateBackend` entry point. Carries the incremental-checkpointing flag, `PriorityQueueStateType`, memory/options configuration, and `createKeyedStateBackend(...)` which constructs a `RocksDBKeyedStateBackendBuilder`. Instantiated from config by `EmbeddedRocksDBStateBackendFactory`. There is no longer a separate `RocksDBStateBackend`; the deprecated predecessors are the `org.apache.flink.contrib.streaming.state` subclasses.
- **`RocksDBKeyedStateBackend<K>`** (extends `AbstractKeyedStateBackend<K>`) — owns the RocksDB instance and the `ColumnFamilyHandle` per registered state; `createState`/`updateState` produce the state primitives; routes snapshots through the configured `RocksDBSnapshotStrategyBase`.
- **`RocksDBKeyedStateBackendBuilder<K>`** (extends `AbstractKeyedStateBackendBuilder<K>`) — assembles the backend: opens/restores the DB (selecting a `RocksDBRestoreOperation`), and wires the snapshot strategy (`RocksIncrementalSnapshotStrategy` when incremental checkpointing is on, else `RocksNativeFullSnapshotStrategy`).
- **State primitives** — `AbstractRocksDBState<K, N, V>` (implements `InternalKvState` + `State`) is the base; concrete impls `RocksDBValueState`, `RocksDBListState`, `RocksDBMapState`, `RocksDBReducingState`, `RocksDBAggregatingState` (the last two via `AbstractRocksDBAppendingState`). Each implements the matching `Internal*State` interface and exposes static `create`/`update` factories. Keys/namespaces/values are serialized to `byte[]` and stored in the state's column family.
- **`RocksDBPriorityQueueSetFactory`** (implements `PriorityQueueSetFactory`) — RocksDB-backed timer/priority-queue storage (the alternative to the heap-based queue), selected via `PriorityQueueStateType`.
- **`RocksDBResourceContainer`** (`AutoCloseable`) — the single entry point for obtaining RocksDB `Options`/`ColumnFamilyOptions`/`DBOptions` (from `PredefinedOptions` + an optional `RocksDBOptionsFactory`) and shared resources; must be closed to avoid native leaks.
- **`RocksDBOperationUtils`** — helpers to create column families and `RocksDbKvStateInfo` state info, and to manage shared caches/write buffers.
- **`RocksDBWriteBatchWrapper`** (`AutoCloseable`) and **`RocksIteratorWrapper`** — thin wrappers over `org.rocksdb.WriteBatch` / `RocksIterator` that centralize native-handle lifecycle.
- **Config holders** — `RocksDBOptions` and `RocksDBConfigurableOptions` (`ConfigOption` definitions), `PredefinedOptions` (named tuning presets, enum), `RocksDBOptionsFactory` / `ConfigurableRocksDBOptionsFactory` (user hook to customize RocksDB options), `RocksDBMemoryConfiguration` / `RocksDBMemoryControllerUtils` (managed-memory sharing), `RocksDBNativeMetricOptions` / `RocksDBNativeMetricMonitor` (RocksDB property metrics), `RocksDBPriorityQueueConfig`.

## Common Change Patterns

### Adding or altering a state primitive

Extend `AbstractRocksDBState<K, N, V>` (or `AbstractRocksDBAppendingState` for reducing/aggregating semantics), implement the relevant `Internal*State` interface, and provide the static `create`/`update` factories that `RocksDBKeyedStateBackend.createState`/`updateState` dispatch to. Keep the on-disk key/value byte layout stable; any change to how keys, namespaces, or values are serialized is a **state-compatibility change — ask first** (see root [AGENTS.md](../../AGENTS.md)) and needs cross-version restore/migration test coverage.

### Snapshot / restore changes (incremental vs full)

Snapshot strategies live in `snapshot/` (extend `RocksDBSnapshotStrategyBase`); restore operations live in `restore/` (implement `RocksDBRestoreOperation`: `RocksDBIncrementalRestoreOperation`, `RocksDBFullRestoreOperation`, `RocksDBNoneRestoreOperation`). Incremental snapshots upload changed SST files and reference previously uploaded ones; the native-full strategy writes a self-contained snapshot. **Incremental and full paths must stay interoperable across versions** — the checkpoint/savepoint format and the set of SST files referenced are compatibility surfaces. **Ask first** for changes to snapshot layout, the handles written, or restore semantics, and add restore tests for both paths.

### Adding a RocksDB `ConfigOption`

Add a `public static final ConfigOption<T>` in `RocksDBOptions` (backend-level: local dirs, timer service type, ...) or `RocksDBConfigurableOptions` (RocksDB tuning: write-buffer size, block cache, ...), built with `ConfigOptions.key(...)`. Wire it through `EmbeddedRocksDBStateBackend.configure(...)` / the resource container so it actually affects the opened DB, and annotate with `@Documentation.*` so it appears in generated config docs (the completeness check is `ConfigOptionsDocsCompletenessITCase` in `flink-docs`). If the option mirrors one in the old `contrib.streaming.state` package, keep both in sync.

### Native-resource lifecycle (`ColumnFamilyHandle` / `RocksObject`)

Every `org.rocksdb.*` handle (`RocksDB`, `ColumnFamilyHandle`, `WriteBatch`, `RocksIterator`, `Options`, caches) is a native object that must be explicitly closed; a leaked handle leaks off-heap memory. Acquire options/resources through `RocksDBResourceContainer` and close it; use `RocksDBWriteBatchWrapper` / `RocksIteratorWrapper` rather than raw handles; register long-lived handles with the backend's `Closeable`/cancel registry so they are released on disposal and on cancellation.

## Testing Patterns

JUnit 5 (`org.junit.jupiter`) + AssertJ is the repo standard for new tests; this module still contains legacy JUnit 4 / Hamcrest tests mid-migration — do not add to them. Most tests load the native RocksDB library, so they run a real embedded instance against a temp dir.

- **Backend conformance:** `EmbeddedRocksDBStateBackendTest` extends `StateBackendTestBase<EmbeddedRocksDBStateBackend>` (from the `flink-runtime` **test-jar**, `org.apache.flink.runtime.state`), exercising the full keyed-state contract against RocksDB.
- **State migration / compatibility:** `EmbeddedRocksDBStateBackendMigrationTest` extends `StateBackendMigrationTestBase` (also from the `flink-runtime` test-jar) — the place to cover serializer/schema evolution for RocksDB-stored state.
- **Config & options:** `RocksDBStateBackendConfigTest` (exported in the module's own test-jar, with `RocksDBTestUtils` and `EmbeddedRocksDBStateBackendTest`, for reuse by PyFlink and other modules), `RocksDBStateOptionTest`, `RocksDBNativeMetricOptionsTest`.
- **Snapshot / restore / rescaling:** `RocksDBAsyncSnapshotTest`, `RocksDBRecoveryTest`, `RocksIncrementalCheckpointRescalingTest`, `RocksDBAutoCompactionIngestRestoreTest`.
- **Resource & native plumbing:** `RocksDBResourceContainerTest`, `RocksDBWriteBatchWrapperTest`, `RocksDBNativeMetricMonitorTest`, the iterator tests (`RocksDBRocksStateKeysIteratorTest`, ...).
- **Test utility:** `RocksDBTestUtils` builds preconfigured backends/builders for tests.
