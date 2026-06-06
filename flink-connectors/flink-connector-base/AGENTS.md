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

# flink-connector-base

Reusable base classes for building FLIP-27 sources and async sinks. The `Source`/`Sink` API *interfaces* themselves (`Source`, `SourceReader`, `SplitEnumerator`, `Sink`, `SinkWriter`, `SimpleVersionedSerializer`) live in `flink-core-api`/`flink-core`; this module provides the heavy-lifting base implementations (the threaded split-fetcher reader framework and the buffered async-sink writer) that concrete connectors — most of which live in separate repos — extend.

## Build Commands

```
./mvnw clean install -DskipTests -pl flink-connectors/flink-connector-base -am
```

After the first `-am` build, drop `-am` for faster rebuilds when only changing code within this module. Run `./mvnw spotless:apply` after edits (google-java-format, AOSP style); CI runs `spotless:check`.

This module **is** checkstyle-enforced — it inherits the root `maven-checkstyle-plugin` binding (`tools/maven/checkstyle.xml`) with no module-level opt-out in its `pom.xml`. Do not suppress checkstyle; fix the code. See the root [AGENTS.md](../../AGENTS.md) for repo-wide conventions.

## Key Directory Structure

Under `flink-connectors/flink-connector-base/src/main/java/org/apache/flink/connector/base/`:

- `source/reader/` — the source-reader framework: `SourceReaderBase`, `SingleThreadMultiplexSourceReaderBase`, `RecordEmitter`, `RecordsWithSplitIds`/`RecordsBySplits`, `SourceReaderOptions`, `RecordEvaluator`
- `source/reader/splitreader/` — the connector-supplied `SplitReader` SPI and split-change events (`SplitsChange`, `SplitsAddition`, `SplitsRemoval`)
- `source/reader/fetcher/` — the threaded fetching layer: `SplitFetcher`, `SplitFetcherManager`, `SingleThreadFetcherManager`, and `SplitFetcherTask` implementations (`FetchTask`, `AddSplitsTask`, `RemoveSplitsTask`, `PauseOrResumeSplitsTask`)
- `source/reader/synchronization/` — `FutureCompletingBlockingQueue`, the hand-off queue between fetcher threads and the reader's main thread
- `source/hybrid/` — `HybridSource` and its enumerator/reader/split plumbing for chaining bounded then unbounded sources
- `source/utils/` — `SerdeUtils`, split-collection (de)serialization helpers for enumerator-state serializers
- `sink/` — `AsyncSinkBase` and `AsyncSinkBaseBuilder`, the entry points for async sinks
- `sink/writer/` — the async writer core: `AsyncSinkWriter`, `ElementConverter`, `AsyncSinkWriterStateSerializer`, `BufferedRequestState`, `ResultHandler`, request-buffer types
- `sink/writer/strategy/` — rate-limiting / congestion-control strategies (`RateLimitingStrategy`, `CongestionControlRateLimitingStrategy`, `AIMDScalingStrategy`)
- `sink/writer/config/` — `AsyncSinkWriterConfiguration` (buffering hints)
- `sink/throwable/` — `FatalExceptionClassifier` for classifying retryable vs. fatal sink errors
- `table/` — Table API bridge for async sinks: `AsyncDynamicTableSinkFactory` and `AsyncSinkConnectorOptions` at the top level, `AsyncDynamicTableSink` under `table/sink`, and option-validation helpers under `table/options`, `table/sink/options`, `table/util`

## Key Abstractions

- **`SourceReaderBase<E, T, SplitT, SplitStateT>`** (`source/reader/`) — implements `SourceReader<T, SplitT>`. Drives a `SplitFetcherManager` of `SplitReader`s through a `FutureCompletingBlockingQueue<RecordsWithSplitIds<E>>`, then converts raw records `E` to emitted records `T` via a `RecordEmitter`. `E` is the raw fetched type, `T` the emitted type, `SplitStateT` the mutable per-split reader state.
- **`SingleThreadMultiplexSourceReaderBase<E, T, SplitT, SplitStateT>`** (`source/reader/`) — `SourceReaderBase` subclass that multiplexes all splits onto a single fetcher thread via `SingleThreadFetcherManager`; the common starting point for new readers.
- **`SplitReader<E, SplitT>`** (`source/reader/splitreader/`) — the connector-implemented SPI: `fetch()` returns a `RecordsWithSplitIds<E>`, `handleSplitsChanges(SplitsChange)`, `wakeUp()`, and (default) `pauseOrResumeSplits(...)`.
- **`RecordEmitter<E, T, SplitStateT>`** (`source/reader/`) — `emitRecord(E, SourceOutput<T>, SplitStateT)`: maps a raw record to output and updates split state (offsets/positions).
- **`SplitFetcherManager` / `SingleThreadFetcherManager`** (`source/reader/fetcher/`) — own the `SplitFetcher` threads; each `SplitFetcher` runs queued `SplitFetcherTask`s (`FetchTask`, `AddSplitsTask`, ...) against one `SplitReader`.
- **`FutureCompletingBlockingQueue<T>`** (`source/reader/synchronization/`) — bounded blocking queue with a `CompletableFuture`-based availability signal, decoupling fetcher threads from the reader main thread.
- **`HybridSource<T>`** (`source/hybrid/`) — implements `Source<T, HybridSourceSplit, HybridSourceEnumeratorState>`; runs a sequence of underlying sources, switching when each finishes.
- **`AsyncSinkBase<InputT, RequestEntryT>`** (`sink/`) — implements `Sink<InputT>` + `SupportsWriterState<...>`; holds the `ElementConverter` and buffering/rate-limit configuration and creates `AsyncSinkWriter`s. Built via `AsyncSinkBaseBuilder`.
- **`AsyncSinkWriter<InputT, RequestEntryT>`** (`sink/writer/`) — the stateful (`StatefulSinkWriter<InputT, BufferedRequestState<RequestEntryT>>`) buffering writer. Converts elements to `RequestEntryT` with an `ElementConverter`, batches them, and flushes via the abstract `submitRequestEntries(List, ResultHandler)`; `getSizeInBytes(RequestEntryT)` sizes each entry for byte-based batching.
- **`ElementConverter<InputT, RequestEntryT>`** (`sink/writer/`) — `apply(InputT, SinkWriter.Context)`: turns an input record into a destination request entry.
- **`AsyncSinkWriterStateSerializer<RequestEntryT>`** (`sink/writer/`) — implements `SimpleVersionedSerializer<BufferedRequestState<RequestEntryT>>` for the writer's in-flight buffer state; subclasses serialize a single `RequestEntryT`.
- **`RateLimitingStrategy`** (`sink/writer/strategy/`) — pluggable admission control for in-flight requests; default `CongestionControlRateLimitingStrategy` uses an `AIMDScalingStrategy`.

## Common Change Patterns

### Implementing a FLIP-27 source

1. **`SplitReader`** (`source/reader/splitreader/`) — implement `fetch()`/`handleSplitsChanges`/`wakeUp` to pull records from the external system for a set of splits.
2. **Source reader** — extend `SingleThreadMultiplexSourceReaderBase` (single fetcher thread) or `SourceReaderBase` directly, supplying a `RecordEmitter` that maps raw records to output and advances per-split state. Tune via `SourceReaderOptions` (e.g. `ELEMENT_QUEUE_CAPACITY`).
3. **`SplitEnumerator`** — implement the enumerator interface from `flink-core` to discover/assign splits (no base class in this module); wire reader + enumerator together in your `Source` implementation.
4. **Serializers** — provide `SimpleVersionedSerializer`s (`flink-core`) for the split and the enumerator checkpoint state; `source/utils/SerdeUtils` helps (de)serialize split collections for enumerator state.

### Implementing an async sink

1. Extend **`AsyncSinkBase<InputT, RequestEntryT>`** (build it through `AsyncSinkBaseBuilder`) and supply an `ElementConverter`.
2. Extend **`AsyncSinkWriter<InputT, RequestEntryT>`**: implement `submitRequestEntries(List<RequestEntryT>, ResultHandler<RequestEntryT>)` (perform the async write; re-queue retryable entries through the `ResultHandler`, signal fatal errors through it) and `getSizeInBytes(RequestEntryT)`.
3. Provide an **`AsyncSinkWriterStateSerializer<RequestEntryT>`** to persist `BufferedRequestState` across checkpoints.
4. Use `FatalExceptionClassifier` (`sink/throwable/`) to separate fatal from retryable failures, and a `RateLimitingStrategy` (`sink/writer/strategy/`) if the destination needs adaptive throughput.

### Exposing an async sink to the Table API

Extend `AsyncDynamicTableSinkFactory` (`DynamicTableSinkFactory`) and `AsyncDynamicTableSink` (`DynamicTableSink`), reusing the shared `AsyncSinkConnectorOptions` and the validators under `table/options` and `table/sink/options`.

### Legacy interfaces are forbidden for new connectors

Do **not** build new connectors on the deprecated `SourceFunction`/`SinkFunction`. Use the FLIP-27 `Source` path and the `Sink` API base classes documented here.

## Testing Patterns

JUnit 5 (`org.junit.jupiter`) + AssertJ.

- **Source readers:** extend `SourceReaderTestBase<SplitT>` from `flink-connector-test-utils` (`org.apache.flink.connector.testutils.source.reader`), with `TestingReaderContext` / `TestingReaderOutput` / `TestingSplitEnumeratorContext` as test doubles. `SourceReaderBaseTest` (which `extends SourceReaderTestBase<MockSourceSplit>`) is the in-module reference.
- **Async sink writers:** `AsyncSinkWriterTest` drives the writer directly; `AsyncSinkWriterTestUtils` provides `getTestState(...)` and `assertThatBufferStatesAreEqual(...)` for state-serializer round-trip checks (see `AsyncSinkWriterStateSerializerTest`).
- **Naming:** integration tests end in `*ITCase`; plain unit tests end in `*Test`. Test layout mirrors the source package structure under `src/test/java/`.
