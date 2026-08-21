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

# flink-streaming-java

Home of the original DataStream API and the stream-processing operator/runtime implementation.

**Read this first.** As of FLINK-36063 (commit `51f1684be02`) the bulk of that surface was **migrated into `flink-runtime`**, keeping the same `org.apache.flink.streaming.*` package names: `StreamExecutionEnvironment`, `DataStream`/`KeyedStream`, `StreamGraph`/`StreamGraphGenerator`/`StreamingJobGraphGenerator`, the operator base classes (`AbstractStreamOperator`, `OneInputStreamOperator`, ...), `StreamTask`, the FLIP-27 `SourceOperator`, and the windowing base classes (`WindowOperator`, `WindowAssigner`, `Trigger`, `Evictor`, `Window`). This module now `compile`-depends on `flink-runtime` and holds only the **residual** operators, user functions, and concrete windowing strategies that build on that core, plus the task-level test harnesses and many runtime `ITCase`s. When you change the DataStream API surface, the `StreamGraph` translation, the operator/task base classes, or window base interfaces, you are editing **`flink-runtime`** — confirm with a `find` before assuming a class lives here, and see [flink-runtime/AGENTS.md](../flink-runtime/AGENTS.md). The Table runtime (`flink-table-runtime`) and DataStream v2 (`flink-datastream-api`/`flink-datastream`) are separate modules built on the same abstractions.

## Build Commands

```
./mvnw clean install -DskipTests -pl flink-streaming-java -am
```

After the first `-am` build, drop `-am` for faster rebuilds when only changing code within this module. Because the production core lives in `flink-runtime`, edits there require rebuilding `flink-runtime` (and re-publishing its `test-jar`, which this module consumes).

Run `./mvnw spotless:apply` after every edit (google-java-format, AOSP style). This module **is** checkstyle-enforced — it inherits the root `maven-checkstyle-plugin` binding (`tools/maven/checkstyle.xml`) with no module-level overrides. (Every module is checkstyle-enforced; `flink-core`/`flink-runtime` differ only by pointing at a broader module-specific suppressions file.) Do not suppress checkstyle; fix the code.

## Key Directory Structure

Under `flink-streaming-java/src/main/java/org/apache/flink/streaming/`:

- `api/datastream/` — only the residual entry points `AsyncDataStream` (`@PublicEvolving`), `DataStreamUtils` (`@Experimental`), `MultipleConnectedStreams`. The main `DataStream`/`KeyedStream`/`SingleOutputStreamOperator` classes live in `flink-runtime`.
- `api/operators/async/` — the FLIP-12 async I/O operator: `AsyncWaitOperator`, `AsyncWaitOperatorFactory`, and the `queue/` element-queue implementations (`OrderedStreamElementQueue`, `UnorderedStreamElementQueue`, `StreamElementQueue`).
- `api/operators/` — the residual operator-support classes `AbstractInput`, `OnWatermarkCallback` (plus `async/`).
- `api/functions/async/` — async user-function layer: `AsyncFunction`, `RichAsyncFunction`, `ResultFuture`, `AsyncRetryStrategy`/`AsyncRetryPredicate`.
- `api/functions/co/` — `RichCoMapFunction`, `RichCoFlatMapFunction`.
- `api/functions/sink/` — `PrintSink`; `sink/v2/DiscardingSink` (current `Sink` API); `sink/legacy/` (`DiscardingSink`, `TwoPhaseCommitSinkFunction` — extend the deprecated `RichSinkFunction`); `sink/filesystem/` + `sink/filesystem/legacy/StreamingFileSink`.
- `api/functions/source/datagen/` — `DataGeneratorSource` and generators (legacy `RichParallelSourceFunction`-based).
- `api/functions/timestamps/` — `AscendingTimestampExtractor`, `BoundedOutOfOrdernessTimestampExtractor`.
- `api/functions/windowing/` — `RichWindowFunction`, `RichAllWindowFunction`, and `delta/` delta functions/extractors.
- `api/windowing/assigners/`, `api/windowing/triggers/`, `api/windowing/evictors/` — concrete windowing strategies only (e.g. `EventTimeSessionWindows`, `DynamicEventTimeSessionWindows`, `ContinuousEventTimeTrigger`, `ProcessingTimeoutTrigger`, `DeltaTrigger`, `TimeEvictor`, `DeltaEvictor`). Their base interfaces (`WindowAssigner`, `Trigger`, `Evictor`, `Window`) live in `flink-runtime`.
- `api/lineage/` — dataset lineage facets (`DatasetSchemaFacet`, `TypeDatasetFacet`, `LineageUtils`, `DefaultLineageVertex`).
- `api/legacy/io/` — `CollectionInputFormat`, `TextInputFormat`, `TextOutputFormat`.
- `runtime/operators/` — `GenericWriteAheadSink`, `CheckpointCommitter`, `windowing/KeyMap`.
- `util/retryable/` — `AsyncRetryStrategies`, `RetryPredicates` (async I/O retry helpers).
- `util/serialize/` — Kryo/Chill serializer registration (`FlinkChillPackageRegistrar`, `InetSocketAddressSerializer`, `PriorityQueueSerializer`).
- `experimental/` — `CollectSink`, `SocketStreamIterator`.

## Key Abstractions

Most are defined in `flink-runtime` (package `org.apache.flink.streaming.*`); they are listed here because they are the abstractions you extend when adding code to this module.

- **`StreamExecutionEnvironment`** (in `flink-runtime`): the DataStream API entry point; builds a `StreamGraph` from registered transformations.
- **`DataStream<T>` / `KeyedStream<K,T>`** (in `flink-runtime`): the user-facing dataflow types; each transform appends a `Transformation` to the environment. `AsyncDataStream` here adds async-I/O transforms to an existing `DataStream`.
- **`Transformation<T>`** (in `flink-core`, `org.apache.flink.api.dag`): the logical-graph node a DataStream operation produces; subclasses live in `flink-runtime` under `api/transformations/`.
- **`StreamGraph` → `JobGraph`** (in `flink-runtime`, `api/graph/`): `StreamGraphGenerator` turns transformations into a `StreamGraph`; `StreamingJobGraphGenerator` lowers it to a `JobGraph` for submission.
- **`AbstractStreamOperator<OUT>` / `AbstractStreamOperatorV2<OUT>`** (in `flink-runtime`): operator base classes. `AbstractUdfStreamOperator` adds a user function. Operators implement `OneInputStreamOperator` / `TwoInputStreamOperator`, or `MultipleInputStreamOperator` (V2) for 3+ inputs.
- **`StreamOperatorFactory<OUT>`** (in `flink-runtime`): how a `Transformation` instantiates an operator. In this module, `AsyncWaitOperatorFactory` extends `AbstractStreamOperatorFactory<OUT>` and implements `OneInputStreamOperatorFactory` + `YieldingOperatorFactory`.
- **`AsyncWaitOperator<IN,OUT>`** (this module): the canonical residual operator — `extends AbstractUdfStreamOperator<OUT, AsyncFunction<IN,OUT>> implements OneInputStreamOperator<IN,OUT>, BoundedOneInput`, driving `AsyncFunction`/`RichAsyncFunction` via ordered/unordered stream-element queues.
- **`StreamTask`** and subclasses (`OneInputStreamTask`, `SourceOperatorStreamTask`, ...) (in `flink-runtime`): the mailbox-driven runtime task that runs the operator chain.
- **`SourceFunction` / `SinkFunction` (and `RichSinkFunction`)** (in `flink-runtime`, `*/legacy/`): the deprecated legacy connector interfaces. `TwoPhaseCommitSinkFunction`, `StreamingFileSink`, and `DataGeneratorSource` in this module still build on them, but **do not use them for new connectors** — use the FLIP-27 `Source` API and the `Sink` API (package `sink2`).
- **`WindowOperator` / `WindowAssigner` / `Trigger` / `Evictor`** (in `flink-runtime`): windowing runtime + base interfaces. This module supplies concrete assigners/triggers/evictors only.

## Common Change Patterns

### Adding or modifying a stream operator

1. Extend `AbstractStreamOperator<OUT>` (or `AbstractUdfStreamOperator<OUT, F>` if it wraps a user function) and implement `OneInputStreamOperator<IN,OUT>` or `TwoInputStreamOperator<IN1,IN2,OUT>`. For 3+ inputs, extend `AbstractStreamOperatorV2` and implement `MultipleInputStreamOperator`. These base types live in `flink-runtime`.
2. Provide a `StreamOperatorFactory` (extend `AbstractStreamOperatorFactory<OUT>` and the matching `*StreamOperatorFactory` interface) so transformations can instantiate it; see `AsyncWaitOperatorFactory`.
3. State/timer access from inside the operator goes through the runtime: keyed state via `getKeyedStateBackend()` / `getRuntimeContext().getState(...)`, and event/processing-time timers via `InternalTimerService` (obtained with `getInternalTimerService(...)`). `AsyncWaitOperator` is a worked example of mailbox-safe processing-time handling.
4. The transform that wires the operator into a `DataStream` (creating the `Transformation`) lives in `flink-runtime`; update it there if you are adding a new transform.

### Adding a DataStream API method/transformation

DataStream/KeyedStream methods create a `Transformation` subclass that `StreamGraphGenerator` translates — **all of this is in `flink-runtime`**. In this module the analogous surface is `AsyncDataStream` (adds async-I/O transforms). Every user-facing API class/method needs a stability annotation (`@Public`/`@PublicEvolving`/`@Experimental`), and new or changed user-facing API requires a voted FLIP — **ask first** (see root [AGENTS.md](../AGENTS.md)).

### Windowing changes

Concrete `WindowAssigner`/`Trigger`/`Evictor` subclasses live here (`api/windowing/{assigners,triggers,evictors}`). The `WindowOperator` and the base interfaces live in `flink-runtime`; behavioral changes to the windowing runtime belong there. Keep concrete strategies free of state-format changes unless you also bump the relevant serializer snapshots (state-compat — ask first).

### Source/Sink integration

Use the FLIP-27 source path (`SourceOperator`/`SourceOperatorFactory`, in `flink-runtime`) and the `Sink` API (package `sink2`; current `DiscardingSink` is at `api/functions/sink/v2/`). The legacy `SourceFunction`/`SinkFunction`/`RichSinkFunction` types and their users here (`TwoPhaseCommitSinkFunction`, `StreamingFileSink`, `DataGeneratorSource`) are **deprecated and must not be used for new connectors**. Most connectors live in separate repos.

## Testing Patterns

JUnit 5 + AssertJ (no JUnit 4 / Hamcrest in new tests). This module's `src/test` is substantial — it hosts the **task-level test harnesses** and many runtime `ITCase`s in addition to operator tests.

- **Operator harness tests:** `OneInputStreamOperatorTestHarness`, `KeyedOneInputStreamOperatorTestHarness`, `TwoInputStreamOperatorTestHarness` (in `org.apache.flink.streaming.util`, provided by the `flink-runtime` **test-jar**, a declared test dependency). `AsyncWaitOperatorTest` is a good reference.
- **Task-level harnesses (defined in this module's test sources):** `StreamTaskMailboxTestHarness` / `StreamTaskMailboxTestHarnessBuilder` for mailbox-driven task tests, and `StreamTaskTestHarness` / `OneInputStreamTaskTestHarness` / `TwoInputStreamTaskTestHarness` for full task-chain tests (`org.apache.flink.streaming.runtime.tasks`).
- **ITCase:** name end-to-end tests `*ITCase` (e.g. `StreamTaskITCase`, `StreamTaskTimerITCase`); they typically spin up a `MiniCluster`.
- Test location mirrors source structure within the module.
