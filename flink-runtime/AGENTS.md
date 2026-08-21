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

# flink-runtime

The distributed runtime of Flink. The **JobManager** side orchestrates execution — the `Dispatcher` accepts jobs, each `JobMaster` builds and drives an `ExecutionGraph` via a `SchedulerNG`, the `ResourceManager` manages slots, and the `CheckpointCoordinator` coordinates checkpoints and failover. The **TaskManager** side (`TaskExecutor`) executes operators inside task slots and manages network buffers, state backends, and I/O. User operators run only on the TaskManager (`Task` / `AbstractInvokable`). JobManager components do not run operators. In application mode the user's `main()` can run in the Dispatcher/JobManager via `Dispatcher.maybeSubmitApplicationInApplicationMode()` and `PackagedProgramApplication` (see [flink-clients/AGENTS.md](../flink-clients/AGENTS.md)). Components communicate over the RPC framework (package `org.apache.flink.runtime.rpc`, implemented in `flink-rpc`), expose a REST API, and persist state through the state-backend framework.

This module is scoped below to the distributed runtime (`org.apache.flink.runtime.*`). Note that since FLINK-36063 it **also physically hosts** the DataStream API and the stream operator/task/windowing runtime under the `org.apache.flink.streaming.*` package (500+ files: `StreamExecutionEnvironment`, `DataStream`, `AbstractStreamOperator`, `StreamTask`, `StreamGraph`, `WindowOperator`, ...). For changes to those classes, see [flink-streaming-java/AGENTS.md](../flink-streaming-java/AGENTS.md), which documents that surface (the classes live here, but the patterns are described there).

## Build Commands

```
./mvnw clean install -DskipTests -pl flink-runtime -am
```

After the first full build, drop `-am` for faster rebuilds when only changing code within this module. Run a single test with `./mvnw -pl flink-runtime -Dtest=JobMasterTest test`.

Run `./mvnw spotless:apply` after edits (google-java-format, AOSP style).

**Checkstyle is enforced** on `flink-runtime` via the inherited root `maven-checkstyle-plugin` binding (`validate` phase, `failOnViolation=true`); the module overrides only its suppressions file (`tools/maven/suppressions-runtime.xml`), which exempts more files/checks than the default but does not disable enforcement. Do not suppress checkstyle; fix the code to satisfy it.

## Key Directory Structure

Packages under `flink-runtime/src/main/java/org/apache/flink/runtime/`.

### JobManager-side (orchestration; never runs user code)

- `dispatcher/` — `Dispatcher`: accepts job submissions, spawns a `JobMaster` per job, hosts `DispatcherRestEndpoint`
- `jobmaster/` — `JobMaster`: drives one job's `ExecutionGraph`; `slotpool/` (`SlotPool`, `DeclarativeSlotPool`) manages slots from the RM
- `resourcemanager/` — `ResourceManager`: registers `TaskExecutor`s, matches slot requests to resources
- `scheduler/` — `SchedulerNG`, `DefaultScheduler` (default), `adaptive/AdaptiveScheduler` (reactive/rescaling); slot allocation and deployment
- `executiongraph/` — `ExecutionGraph` / `DefaultExecutionGraph`, `ExecutionVertex`, `Execution`; `failover/` restart-backoff and failover strategies
- `jobgraph/` — `JobGraph`, `JobVertex` (the serializable job topology submitted to the cluster); `jobgraph/tasks/AbstractInvokable`
- `checkpoint/` — `CheckpointCoordinator` and checkpoint metadata/storage plumbing
- `entrypoint/` — `ClusterEntrypoint` and cluster bootstrap (session/application)

### TaskManager-side (executes operators)

- `taskexecutor/` — `TaskExecutor` (the TaskManager RPC endpoint) and slot/job management
- `taskmanager/` — `Task`: the unit of execution that runs an `AbstractInvokable`
- `io/network/` — `ResultPartition`/`ResultSubpartition` (output), `partition/consumer/InputGate` (input), Netty stack, buffers
- `memory/` — `MemoryManager` for managed (off-heap) memory
- `operators/` — batch driver operators (`BatchTask`, join/sort/reduce drivers)

### Shared / cluster services

- `state/` — state-backend framework: `StateBackend`, `CheckpointStorage`, `KeyedStateBackend`/`AbstractKeyedStateBackend`, `OperatorStateBackend`, `StateSnapshot`; backends under `heap/`, `filesystem/`, `memory/`, `ttl/`, `changelog/`, `v2/`
- `shuffle/` — `ShuffleEnvironment` SPI and `NettyShuffleMaster`/`NettyShuffleEnvironment` (the default shuffle service)
- `rest/` — `RestServerEndpoint`, `RestClient`, `handler/` (handlers), `messages/` (`MessageHeaders`, `RequestBody`, `ResponseBody`); `webmonitor/WebMonitorEndpoint`
- `highavailability/` — `HighAvailabilityServices` (leader services, persistent stores)
- `leaderelection/`, `leaderretrieval/` — `LeaderElection`, `LeaderRetrievalService`
- `blob/` — `BlobServer`/`BlobService`: distributes jars and large artifacts
- `source/coordinator/` — `SourceCoordinator` (JM-side coordinator for FLIP-27 sources)
- `operators/coordination/` — `OperatorCoordinator`: JM-side operator coordination, RPC-bridged to runtime tasks
- `metrics/`, `heartbeat/`, `minicluster/` (`MiniCluster`), `clusterframework/`

Note: the RPC base types (`RpcEndpoint`, `RpcGateway`, `RpcService`) live in `flink-rpc/flink-rpc-core` under package `org.apache.flink.runtime.rpc`, not in this module. The main user-facing runtime config options (`JobManagerOptions`, `TaskManagerOptions`, `CheckpointingOptions`) live in `flink-core`'s `org.apache.flink.configuration`. The DataStream API / stream operators / `StreamTask` / windowing core live in this module under the separate `org.apache.flink.streaming.*` tree (see the note above and [flink-streaming-java/AGENTS.md](../flink-streaming-java/AGENTS.md)).

## Key Abstractions

- **RPC model** (`org.apache.flink.runtime.rpc`, in `flink-rpc-core`): `RpcService` hosts `RpcEndpoint`s; each endpoint is addressed through a typed `RpcGateway` whose remote methods return `CompletableFuture<...>`. `JobMaster`, `Dispatcher`, and `ResourceManager` are `FencedRpcEndpoint`s; `TaskExecutor` is a plain `RpcEndpoint`. **Each endpoint runs single-threaded on its own main thread** — use `getMainThreadExecutor()` / `runAsync(...)` / `callAsync(...)` to mutate endpoint state from callbacks, and never block the main thread.
- **`SchedulerNG` / `DefaultScheduler`** — the scheduling SPI; `DefaultScheduler` (pipelined-region scheduling) is standard, `AdaptiveScheduler` supports reactive rescaling.
- **`ExecutionGraph` / `ExecutionVertex` / `Execution`** — runtime execution topology: the graph, one vertex per parallel subtask, and a single deployment attempt. Built from the `JobGraph` / `JobVertex`.
- **`CheckpointCoordinator`** — triggers checkpoints, tracks acks from tasks, and finalizes/cleans up completed checkpoints.
- **State stack** — `StateBackend` (factory for keyed/operator state) and `CheckpointStorage` (where snapshots are written) are configured independently; `KeyedStateBackend`/`AbstractKeyedStateBackend` and `OperatorStateBackend` hold per-task state; `StateSnapshot` is the snapshot abstraction.
- **Task execution** — `Task` (`taskmanager/`) is the runnable unit a `TaskExecutor` schedules into a slot; it runs an `AbstractInvokable` (`jobgraph/tasks/`), the base class for the actual operator-driving code (e.g. the `StreamTask` family).
- **Network** — `ResultPartition`/`ResultSubpartition` produce data, `InputGate` consumes it, both provided by a `ShuffleEnvironment` (default `NettyShuffleEnvironment`).
- **`HighAvailabilityServices`** — supplies leader election/retrieval and persistent stores for HA setups.
- **REST** — `RestServerEndpoint` hosts handlers extending `AbstractRestHandler<...>`, each bound to a `MessageHeaders`; request/response payloads implement `RequestBody` / `ResponseBody`.

## Common Change Patterns

### Adding an RPC method

1. Add the method to the relevant `*Gateway` interface (e.g. `JobMasterGateway`, `TaskExecutorGateway`, `ResourceManagerGateway`), returning `CompletableFuture<...>`.
2. Implement it in the corresponding `RpcEndpoint` (`JobMaster`, `TaskExecutor`, `ResourceManager`).
3. The method body runs on the endpoint's main thread — do not block. Touch endpoint state directly there; for async work, hop back with `getMainThreadExecutor()` / `runAsync`.

### Adding a REST handler / endpoint

1. Implement `AbstractRestHandler<G, RequestBody, ResponseBody, MessageParameters>` and a `MessageHeaders` describing the URL, method, and body types (under `rest/messages/`). Request/response bodies implement `RequestBody` / `ResponseBody`.
2. Register the handler in the appropriate `*RestEndpoint` / `WebMonitorEndpoint` (e.g. `DispatcherRestEndpoint`).
3. Regenerate the REST reference with `./mvnw package -Dgenerate-rest-docs -pl flink-docs -am -nsu -DskipTests`. That profile runs `RuntimeRestAPIDocGenerator` (HTML shortcodes under `docs/layouts/shortcodes/generated/`, e.g. `rest_v1_dispatcher.html`) and `RuntimeOpenApiSpecGenerator` (OpenAPI YAML under `docs/static/generated/`, e.g. `rest_v1_dispatcher.yml`). Do not invent `docs/.../rest_api_*.html` files or edit the generated output by hand. See [flink-docs/README.md](../flink-docs/README.md).

### Scheduling / failover changes

Touch `SchedulerNG` / `DefaultScheduler` (or `AdaptiveScheduler`), `ExecutionGraph`, and `executiongraph/failover/`. These are sensitive areas; per root [AGENTS.md](../AGENTS.md), **ask first** for anything that changes checkpoint/savepoint behavior or failover semantics.

### State backend / checkpoint changes

Work through `StateBackend`, `KeyedStateBackend`/`OperatorStateBackend`, `CheckpointStorage`, and `CheckpointCoordinator`. Changes to serializer snapshots, snapshot layout, or checkpoint format affect **state compatibility** and are an **ask-first** area (root [AGENTS.md](../AGENTS.md)). Add cross-version restore/migration tests for any such change.

### Configuration options

Most runtime options are defined in `flink-core` (`org.apache.flink.configuration.JobManagerOptions` / `TaskManagerOptions` / `CheckpointingOptions`), not here. A few subsystem-specific holders live in this module (e.g. `shuffle/ShuffleServiceOptions`, `highavailability/JobResultStoreOptions`). Confirm where the relevant options class lives before adding an entry, and add the `@Documentation` annotation so the generated config docs pick it up. (Note: `checkpoint/CheckpointOptions` is a per-checkpoint runtime value object, not a `ConfigOption` holder.)

## Testing Patterns

JUnit 5 (`org.junit.jupiter`) + AssertJ.

- **Testing RPC:** `TestingRpcService` (`src/test/.../rpc/`) hosts endpoints without real networking. Collaborating gateways are faked with builder-style test doubles — `TestingJobMasterGateway` via `TestingJobMasterGatewayBuilder`, `TestingResourceManagerGateway`, etc. — letting a test stub individual RPC responses.
- **End-to-end / cluster tests:** `MiniCluster` for an in-JVM cluster; runtime tests use `InternalMiniClusterExtension` (and the shared `MiniClusterExtension` from `flink-test-utils`). Use these for `*ITCase`-style tests (e.g. `JobExecutionITCase`, `JobRecoveryITCase`).
- **Naming:** integration tests end in `*ITCase`; plain unit tests end in `*Test` (e.g. `JobMasterTest`).
