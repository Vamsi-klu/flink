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

# flink-clients

Client-side job submission: the `bin/flink` command-line entry point (`CliFrontend`) and the programmatic path that turns a user `Pipeline` into a `JobGraph` and hands it to a cluster. These classes usually run in the client JVM. In application mode the same entry-point path (`PackagedProgram`, `ClientUtils.executeProgram()`, `PackagedProgramApplication`) runs inside the Dispatcher/JobManager process (`Dispatcher.maybeSubmitApplicationInApplicationMode()`). User operators still execute only on TaskManagers. The deployment-target SPIs it implements (`PipelineExecutor`, `PipelineExecutorFactory`) are defined in `flink-core`, and per-target cluster descriptors (YARN, Kubernetes) live in their own modules.

## Build Commands

```
./mvnw clean install -DskipTests -pl flink-clients -am
```

After the first `-am` build, drop `-am` for faster rebuilds when only changing code within this module. Run a single test with `./mvnw -pl flink-clients -Dtest=CliFrontendRunTest test`.

Run `./mvnw spotless:apply` after edits (google-java-format, AOSP style). This module is checkstyle-enforced — it inherits the root `maven-checkstyle-plugin` binding (`tools/maven/checkstyle.xml`) with no module-level overrides (some modules such as `flink-core`/`flink-runtime` override only their `suppressionsLocation`, not the binding itself; see root [AGENTS.md](../AGENTS.md)). Do not suppress checkstyle; fix the code.

## Key Directory Structure

Under `flink-clients/src/main/java/org/apache/flink/client/`:

- `cli/` — `CliFrontend` (the `bin/flink` entry point), command-line parsing (`CliFrontendParser`), the `CustomCommandLine` SPI and its implementations (`GenericCLI`, `DefaultCLI`, `AbstractCustomCommandLine`), and the per-action option holders (`ProgramOptions`, `CancelOptions`, `StopOptions`, `SavepointOptions`, `ListOptions`, `CheckpointOptions`, `ClientOptions`).
- `program/` — the program abstraction and `ClusterClient`: `PackagedProgram`, `PackagedProgramUtils`, `StreamContextEnvironment`/`StreamPlanEnvironment`, `MiniClusterClient`, `PerJobMiniClusterFactory`; `program/rest/` holds `RestClusterClient` (the production `ClusterClient`); `program/artifact/` fetches job jars (`ArtifactFetchManager`, `ArtifactFetcher` + Fs/Http/Local impls).
- `deployment/` — cluster-facing SPIs: `ClusterDescriptor`, `ClusterClientFactory`, `ClusterClientServiceLoader` (+ `DefaultClusterClientServiceLoader`), and the standalone implementations (`StandaloneClientFactory`, `StandaloneClusterDescriptor`, `StandaloneClusterId`); `ClusterClientJobClientAdapter` adapts a `ClusterClient` to a `JobClient`.
- `deployment/executors/` — the `PipelineExecutor`/`PipelineExecutorFactory` implementations for the built-in targets: `LocalExecutor`(`Factory`), `RemoteExecutor`(`Factory`), and the shared `AbstractSessionClusterExecutor`; `PipelineExecutorUtils` builds the `JobGraph`.
- `deployment/application/` — application-mode support: `ApplicationRunner`/`DetachedApplicationRunner`, `EmbeddedJobClient`/`WebSubmissionJobClient`, `JarManifestParser`, and the `EntryClassInformationProvider` family; `deployment/application/executors/` holds the `EmbeddedExecutor`/`WebSubmissionExecutor` factories used inside the dispatcher.

## Key Abstractions

- **`CliFrontend`** (`cli/`) — the `bin/flink` entry point. `main(String[])` loads the active `CustomCommandLine`s and dispatches the sub-command (`run`, `list`, `cancel`, `stop`, `savepoint`, ...) to a protected method on the instance.
- **`CustomCommandLine`** (`cli/`) — SPI for a deployment target's CLI: `isActive(CommandLine)`, `addRunOptions`/`addGeneralOptions`, and `toConfiguration(CommandLine)` which folds parsed options into a `Configuration`. `GenericCLI` (the `-t`/`--target` executor selector) and `DefaultCLI` (standalone, the fallback) are the in-module implementations; `AbstractCustomCommandLine` is the shared base.
- **`PackagedProgram`** (`program/`) — a user jar + entry point; built via `PackagedProgram.newBuilder()`. `invokeInteractiveModeForExecution()` calls the user `main()`, which builds the pipeline against the context environment.
- **`StreamContextEnvironment` / `StreamPlanEnvironment`** (`program/`) — `StreamExecutionEnvironment` subclasses installed via `setAsContext(...)` so a user `main()` submits through the client's executor (context) or yields only its `StreamGraph` for plan extraction (plan).
- **`PipelineExecutor` / `PipelineExecutorFactory`** (defined in `flink-core`, `org.apache.flink.core.execution`) — the deployment-target SPI. `flink-clients` provides `LocalExecutor`, `RemoteExecutor` (extends `AbstractSessionClusterExecutor`), and the application-mode `EmbeddedExecutor`/`WebSubmissionExecutor`; factories are registered via `META-INF/services`.
- **`ClusterClient<T>`** (`program/`) — the handle to a running cluster (submit/cancel/stop/trigger savepoint, query jobs). `RestClusterClient` (`program/rest/`) talks to the REST endpoint; `MiniClusterClient` targets an in-JVM `MiniCluster`.
- **`ClusterDescriptor<T>` / `ClusterClientFactory<ClusterID>`** (`deployment/`) — `ClusterDescriptor` deploys/retrieves a cluster (`deploySessionCluster`, `deployApplicationCluster`, `retrieve`) and yields a `ClusterClientProvider`; the factory creates the descriptor for a given `Configuration` and is resolved through `ClusterClientServiceLoader`.
- **`ApplicationRunner` / `ApplicationDeployer`** (`deployment/application/`, `cli/`) — application-mode hooks: `ApplicationRunner` runs a `PackagedProgram` inside the cluster (`DetachedApplicationRunner`); `ApplicationDeployer` (impl `ApplicationClusterDeployer`) deploys an application cluster from the client.

## Common Change Patterns

### The submission flow (read this first)

`CliFrontend.main` → `loadCustomCommandLines` → `CliFrontend.run` parses args, picks the active `CustomCommandLine` via `validateAndGetActiveCommandLine`, and folds its `toConfiguration` result plus `ProgramOptions`/`ExecutionConfigAccessor` into an effective `Configuration`. It then calls `executeProgram` → `ClientUtils.executeProgram`, which installs a `StreamContextEnvironment` via `setAsContext` and runs `PackagedProgram.invokeInteractiveModeForExecution()`. The user `main()`'s `env.execute()` resolves a `PipelineExecutor` from the config's `execution.target` and submits. Trace through these classes before changing submission behavior.

### Adding a CLI command or option

- A new global/run option: add it in `CliFrontendParser` (the `Options` definitions) and a per-action holder under `cli/` (e.g. `ProgramOptions`, `CancelOptions`). A new sub-command: add a `protected` handler on `CliFrontend` and a dispatch case in `main`/`parseAndRun`. Wire `CliArgsException` for bad input.
- A new deployment target's CLI surface: implement `CustomCommandLine` (or extend `AbstractCustomCommandLine`), register it in `CliFrontend.loadCustomCommandLines`. `DefaultCLI` must remain last so it is the catch-all active CLI. Prefer the config-driven `GenericCLI` (`-t <target>`) over a bespoke CLI when the target only needs `Configuration` keys.

### Adding a deployment target (executor)

1. Implement `PipelineExecutor` (or extend `AbstractSessionClusterExecutor` for a session cluster), plus a `PipelineExecutorFactory` whose `getName()` matches the `execution.target` value.
2. Register the factory in `META-INF/services/org.apache.flink.core.execution.PipelineExecutorFactory` (the in-module file lists `RemoteExecutorFactory` and `LocalExecutorFactory`).
3. If the target deploys its own cluster, also provide a `ClusterClientFactory` + `ClusterDescriptor` and register the factory in `META-INF/services/org.apache.flink.client.deployment.ClusterClientFactory` (resolved by `DefaultClusterClientServiceLoader`). Per-target descriptors (YARN, Kubernetes) live in their own modules; the standalone one here is the template.

### Building a JobGraph from a pipeline

`PackagedProgramUtils.createJobGraph(...)` / `getPipelineFromProgram(...)` extract the `Pipeline` and lower it via `FlinkPipelineTranslationUtil` → `FlinkPipelineTranslator` (`StreamGraphTranslator` for the DataStream `StreamGraph`). Use these rather than re-deriving a `JobGraph` by hand.

### Application-mode submission

The client side (`ApplicationClusterDeployer`) deploys an application cluster. For application-mode entry points, the program runs in the dispatcher via `PackagedProgramApplication` driving the `EmbeddedExecutor` (wired by `ApplicationDispatcherGatewayServiceFactory` plus the runtime's `ApplicationBootstrap`). The web/REST jar-run path instead drives the `WebSubmissionExecutor` through an `ApplicationRunner` (`DetachedApplicationRunner`); the handler that invokes it (`JarRunHandler`/`WebSubmissionExtension`) lives in `flink-runtime-web`, not here — confirm with a `find` before assuming a class is in this module.

## Testing Patterns

JUnit 5 (`org.junit.jupiter`) + AssertJ.

- **CLI tests:** extend `CliFrontendTestBase`; `CliFrontendTestUtils` provides config/jar fixtures. Action-specific tests are split by command (`CliFrontendRunTest`, `CliFrontendCancelTest`, `CliFrontendStopWithSavepointTest`, `CliFrontendListTest`, `CliFrontendSavepointTest`, `CliFrontendCheckpointTest`, `CliFrontendDynamicPropertiesTest`, ...). They construct a `CliFrontend` with stub `CustomCommandLine`s/`ClusterClient`s and assert on parsed configuration and dispatched calls — no real cluster.
- **Program tests:** `PackagedProgramTest`, `StreamContextEnvironmentTest`, `ClientUtilsTest`, `ClientHeartbeatTest` cover the context-environment + submission plumbing in isolation.
- **End-to-end (`*ITCase`):** `CliFrontendITCase`, `PackagedProgramApplicationITCase`, `DefaultPackagedProgramRetrieverITCase`, and `FromClasspathEntryClassInformationProviderITCase` exercise the full path; cluster-backed ITCases spin up a `MiniCluster` (often via `MiniClusterClient`/`PerJobMiniClusterFactory`).
- **Naming:** integration tests end in `*ITCase`; plain unit tests end in `*Test`.
