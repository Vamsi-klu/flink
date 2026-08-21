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

# flink-table-api-java

The Java Table/SQL API entry points and configuration: `TableEnvironment`, `Table`, the `Operation` tree that user programs are parsed into, and the `table.exec.*` / `table.optimizer.*` config-option holders. This module defines the API surface and the delegation interfaces (`Planner`, `Executor`, `Parser`); the actual planning/optimization happens in `flink-table-planner` behind those interfaces. The user-facing value types it builds on (`Schema`, `ResolvedSchema`, `DataType`, `RowData`) live in `flink-table-common`.

## Build Commands

```
./mvnw clean install -DskipTests -pl flink-table/flink-table-api-java -am
```

After the first `-am` build, drop `-am` for faster rebuilds when only changing code within this module.

Run `./mvnw spotless:apply` after edits (google-java-format, AOSP style). This module **is** checkstyle-enforced — it inherits the root `maven-checkstyle-plugin` binding (`tools/maven/checkstyle.xml`) with no module-level opt-out in its `pom.xml`. Do not suppress checkstyle; fix the code.

## Key Directory Structure

Under `flink-table-api-java/src/main/java/org/apache/flink/table/`:

- `api/` — the public API surface: `TableEnvironment`, `Table`, `TableConfig`, `EnvironmentSettings`, `TableDescriptor`/`FormatDescriptor`, `StatementSet`, `TablePipeline`, `CompiledPlan`, `Expressions` (the `Expressions.$(...)` DSL), the window builders (`Tumble`, `Slide`, `Session`, `Over`), and `Model`/`ModelDescriptor`.
- `api/config/` — `ConfigOption` holders: `ExecutionConfigOptions` (`table.exec.*`), `OptimizerConfigOptions` (`table.optimizer.*`), `TableConfigOptions`, plus `MaterializedTableConfigOptions`, `LookupJoinHintOptions`, `MLPredictRuntimeConfigOptions`.
- `api/internal/` — implementations behind the API interfaces: `TableEnvironmentImpl` (and `TableEnvironmentInternal`), `TableImpl`, `StatementSetImpl`, `TablePipelineImpl`, `TableResultImpl`, `CompiledPlanImpl`, plan caching (`PlanCacheManager`, `CachedPlan`).
- `operations/` — the `Operation` tree: `QueryOperation` and `ModifyOperation` subtrees, `ExecutableOperation`, and the visitors; subpackages `ddl/`, `command/`, `materializedtable/`, `utils/`.
- `delegation/` — SPI interfaces the planner implements: `Planner`, `Executor`, `Parser` (with `PlannerFactory`, `ExecutorFactory`, `ParserFactory`, `InternalPlan`).
- `catalog/` — `CatalogManager` and catalog object impls (`CatalogManager`, `CatalogRegistry`, `ContextResolvedFunction`, in-memory/connector catalog tables).
- `expressions/` — API expression nodes and `resolver/ExpressionResolver` (resolves unresolved API/SQL expressions against a schema).
- `module/` — `ModuleManager` and `ModuleEntry`; `factories/` — `PlannerFactoryUtil`, `TableFactoryUtil`; `resource/`, `functions/`, `typeutils/`, `secret/`, `legacy/`.

## Key Abstractions

- **`TableEnvironment`** (`api/`) — the API entry point: registers catalogs/tables/functions/modules, runs SQL (`executeSql`, `sqlQuery`), creates `Table`s, and builds `StatementSet`s. Implemented by `TableEnvironmentImpl` (which also implements the internal `TableEnvironmentInternal`); created via `TableEnvironment.create(EnvironmentSettings)`.
- **`Table`** (`api/`) — the relational API handle (`extends Explainable<Table>, Executable`); each transformation appends to a `QueryOperation`. Implemented by `TableImpl`.
- **`TableConfig` / `EnvironmentSettings`** (`api/`) — `TableConfig` is the mutable per-environment config (`implements WritableConfig, ReadableConfig`) holding the `Configuration`; `EnvironmentSettings` holds instantiation-time-only settings (batch vs. streaming, catalog/database names).
- **`Operation`** (`operations/`) — the root of the parsed-program tree. `QueryOperation` models relational reads (`ProjectQueryOperation`, `JoinQueryOperation`, ...); `ModifyOperation` models writes/inserts; `ExecutableOperation` is a directly-runnable command. `Parser` turns SQL into `Operation`s; the planner turns `QueryOperation`s into a plan.
- **`Planner` / `Executor` / `Parser`** (`delegation/`) — the boundary to `flink-table-planner`. `TableEnvironmentImpl` holds a `Planner` and `Executor` (resolved via `PlannerFactory`/`ExecutorFactory`) and never depends on planner internals directly.
- **`CatalogManager`** (`catalog/`) — tracks registered catalogs and the current catalog/database; resolves identifiers to `ContextResolvedTable`/`ContextResolvedFunction`.
- **`ConfigOption<T>` holders** (`api/config/`) — `ExecutionConfigOptions` and `OptimizerConfigOptions` are the canonical `table.exec.*` / `table.optimizer.*` keys, built with `ConfigOptions.key(...)` (from `flink-core`) and annotated with `@Documentation.TableOption`.

## Common Change Patterns

### Adding a `table.exec.*` / `table.optimizer.*` config option

Add a `public static final ConfigOption<T>` to `ExecutionConfigOptions` (runtime tunables) or `OptimizerConfigOptions` (planner/optimizer tunables) in `api/config/`. Per the root [AGENTS.md](../../AGENTS.md), this is where table config options live.

```java
@Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
public static final ConfigOption<Integer> MY_OPTION =
        key("table.exec.my-feature.size")
                .intType()
                .defaultValue(128)
                .withDescription("What this controls.");
```

- `key(...)` is `ConfigOptions.key` (statically imported); the builder chain (`.intType()/.enumType()/...` → `.defaultValue(...)`/`.noDefaultValue()` → `.withDescription(...)`) is the `flink-core` `ConfigOptions` API.
- Annotate with `@Documentation.TableOption(execMode = ...)` (`org.apache.flink.annotation.docs.Documentation`, from `flink-annotations`) using the right `Documentation.ExecMode` (`STREAMING`, `BATCH`, or `BATCH_STREAMING`) so the option lands in the generated table-config docs. The completeness check is `ConfigOptionsDocsCompletenessITCase` in `flink-docs` — it fails if a public option is undocumented.

### Adding a Table API method / operation

1. Add the method to `Table` (or `TableEnvironment`/`StatementSet`) and implement it in `TableImpl` (or `TableEnvironmentImpl`). User-facing API needs a stability annotation (`@PublicEvolving`/`@Experimental`), and new public API generally requires a voted FLIP — **ask first** (root [AGENTS.md](../../AGENTS.md)).
2. If the method introduces a new relational shape, add a `QueryOperation` (or `ModifyOperation`) subclass in `operations/` and handle it in the relevant visitor; the planner (`flink-table-planner`) translates it to an `ExecNode` — see [flink-table-planner/AGENTS.md](../flink-table-planner/AGENTS.md).
3. New SQL syntax goes through the parser grammar in `flink-sql-parser` and `Parser`/`SqlNodeToOperationConversion` in the planner, not here; this module only defines the `Operation` target types.

### Adding a SQL command / DDL operation

Add an `Operation` (often an `ExecutableOperation`) under `operations/ddl/`, `operations/command/`, or `operations/materializedtable/`. `ExecutableOperation`s implement their own `execute(Context)`; the planner is responsible for wiring SQL text to the new operation.

## Testing Patterns

JUnit 5 (`org.junit.jupiter`) + AssertJ (`org.assertj`); tests mirror the source package layout under `src/test/java/`.

- **API / operation unit tests:** plain `*Test` classes (e.g. `TableEnvironmentTest`, `TableConfigTest`, `TableDescriptorTest`, `EnvironmentSettingsTest`, `QueryOperationTest`) asserting with AssertJ. No `MiniCluster` is needed — these test API behavior and operation construction, not execution.
- **Test mocks / doubles** (`src/test/.../utils/`): `TableEnvironmentMock`, `PlannerMock`, `ExecutorMock`, `ParserMock`, `CatalogManagerMocks`, `ExpressionResolverMocks`, `FunctionLookupMock`, `ModuleMock` — let API tests run without a real planner/executor.
- **Table test programs** (`src/test/.../test/program/`): `TableTestProgram` + `TableTestProgramRunner` and the `TestStep` family (`SqlTestStep`, `TableApiTestStep`, `ConfigOptionTestStep`, `SourceTestStep`/`SinkTestStep`, ...) define declarative, reusable program specs. These are the building blocks consumed by the planner's semantic/restore test bases — see [flink-table-planner/AGENTS.md](../flink-table-planner/AGENTS.md).
- **Catalog tests:** `CatalogTestBase` (`src/test/.../catalog/`) is the base for catalog-contract tests.
