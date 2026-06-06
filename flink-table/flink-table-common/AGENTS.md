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

# flink-table-common

Shared Table/SQL building blocks with minimal dependencies: the type system (`DataType`/`LogicalType`), function definitions and type inference, catalog interfaces, and connector factory/source/sink interfaces. Depended on by `flink-table-planner`, `flink-table-runtime`, and the Table APIs — changes here ripple into all of them.

## Build Commands

```
./mvnw clean install -DskipTests -pl flink-table/flink-table-common -am
```

After the first `-am` build, drop `-am` for faster rebuilds when only changing code within this module.

Run `./mvnw spotless:apply` after every edit (google-java-format, AOSP style). This module **is** checkstyle-enforced — it inherits the root `maven-checkstyle-plugin` binding (`tools/maven/checkstyle.xml`) with no module-level opt-out (see root [AGENTS.md](../../AGENTS.md)). Do not suppress checkstyle; fix the code.

## Key Directory Structure

Under `flink-table/flink-table-common/src/main/java/org/apache/flink/table/`:

- `api/` — user-facing entry types: `DataTypes` (the factory for `DataType`s), `Schema`, the `*Exception` types (`ValidationException`, `TableException`, ...)
- `types/` — `DataType` and its subclasses (`AtomicDataType`, `CollectionDataType`, `FieldsDataType`, `KeyValueDataType`), `AbstractDataType`, `UnresolvedDataType`
- `types/logical/` — the `LogicalType` hierarchy (`IntType`, `VarCharType`, `RowType`, `ArrayType`, `MapType`, `StructuredType`, ...), plus `LogicalTypeRoot`, `LogicalTypeFamily`, and the `LogicalTypeVisitor`
- `types/inference/` — `TypeInference` and the strategy SPIs (`InputTypeStrategy`, `ArgumentTypeStrategy`, `TypeStrategy`) with the `InputTypeStrategies` / `TypeStrategies` factories
- `types/extraction/` — reflection-based extraction of `DataType`s and `TypeInference` from Java classes/methods (`DataTypeExtractor`, `TypeInferenceExtractor`)
- `functions/` — `FunctionDefinition`, `BuiltInFunctionDefinition`, the `BuiltInFunctionDefinitions` registry, `FunctionKind`, and the user-facing UDF base classes (`ScalarFunction`, `TableFunction`, `AggregateFunction`, `ProcessTableFunction`)
- `catalog/` — `Catalog`, `CatalogBaseTable`/`CatalogTable`/`CatalogView`, their `Resolved*` forms, `ResolvedSchema`, `Column`, `ObjectIdentifier`/`ObjectPath`, `DataTypeFactory`
- `factories/` — connector/format/catalog factory interfaces (`Factory`, `DynamicTableSourceFactory`, `DynamicTableSinkFactory`, `FactoryUtil`, `CatalogFactory`, `FormatFactory`)
- `connector/source/`, `connector/sink/` — the runtime-facing connector interfaces (`DynamicTableSource`, `ScanTableSource`, `LookupTableSource`, `DynamicTableSink`) and their provider types
- `data/` — internal data structures (`RowData`, `binary/`, `columnar/`) used by the runtime
- `module/`, `procedures/`, `ml/`, `legacy/` — modules SPI, stored procedures, model functions, and the deprecated descriptor/source/sink APIs

## Key Abstractions

- **`DataType`** (`types/`) — the API-level type: a `LogicalType` plus the conversion class (the JVM class data is exchanged as). Built via the `DataTypes` factory; `getLogicalType()` returns the underlying logical type. `AbstractDataType` is the common supertype shared with `UnresolvedDataType`.
- **`LogicalType`** (`types/logical/`) — the abstract SQL type, independent of any JVM representation. Carries `isNullable()` and a `LogicalTypeRoot` (`getTypeRoot()`); subclasses implement `copy(boolean isNullable)` and `accept(LogicalTypeVisitor)`. `LogicalTypeFamily` groups roots (e.g. `NUMERIC`, `DATETIME`).
- **`FunctionDefinition`** (`functions/`) — the logical description of a function: a `FunctionKind` (`SCALAR`, `TABLE`, `AGGREGATE`, `TABLE_AGGREGATE`, `PROCESS_TABLE`, plus `ASYNC_*` variants) and a `TypeInference`. `BuiltInFunctionDefinition` is the built-in implementation; user functions (`ScalarFunction`, `TableFunction`, ...) extend `UserDefinedFunction`.
- **`BuiltInFunctionDefinitions`** (`functions/`) — the central registry of every built-in SQL function as `public static final BuiltInFunctionDefinition` constants.
- **`TypeInference`** (`types/inference/`) — the resolved inference contract for a function: a list of `StaticArgument`s / an `InputTypeStrategy` (validates and coerces call arguments) and a `TypeStrategy` (computes the output type). Built with `TypeInference.newBuilder()`.
- **`InputTypeStrategy` / `ArgumentTypeStrategy` / `TypeStrategy`** (`types/inference/`) — the inference SPIs; concrete instances are produced by the `InputTypeStrategies` (e.g. `sequence(...)`, `or(...)`, `logical(...)`, `explicit(...)`) and `TypeStrategies` (e.g. `explicit(...)`, `argument(...)`, `nullableIfArgs(...)`) factories.
- **`Catalog`** (`catalog/`) — the catalog SPI for databases/tables/functions. `CatalogBaseTable` is the base of `CatalogTable` and `CatalogView`; the `Resolved*` variants carry a `ResolvedSchema` (resolved `Column`s + constraints). `ObjectIdentifier` (catalog.database.object) and `ObjectPath` identify objects; `DataTypeFactory` resolves type references.
- **`Factory`** (`factories/`) — the SPI base for all pluggable factories. Each declares a `factoryIdentifier()` plus `requiredOptions()` / `optionalOptions()` (`Set<ConfigOption<?>>`); discovered via `META-INF/services/org.apache.flink.table.factories.Factory`.
- **`DynamicTableSource` / `DynamicTableSink`** (`connector/source/`, `connector/sink/`) — the planner-facing connector contracts a `DynamicTableSourceFactory` / `DynamicTableSinkFactory` produces; `ScanTableSource` and `LookupTableSource` specialize sources for scan vs. point-lookup reads.

## Common Change Patterns

### Registering a built-in function

Add a `public static final BuiltInFunctionDefinition` constant in `BuiltInFunctionDefinitions` (`functions/`) using `BuiltInFunctionDefinition.newBuilder()`:

- `.name("...")` and `.kind(SCALAR)` (or `TABLE`/`AGGREGATE`/...).
- `.inputTypeStrategy(...)` from `InputTypeStrategies` (e.g. `sequence(logical(LogicalTypeFamily.NUMERIC), ...)`, wrapped in `or(...)` for overloads) and `.outputTypeStrategy(...)` from `TypeStrategies`.
- `.runtimeClass("...")` pointing at the `BuiltInScalarFunction`/`BuiltInTableFunction`/... implementation in `flink-table-runtime`, or `.runtimeDeferred()` when the planner generates code for it directly.
- Optionally `.callSyntax(...)` (custom SQL rendering) and `.notDeterministic()`.

The runtime implementation lives in `flink-table-runtime` and the planner wiring/tests in `flink-table-planner` — see the root [AGENTS.md](../../AGENTS.md) "Adding a new SQL built-in function" and those modules' AGENTS.md.

### Adding a `LogicalType`

Subclass `LogicalType` in `types/logical/`, add a `LogicalTypeRoot` enum entry (and `LogicalTypeFamily` membership where appropriate), implement `copy(boolean)`, `asSerializableString()`/`asSummaryString()`, `getChildren()`, and `accept(LogicalTypeVisitor)`. Adding a visitor case touches every `LogicalTypeVisitor` implementation across the table modules — a wide blast radius; treat new roots as an ask-first change. Pair it with a matching `DataType` factory method on `DataTypes`.

### Writing a type-inference strategy

Implement `InputTypeStrategy` / `ArgumentTypeStrategy` (argument validation/coercion) or `TypeStrategy` (output type) in `types/inference/`. Prefer composing the existing factories in `InputTypeStrategies` / `TypeStrategies` over a new class; add a hand-written strategy only when composition can't express the rule. Strategies must implement `getExpectedSignatures(...)` so error messages and docs render correctly.

### Adding a connector factory

Implement `DynamicTableSourceFactory` and/or `DynamicTableSinkFactory` (both extend `Factory`) in the connector's own module: provide `factoryIdentifier()`, `requiredOptions()`, `optionalOptions()`, and build a `DynamicTableSource`/`DynamicTableSink`. Register the implementation in `META-INF/services/org.apache.flink.table.factories.Factory`. Use `FactoryUtil.createTableFactoryHelper(...)` / `FactoryUtil.createDynamicTableSource(...)` in the factory body to validate options and discover format factories. Format factories implement `FormatFactory` (or the `DeserializationFormatFactory`/`SerializationFormatFactory` specializations).

### UDF type extraction

User functions rely on reflection-based extraction rather than an explicit `TypeInference`: `TypeInferenceExtractor.forScalarFunction(...)` / `forTableFunction(...)` / `forAggregateFunction(...)` and `DataTypeExtractor` (in `types/extraction/`) derive signatures from method signatures and `@DataTypeHint`/`@FunctionHint` annotations. Adjust extraction here when changing how hints map to types.

## Testing Patterns

JUnit 5 (`org.junit.jupiter`) + AssertJ. Tests mirror the source package layout under `flink-table/flink-table-common/src/test/java/`. This module is dependency-light — most tests are plain unit tests with no cluster.

- **Input strategies:** extend `InputTypeStrategiesTestBase` (`types/inference/`) — a parameterized base that runs a strategy against argument lists and asserts the resolved/expected signatures or the validation error.
- **Output strategies:** extend `TypeStrategiesTestBase` (`types/inference/`) — drives a `TypeStrategy` over call contexts and checks the produced `DataType`.
- **Logical types:** `LogicalTypesTest` (`types/`) and `LogicalTypeChecksTest` / `LogicalTypeMergingTest` (`types/logical/utils/`) cover serializable-string round-trips, type checks/casts, and type merging; `DataTypesTest` (`types/`) covers the `DataTypes` factory.
- **Type extraction:** tests under `types/extraction/` exercise `DataTypeExtractor`/`TypeInferenceExtractor` against annotated sample classes.
- **Factories:** `FactoryUtilTest` and connector-factory tests verify option validation and SPI discovery.
