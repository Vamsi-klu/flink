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

# flink-core

Core implementation shared by all Flink APIs: the type system and serialization stack, memory management, configuration, file systems, and IO formats. The public, user-facing API *interfaces* live in `flink-core-api`; this module is the implementation (plus the older `org.apache.flink.api.common.*` runtime-facing classes). Changes here ripple into nearly every other module, and serializer changes affect state/checkpoint compatibility.

## Build Commands

Build this module and its dependencies:

```
./mvnw clean install -DskipTests -pl flink-core -am
```

After the first `-am` build, drop `-am` for faster rebuilds when only touching this module.

Checkstyle **is** enforced on `flink-core`: the root `maven-checkstyle-plugin` binding runs in the `validate` phase with `failOnViolation=true` and is inherited by every module. `flink-core` overrides only its suppressions file (`combine.self="override"` → `tools/maven/suppressions-core.xml`), which exempts a handful of specific files/checks but does not disable enforcement. Run `./mvnw spotless:apply` after edits to format the code; CI runs `spotless:check`.

## Key Directory Structure

Under `flink-core/src/main/java/org/apache/flink/`:

- `api/common/typeinfo/` — `TypeInformation<T>` and the built-in type descriptors (`BasicTypeInfo`, `BasicArrayTypeInfo`, etc.)
- `api/common/typeutils/` — The serialization core: `TypeSerializer<T>`, `TypeSerializerSnapshot<T>`, `TypeSerializerSchemaCompatibility<T>`, composite/snapshot helpers (`CompositeTypeSerializerSnapshot`, `SimpleTypeSerializerSnapshot`), and `TypeComparator`
- `api/common/typeutils/base/` — Concrete serializers for primitive/common types
- `api/java/typeutils/runtime/` — Composite serializers and their snapshots (`PojoSerializer`, `RowSerializer`, `TupleSerializer`, `NullableSerializer`, `EitherSerializer`)
- `api/common/serialization/` — `SerializationSchema` / `DeserializationSchema`, `BulkWriter`, `Encoder`, `SerializerConfig`
- `api/common/io/` — `InputFormat<T, S>` / `OutputFormat<T>` base interfaces
- `api/common/state/` — `StateDescriptor` subclasses (`ListStateDescriptor`, `MapStateDescriptor`, ...), `StateTtlConfig`, `CheckpointListener`
- `api/common/functions/` — User-function interfaces and `AbstractRichFunction`
- `core/memory/` — `MemorySegment`, `MemorySegmentFactory`, the `DataInputView`/`DataOutputView` abstractions and their stream/array-backed implementations
- `core/fs/` — `FileSystem`, `Path`, `FileSystemFactory`, `FSDataInputStream`/`FSDataOutputStream`
- `core/io/` — `IOReadableWritable`, `VersionedIOReadableWritable`/`PostVersionedIOReadableWritable`, `SimpleVersionedSerialization`, input-split types
- `configuration/` — `Configuration`, `ConfigOption<T>`, `ConfigOptions`, and the `*Options` classes (`CoreOptions`, `ExecutionOptions`, `PipelineOptions`, `CheckpointingOptions`, ...)
- `types/` — `Value` types (`IntValue`, `StringValue`, `NullValue`, ...), `Row`, `RowKind`, `Either`
- `util/` — General utilities (`Collector`, `CloseableIterator`, classloader utils, function helpers)

## Key Abstractions

- **`TypeInformation<T>`** (`api/common/typeinfo/`) — Describes a type and produces its serializer via `createSerializer(SerializerConfig)`. The root of the type system.
- **`TypeSerializer<T>`** (`api/common/typeutils/`) — Abstract serializer. Implements `serialize`/`deserialize` (against `DataOutputView`/`DataInputView`), `duplicate()` (stateful serializers must return an independent copy for use by a different thread), `copy`, `isImmutableType`, `getLength`, `equals`/`hashCode`, and `snapshotConfiguration()` returning a `TypeSerializerSnapshot<T>`.
- **`TypeSerializerSnapshot<T>`** (`api/common/typeutils/`) — The point-in-time, persisted configuration of a serializer; the compatibility contract for state/checkpoint evolution. Key methods: `getCurrentVersion()`, `writeSnapshot(DataOutputView)`, `readSnapshot(int readVersion, DataInputView, ClassLoader)`, `restoreSerializer()`, and `resolveSchemaCompatibility(TypeSerializerSnapshot<T> oldSerializerSnapshot)` returning a `TypeSerializerSchemaCompatibility<T>` (compatible-as-is / after-migration / after-reconfiguration / incompatible).
- **`MemorySegment`** (`core/memory/`) — The off-/on-heap byte-addressable memory unit underpinning Flink's managed memory; created via `MemorySegmentFactory`.
- **`DataInputView` / `DataOutputView`** (`core/memory/`) — The read/write abstractions all serializers use; backed by streams (`DataInputViewStreamWrapper`), byte arrays (`DataInputDeserializer`/`DataOutputSerializer`), or paged memory.
- **`ConfigOption<T>` + `ConfigOptions.key(...)`** (`configuration/`) — Typed, documented configuration keys. Built with the `OptionBuilder` returned by `ConfigOptions.key("...")`, then `.<type>Type().defaultValue(...)` / `.noDefaultValue()`, then `.withDescription(...)`.
- **`FileSystem` / `Path`** (`core/fs/`) — The pluggable file-system abstraction. `FileSystem.get(URI)` resolves a scheme to an implementation via registered `FileSystemFactory` SPI providers; `Path` is the location type.
- **`IOReadableWritable`** (`core/io/`) — Minimal `read(DataInputView)`/`write(DataOutputView)` contract; `Post`/`VersionedIOReadableWritable` add version handling for evolvable on-wire formats.

## Common Change Patterns

### Modifying or adding a `TypeSerializer`

Serializers are paired with a `TypeSerializerSnapshot` that governs forward/backward compatibility of any state written with the serializer. **This is an "Ask first" area** — a wrong change silently breaks restoring from existing checkpoints/savepoints.

- The snapshot must have a **public no-arg constructor** (it is instantiated reflectively during restore).
- When you change the serialized format or the snapshot's persisted fields, **bump `getCurrentVersion()`** and keep `readSnapshot(readVersion, ...)` able to read all older versions.
- Implement `resolveSchemaCompatibility(oldSnapshot)` to declare how an old-state serializer relates to the new one (compatible as-is, compatible after migration, after reconfiguration, or incompatible).
- For composite serializers (wrapping nested serializers), extend `CompositeTypeSerializerSnapshot` rather than hand-rolling delegation; for stateless serializers, `SimpleTypeSerializerSnapshot` suffices.
- Snapshot/upgrade tests live in `flink-core/src/test/java/org/apache/flink/api/common/typeutils/` (`SerializerTestBase`, `TypeSerializerUpgradeTestBase`) and, for composite serializers, alongside them in `api/java/typeutils/runtime/` (e.g. `PojoSerializerUpgradeTest`, `RowSerializerUpgradeTest`).

### Adding a configuration option

Define a `public static final ConfigOption<T>` in the relevant `*Options` class under `configuration/` (e.g. `CoreOptions`, `ExecutionOptions`, `PipelineOptions`):

```java
public static final ConfigOption<Integer> MY_OPTION =
        ConfigOptions.key("my.feature.size")
                .intType()
                .defaultValue(128)
                .withDescription("What this controls.");
```

Use `.noDefaultValue()` when there is no sensible default, and `withDeprecatedKeys(...)`/`FallbackKey` for renames. Annotate with `@Documentation.Section(...)` / `@Documentation.SuffixOption` etc. (`org.apache.flink.annotation.docs.Documentation`, in `flink-annotations`) so the option appears in generated config docs — the generator and its completeness check live in `flink-docs` (`ConfigOptionsDocGenerator`, `ConfigOptionsDocsCompletenessITCase`), so a new documented option must be reflected there or that ITCase fails.

### Adding a `FileSystem` implementation

Subclass `FileSystem` (implementing `getFileStatus`, `open`, `create`, `listStatus`, `getWorkingDirectory`/`getHomeDirectory`, `getUri`, ...), provide a `FileSystemFactory` (with the scheme it handles), and register it via `META-INF/services/org.apache.flink.core.fs.FileSystemFactory`. `FileSystem.get(URI)` dispatches by scheme.

### Adding a memory/IO utility

New low-level read/write helpers should operate against `DataInputView`/`DataOutputView` (not raw streams) so they compose with serializers; on-wire formats that may evolve should extend `VersionedIOReadableWritable`/`PostVersionedIOReadableWritable` or use `SimpleVersionedSerialization` rather than inventing ad-hoc versioning.

## Testing Patterns

- **JUnit 5 + AssertJ** is the repo standard (per root [AGENTS.md](../AGENTS.md)). Tests mirror the source package layout under `flink-core/src/test/java/`.
- **Serializer correctness:** extend `SerializerTestBase<T>` — it exercises round-trip serialize/deserialize, `copy`, `duplicate`, length, and snapshot serialization for your serializer.
- **Serializer upgrades/compatibility:** extend `TypeSerializerUpgradeTestBase` — verifies that state written by a previous serializer version restores and that `resolveSchemaCompatibility` reports the right outcome. See concrete cases like `PojoSerializerUpgradeTest`, `RowSerializerUpgradeTest`, `CompositeTypeSerializerUpgradeTest`.
- **Type info / comparators:** `TypeInformationTestBase`, comparator behavior via `ComparatorTestBase`.
- **Config docs:** there is no doc-generation test inside flink-core; the completeness check is `ConfigOptionsDocsCompletenessITCase` in `flink-docs` — run it there after adding/changing options.
