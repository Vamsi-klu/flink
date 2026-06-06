/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.avro;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.avro.AvroFormatOptions.AvroEncoding;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.format.ProjectableDecodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.factories.TestDynamicTableFactory;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link AvroFormatFactory}. */
class AvroFormatFactoryTest {

    private static final ResolvedSchema SCHEMA =
            ResolvedSchema.of(
                    Column.physical("a", DataTypes.STRING()),
                    Column.physical("b", DataTypes.INT()),
                    Column.physical("c", DataTypes.BOOLEAN()),
                    Column.physical("d", DataTypes.TIMESTAMP(3)));

    private static final ResolvedSchema NEW_SCHEMA =
            ResolvedSchema.of(
                    Column.physical("a", DataTypes.STRING()),
                    Column.physical("b", DataTypes.INT()),
                    Column.physical("c", DataTypes.BOOLEAN()),
                    Column.physical("d", DataTypes.TIMESTAMP(3)),
                    Column.physical("e", DataTypes.TIMESTAMP(6)),
                    Column.physical("f", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)),
                    Column.physical("g", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6)));

    private static final RowType ROW_TYPE =
            (RowType) SCHEMA.toPhysicalRowDataType().getLogicalType();

    private static final RowType NEW_ROW_TYPE =
            (RowType) NEW_SCHEMA.toPhysicalRowDataType().getLogicalType();

    @ParameterizedTest
    @EnumSource(AvroEncoding.class)
    void testSeDeSchema(AvroEncoding encoding) {
        final AvroRowDataDeserializationSchema expectedDeser =
                new AvroRowDataDeserializationSchema(
                        ROW_TYPE, InternalTypeInfo.of(ROW_TYPE), encoding);

        final Map<String, String> options = getAllOptions(true);

        final DynamicTableSource actualSource = FactoryMocks.createTableSource(SCHEMA, options);
        assertThat(actualSource).isInstanceOf(TestDynamicTableFactory.DynamicTableSourceMock.class);
        TestDynamicTableFactory.DynamicTableSourceMock scanSourceMock =
                (TestDynamicTableFactory.DynamicTableSourceMock) actualSource;

        DeserializationSchema<RowData> actualDeser =
                scanSourceMock.valueFormat.createRuntimeDecoder(
                        ScanRuntimeProviderContext.INSTANCE, SCHEMA.toPhysicalRowDataType());

        assertThat(actualDeser).isEqualTo(expectedDeser);

        final AvroRowDataSerializationSchema expectedSer =
                new AvroRowDataSerializationSchema(ROW_TYPE, encoding);

        final DynamicTableSink actualSink = FactoryMocks.createTableSink(SCHEMA, options);
        assertThat(actualSink).isInstanceOf(TestDynamicTableFactory.DynamicTableSinkMock.class);
        TestDynamicTableFactory.DynamicTableSinkMock sinkMock =
                (TestDynamicTableFactory.DynamicTableSinkMock) actualSink;

        SerializationSchema<RowData> actualSer =
                sinkMock.valueFormat.createRuntimeEncoder(null, SCHEMA.toPhysicalRowDataType());

        assertThat(actualSer).isEqualTo(expectedSer);
    }

    @Test
    void testOldSeDeNewSchema() {
        assertThatThrownBy(
                        () -> {
                            new AvroRowDataDeserializationSchema(
                                    NEW_ROW_TYPE, InternalTypeInfo.of(NEW_ROW_TYPE));
                        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Avro does not support TIMESTAMP type with precision: 6, it only supports precision less than 3.");

        assertThatThrownBy(
                        () -> {
                            new AvroRowDataSerializationSchema(NEW_ROW_TYPE);
                        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Avro does not support TIMESTAMP type with precision: 6, it only supports precision less than 3.");
    }

    @Test
    void testNewSeDeNewSchema() {
        testSeDeSchema(NEW_ROW_TYPE, NEW_SCHEMA, false);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testSeDeSchema(boolean legacyTimestampMapping) {
        testSeDeSchema(ROW_TYPE, SCHEMA, legacyTimestampMapping);
    }

    void testSeDeSchema(RowType rowType, ResolvedSchema schema, boolean legacyTimestampMapping) {
        final AvroRowDataDeserializationSchema expectedDeser =
                new AvroRowDataDeserializationSchema(
                        rowType,
                        InternalTypeInfo.of(rowType),
                        AvroEncoding.BINARY,
                        legacyTimestampMapping);

        final Map<String, String> options = getAllOptions(legacyTimestampMapping);

        final DynamicTableSource actualSource = FactoryMocks.createTableSource(schema, options);
        assertThat(actualSource).isInstanceOf(TestDynamicTableFactory.DynamicTableSourceMock.class);
        TestDynamicTableFactory.DynamicTableSourceMock scanSourceMock =
                (TestDynamicTableFactory.DynamicTableSourceMock) actualSource;

        DeserializationSchema<RowData> actualDeser =
                scanSourceMock.valueFormat.createRuntimeDecoder(
                        ScanRuntimeProviderContext.INSTANCE, schema.toPhysicalRowDataType());

        assertThat(actualDeser).isEqualTo(expectedDeser);

        final AvroRowDataSerializationSchema expectedSer =
                new AvroRowDataSerializationSchema(
                        rowType, AvroEncoding.BINARY, legacyTimestampMapping);

        final DynamicTableSink actualSink = FactoryMocks.createTableSink(schema, options);
        assertThat(actualSink).isInstanceOf(TestDynamicTableFactory.DynamicTableSinkMock.class);
        TestDynamicTableFactory.DynamicTableSinkMock sinkMock =
                (TestDynamicTableFactory.DynamicTableSinkMock) actualSink;

        SerializationSchema<RowData> actualSer =
                sinkMock.valueFormat.createRuntimeEncoder(null, schema.toPhysicalRowDataType());

        assertThat(actualSer).isEqualTo(expectedSer);
    }

    // ------------------------------------------------------------------------
    //  Projection pushdown (FLINK-35324)
    // ------------------------------------------------------------------------

    private static final ResolvedSchema PROJ_SCHEMA =
            ResolvedSchema.of(
                    Column.physical("a", DataTypes.STRING()),
                    Column.physical("b", DataTypes.INT()),
                    Column.physical("c", DataTypes.BOOLEAN()),
                    Column.physical("d", DataTypes.BIGINT()));

    private static final RowType PROJ_ROW_TYPE =
            (RowType) PROJ_SCHEMA.toPhysicalRowDataType().getLogicalType();

    @Test
    void testProjectionPushdownNoOpProjection() throws Exception {
        final byte[] bytes = serializeFull(fullRow(), AvroEncoding.BINARY);
        final int[][] projection = {{0}, {1}, {2}, {3}};
        final RowData actual =
                createProjectedDeserializationSchema(AvroEncoding.BINARY, projection)
                        .deserialize(bytes);
        assertThat(materialize(actual, projection))
                .isEqualTo(GenericRowData.of(StringData.fromString("hello"), 33, true, 7L));
    }

    @Test
    void testProjectionPushdownContiguousPrefix() throws Exception {
        final byte[] bytes = serializeFull(fullRow(), AvroEncoding.BINARY);
        final int[][] projection = {{0}, {1}, {2}};
        final RowData actual =
                createProjectedDeserializationSchema(AvroEncoding.BINARY, projection)
                        .deserialize(bytes);
        assertThat(materialize(actual, projection))
                .isEqualTo(GenericRowData.of(StringData.fromString("hello"), 33, true));
    }

    /** The JIRA reproducer: a projection that skips a leading field. Fails on master. */
    @Test
    void testProjectionPushdownSkipLeadingFieldRegression() throws Exception {
        final byte[] bytes = serializeFull(fullRow(), AvroEncoding.BINARY);
        final int[][] projection = {{1}, {2}};
        final RowData actual =
                createProjectedDeserializationSchema(AvroEncoding.BINARY, projection)
                        .deserialize(bytes);
        // On master this throws ArrayIndexOutOfBoundsException (the schema-less reader mis-reads
        // field 0's bytes as field 1); after the fix it must decode correctly without exception.
        assertThat(materialize(actual, projection)).isEqualTo(GenericRowData.of(33, true));
    }

    @Test
    void testProjectionPushdownSingleNonZeroField() throws Exception {
        final byte[] bytes = serializeFull(fullRow(), AvroEncoding.BINARY);
        assertThat(
                        materialize(
                                createProjectedDeserializationSchema(
                                                AvroEncoding.BINARY, new int[][] {{1}})
                                        .deserialize(bytes),
                                new int[][] {{1}}))
                .isEqualTo(GenericRowData.of(33));
        assertThat(
                        materialize(
                                createProjectedDeserializationSchema(
                                                AvroEncoding.BINARY, new int[][] {{3}})
                                        .deserialize(bytes),
                                new int[][] {{3}}))
                .isEqualTo(GenericRowData.of(7L));
    }

    @Test
    void testProjectionPushdownNonContiguous() throws Exception {
        final byte[] bytes = serializeFull(fullRow(), AvroEncoding.BINARY);
        assertThat(
                        materialize(
                                createProjectedDeserializationSchema(
                                                AvroEncoding.BINARY, new int[][] {{0}, {2}})
                                        .deserialize(bytes),
                                new int[][] {{0}, {2}}))
                .isEqualTo(GenericRowData.of(StringData.fromString("hello"), true));
        assertThat(
                        materialize(
                                createProjectedDeserializationSchema(
                                                AvroEncoding.BINARY, new int[][] {{1}, {3}})
                                        .deserialize(bytes),
                                new int[][] {{1}, {3}}))
                .isEqualTo(GenericRowData.of(33, 7L));
    }

    /** Reordered projections: the discriminator that a naive decode-full-then-return fails. */
    @Test
    void testProjectionPushdownReordered() throws Exception {
        final byte[] bytes = serializeFull(fullRow(), AvroEncoding.BINARY);
        assertThat(
                        materialize(
                                createProjectedDeserializationSchema(
                                                AvroEncoding.BINARY, new int[][] {{2}, {0}})
                                        .deserialize(bytes),
                                new int[][] {{2}, {0}}))
                .isEqualTo(GenericRowData.of(true, StringData.fromString("hello")));
        assertThat(
                        materialize(
                                createProjectedDeserializationSchema(
                                                AvroEncoding.BINARY, new int[][] {{3}, {1}})
                                        .deserialize(bytes),
                                new int[][] {{3}, {1}}))
                .isEqualTo(GenericRowData.of(7L, 33));
        assertThat(
                        materialize(
                                createProjectedDeserializationSchema(
                                                AvroEncoding.BINARY, new int[][] {{1}, {0}})
                                        .deserialize(bytes),
                                new int[][] {{1}, {0}}))
                .isEqualTo(GenericRowData.of(33, StringData.fromString("hello")));
    }

    @Test
    void testProjectionPushdownEmptyProjection() throws Exception {
        final byte[] bytes = serializeFull(fullRow(), AvroEncoding.BINARY);
        final RowData actual =
                createProjectedDeserializationSchema(AvroEncoding.BINARY, new int[][] {})
                        .deserialize(bytes);
        assertThat(materialize(actual, new int[][] {})).isEqualTo(GenericRowData.of());
    }

    @Test
    void testProjectionPushdownNullMessage() throws Exception {
        assertThat(
                        createProjectedDeserializationSchema(
                                        AvroEncoding.BINARY, new int[][] {{1}, {2}})
                                .deserialize(null))
                .isNull();
    }

    @ParameterizedTest
    @EnumSource(AvroEncoding.class)
    void testProjectionPushdownBothEncodings(AvroEncoding encoding) throws Exception {
        final byte[] bytes = serializeFull(fullRow(), encoding);
        final int[][] projection = {{1}, {2}};
        final RowData actual =
                createProjectedDeserializationSchema(encoding, projection).deserialize(bytes);
        assertThat(materialize(actual, projection)).isEqualTo(GenericRowData.of(33, true));
    }

    @Test
    void testProjectionProducedTypeAndEquality() {
        // produced type follows the projection order
        final DeserializationSchema<RowData> reordered =
                createProjectedDeserializationSchemaUnopened(
                        AvroEncoding.BINARY, new int[][] {{3}, {1}});
        assertThat(reordered.getProducedType())
                .isEqualTo(
                        InternalTypeInfo.of(
                                (RowType)
                                        Projection.of(new int[][] {{3}, {1}})
                                                .project(PROJ_SCHEMA.toPhysicalRowDataType())
                                                .getLogicalType()));

        // identity projection short-circuits to the bare AvroRowDataDeserializationSchema
        final DeserializationSchema<RowData> identity =
                createProjectedDeserializationSchemaUnopened(
                        AvroEncoding.BINARY, new int[][] {{0}, {1}, {2}, {3}});
        assertThat(identity)
                .isInstanceOf(AvroRowDataDeserializationSchema.class)
                .isEqualTo(
                        new AvroRowDataDeserializationSchema(
                                PROJ_ROW_TYPE, InternalTypeInfo.of(PROJ_ROW_TYPE)));

        // two wrappers with the same projection are equal; a different projection is not
        final DeserializationSchema<RowData> a =
                createProjectedDeserializationSchemaUnopened(
                        AvroEncoding.BINARY, new int[][] {{1}, {2}});
        final DeserializationSchema<RowData> b =
                createProjectedDeserializationSchemaUnopened(
                        AvroEncoding.BINARY, new int[][] {{1}, {2}});
        final DeserializationSchema<RowData> c =
                createProjectedDeserializationSchemaUnopened(
                        AvroEncoding.BINARY, new int[][] {{2}, {1}});
        assertThat(a).isInstanceOf(ProjectingAvroRowDataDeserializationSchema.class);
        assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
        assertThat(a).isNotEqualTo(c);
    }

    private static GenericRowData fullRow() {
        return GenericRowData.of(StringData.fromString("hello"), 33, true, 7L);
    }

    private static byte[] serializeFull(RowData full, AvroEncoding encoding) throws Exception {
        final AvroRowDataSerializationSchema ser =
                new AvroRowDataSerializationSchema(PROJ_ROW_TYPE, encoding, true);
        ser.open(null);
        return ser.serialize(full);
    }

    /** Materializes any {@link RowData} (incl. a non-comparable {@code ProjectedRowData}). */
    private static GenericRowData materialize(RowData row, int[][] projections) {
        final RowType projectedType =
                (RowType)
                        Projection.of(projections)
                                .project(PROJ_SCHEMA.toPhysicalRowDataType())
                                .getLogicalType();
        final GenericRowData out = new GenericRowData(projectedType.getFieldCount());
        for (int i = 0; i < projectedType.getFieldCount(); i++) {
            out.setField(
                    i,
                    RowData.createFieldGetter(projectedType.getTypeAt(i), i).getFieldOrNull(row));
        }
        return out;
    }

    private DeserializationSchema<RowData> createProjectedDeserializationSchema(
            AvroEncoding encoding, int[][] projections) throws Exception {
        final DeserializationSchema<RowData> deser =
                createProjectedDeserializationSchemaUnopened(encoding, projections);
        deser.open(null);
        return deser;
    }

    @SuppressWarnings("unchecked")
    private DeserializationSchema<RowData> createProjectedDeserializationSchemaUnopened(
            AvroEncoding encoding, int[][] projections) {
        final Map<String, String> options = getAllOptions(true);
        options.put("avro.encoding", encoding.name());
        final DynamicTableSource source = FactoryMocks.createTableSource(PROJ_SCHEMA, options);
        final TestDynamicTableFactory.DynamicTableSourceMock sourceMock =
                (TestDynamicTableFactory.DynamicTableSourceMock) source;
        final ProjectableDecodingFormat<DeserializationSchema<RowData>> valueFormat =
                (ProjectableDecodingFormat<DeserializationSchema<RowData>>) sourceMock.valueFormat;
        return valueFormat.createRuntimeDecoder(
                ScanRuntimeProviderContext.INSTANCE,
                PROJ_SCHEMA.toPhysicalRowDataType(),
                projections);
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    private Map<String, String> getAllOptions(boolean legacyTimestampMapping) {
        final Map<String, String> options = new HashMap<>();
        options.put("connector", TestDynamicTableFactory.IDENTIFIER);
        options.put("target", "MyTarget");
        options.put("buffer-size", "1000");

        if (!legacyTimestampMapping) {
            options.put("avro.timestamp_mapping.legacy", "false");
        }

        options.put("format", AvroFormatFactory.IDENTIFIER);
        return options;
    }
}
