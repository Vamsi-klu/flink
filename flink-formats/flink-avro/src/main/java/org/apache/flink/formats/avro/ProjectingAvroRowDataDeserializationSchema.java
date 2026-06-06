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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.ProjectedRowData;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * A {@link DeserializationSchema} that decodes the <b>full</b> physical Avro record and then
 * exposes only the projected (and possibly reordered) top-level columns via a {@link
 * ProjectedRowData}.
 *
 * <p>Plain Avro binary carries no writer schema, so it is decoded positionally against the reader
 * schema. Feeding only the projected subset schema to the reader therefore mis-aligns the
 * positional decode for any projection that is not a contiguous prefix starting at index 0,
 * silently corrupting the result or throwing an {@link ArrayIndexOutOfBoundsException}. To stay
 * correct for arbitrary projections, this wrapper decodes every physical field (the bytes contain
 * them all) and projects after decoding (FLINK-35324).
 *
 * <p>The projection is carried as a serializable {@code int[]}; the (mutable, non-serializable)
 * {@link ProjectedRowData} view is allocated lazily per instance at runtime.
 */
@Internal
final class ProjectingAvroRowDataDeserializationSchema implements DeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    /** Decodes the full physical record (the schema-less bytes contain every physical field). */
    private final AvroRowDataDeserializationSchema fullSchemaDeserializer;

    /** Top-level projection indexes (select + reorder), serializable. */
    private final int[] projection;

    /** Produced type information describing the {@code projected} row. */
    private final TypeInformation<RowData> producedTypeInfo;

    /** Per-instance, lazily created reusable projected view. Not serialized. */
    private transient ProjectedRowData projectedRowData;

    ProjectingAvroRowDataDeserializationSchema(
            AvroRowDataDeserializationSchema fullSchemaDeserializer,
            int[] projection,
            TypeInformation<RowData> producedTypeInfo) {
        this.fullSchemaDeserializer = fullSchemaDeserializer;
        this.projection = projection;
        this.producedTypeInfo = producedTypeInfo;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        fullSchemaDeserializer.open(context);
        this.projectedRowData = ProjectedRowData.from(projection);
    }

    @Override
    public RowData deserialize(@Nullable byte[] message) throws IOException {
        final RowData full = fullSchemaDeserializer.deserialize(message);
        if (full == null) {
            // tombstone / empty message -> propagate null, never replaceRow(null)
            return null;
        }
        if (projectedRowData == null) {
            // safety net if open() was skipped (e.g. some unit-test paths)
            projectedRowData = ProjectedRowData.from(projection);
        }
        return projectedRowData.replaceRow(full);
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ProjectingAvroRowDataDeserializationSchema that =
                (ProjectingAvroRowDataDeserializationSchema) o;
        return fullSchemaDeserializer.equals(that.fullSchemaDeserializer)
                && Arrays.equals(projection, that.projection)
                && producedTypeInfo.equals(that.producedTypeInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fullSchemaDeserializer, producedTypeInfo, Arrays.hashCode(projection));
    }
}
