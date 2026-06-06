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

package org.apache.flink.table.planner.functions.sql;

import org.apache.flink.table.api.ValidationException;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.type.SqlTypeUtil;

/**
 * Shared operand checks for the native-Calcite JSON operators {@code JSON_VALUE}, {@code
 * JSON_QUERY} and {@code JSON_EXISTS}.
 *
 * <p>Calcite registers these functions with {@code SqlTypeFamily.ANY} for the JSON document
 * operand, so the SQL validation path accepts a non-character first argument that the Table API
 * {@code InputTypeStrategy} already rejects. These helpers tighten the SQL path to match
 * (FLINK-34507).
 */
final class JsonFunctionsOperandChecks {

    private JsonFunctionsOperandChecks() {}

    /**
     * Requires operand 0 (the JSON document) to be a character string.
     *
     * <p>Operand 0 always exists for these functions (the parser requires a document and a path), so
     * this may be called before or after {@code super.checkOperandTypes(...)}.
     *
     * @return {@code true} if operand 0 is a character string; otherwise either throws a {@link
     *     ValidationException} (when {@code throwOnFailure} is {@code true}) or returns {@code
     *     false} without throwing, so Calcite overload resolution is not disturbed.
     */
    static boolean checkFirstOperandIsCharacter(
            SqlCallBinding callBinding, boolean throwOnFailure) {
        final RelDataType type = SqlTypeUtil.deriveType(callBinding, callBinding.operand(0));
        if (!SqlTypeUtil.isCharacter(type)) {
            if (throwOnFailure) {
                throw new ValidationException(
                        String.format(
                                "The first argument of '%s' must be a character string but was '%s'. "
                                        + "Wrap it in CAST(... AS STRING) if needed.",
                                callBinding.getOperator().getName(), type));
            }
            return false;
        }
        return true;
    }
}
