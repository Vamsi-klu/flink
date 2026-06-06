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

import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.fun.SqlJsonExistsFunction;

/**
 * A wrapper for Calcite's {@link SqlJsonExistsFunction} that tightens operand 0 (the JSON document)
 * to a character string, aligning the SQL validation path with the already-strict Table API
 * (FLINK-34507).
 *
 * <p>Calcite's base operator uses {@code SqlTypeFamily.ANY} for the document operand and stores its
 * {@code SqlOperandTypeChecker} in a {@code private final} field, so the check is added by
 * overriding {@link #checkOperandTypes(SqlCallBinding, boolean)} rather than by replacing the
 * checker.
 */
public class SqlJsonExistsFunctionWrapper extends SqlJsonExistsFunction {

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        // super validates arity for both the (ANY, CHARACTER) and (ANY, CHARACTER, ANY) overloads.
        if (!super.checkOperandTypes(callBinding, throwOnFailure)) {
            return false;
        }
        return JsonFunctionsOperandChecks.checkFirstOperandIsCharacter(callBinding, throwOnFailure);
    }
}
