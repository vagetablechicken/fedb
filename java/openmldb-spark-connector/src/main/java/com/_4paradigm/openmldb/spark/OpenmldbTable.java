/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com._4paradigm.openmldb.spark;

import com._4paradigm.openmldb.sdk.Column;
import com._4paradigm.openmldb.sdk.Schema;
import com._4paradigm.openmldb.sdk.SdkOption;
import com._4paradigm.openmldb.sdk.SqlException;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;
import com._4paradigm.openmldb.spark.write.OpenmldbWriteBuilder;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.SQLException;
import java.sql.Types;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class OpenmldbTable implements SupportsWrite {
    private final String dbName;
    private final String tableName;
    private Set<TableCapability> capabilities;
    private SqlExecutor executor = null;

    public OpenmldbTable(String dbName, String tableName, SdkOption option) {
        this.dbName = dbName;
        this.tableName = tableName;
        try {
            this.executor = new SqlClusterExecutor(option);
            // TODO(hw): check table exists
        } catch (SqlException e) {
            e.printStackTrace();
        }
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
        return new OpenmldbWriteBuilder(info);
    }

    @Override
    public String name() {
        // TODO(hw): db?
        return tableName;
    }

    private DataType sdkTypeToSparkType(int sqlType) {
        switch (sqlType) {
            case Types.BOOLEAN:
                return DataTypes.BooleanType;
            case Types.SMALLINT:
                return DataTypes.ShortType;
            case Types.INTEGER:
                return DataTypes.IntegerType;
            case Types.BIGINT:
                return DataTypes.LongType;
            case Types.FLOAT:
                return DataTypes.FloatType;
            case Types.DOUBLE:
                return DataTypes.DoubleType;
            case Types.VARCHAR:
                return DataTypes.StringType;
            case Types.DATE:
                return DataTypes.DateType;
            case Types.TIMESTAMP:
                return DataTypes.TimestampType;
            default:
                throw new IllegalArgumentException("No support for sql type " + sqlType);
        }
    }

    @Override
    public StructType schema() {
        // TODO(hw): ref scala DefaultSource
        try {
            Schema schema = executor.getTableSchema(dbName, tableName);
            List<Column> schemaList = schema.getColumnList();
            StructField[] fields = new StructField[schemaList.size()];
            for (int i = 0; i < schemaList.size(); i++) {
                Column column = schemaList.get(i);
                fields[i] = new StructField(column.getColumnName(), sdkTypeToSparkType(column.getSqlType()), !column.isNotNull(), Metadata.empty());
            }
            return new StructType(fields);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Set<TableCapability> capabilities() {
        if (capabilities == null) {
            this.capabilities = new HashSet<>();
//            capabilities.add(TableCapability.BATCH_READ);
            capabilities.add(TableCapability.BATCH_WRITE);
        }
        return capabilities;
    }
}
