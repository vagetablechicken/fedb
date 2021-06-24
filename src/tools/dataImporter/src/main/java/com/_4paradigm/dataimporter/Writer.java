package com._4paradigm.dataimporter;

import com._4paradigm.hybridsql.fedb.DataType;
import com._4paradigm.hybridsql.fedb.SQLInsertRow;
import com._4paradigm.hybridsql.fedb.Schema;
import com._4paradigm.hybridsql.fedb.sdk.SdkOption;
import com._4paradigm.hybridsql.fedb.sdk.SqlException;
import com._4paradigm.hybridsql.fedb.sdk.SqlExecutor;
import com._4paradigm.hybridsql.fedb.sdk.impl.SqlClusterExecutor;

public class Writer {
    private SqlExecutor router;

    Writer(SdkOption options) {
        try {
            router = new SqlClusterExecutor(options);
        } catch (SqlException e) {
            e.printStackTrace();
        }
    }

    // TODO(hw): [] or list?
    public static boolean Vec2SQLRequestRow(String[] values, SQLInsertRow row) {
        Schema sch = row.GetSchema(); // TODO(hw): file schema is checked before, so here needs schema to convert string to proper type

        // scan all strings to init the total string length
        int strLenSum = 0;
        for (int i = 0; i < values.length; i++) {
            if (sch.GetColumnType(i) != DataType.kTypeString) {
                continue;
            }
            strLenSum += values[i].length();
        }
        row.Init(strLenSum);

        for (int i = 0; i < values.length; i++) {
            if (AppendJsonValue(values[i], sch.GetColumnType(i), sch.IsColumnNotNull(i), row)) {
                return false;
            }
        }
        return true;
    }

    public static boolean AppendJsonValue(String v, DataType type, boolean is_not_null, SQLInsertRow row) {
        // check if null
        // TODO(hw): string null?
//        if (v.IsNull()) {
//            if (is_not_null) {
//                return false;
//            }
//            return row.AppendNULL();
//        }

        if (DataType.kTypeBool.equals(type)) {// TODO(hw): true/false case sensitive?
            return row.AppendBool(v.equals("true"));
        } else if (DataType.kTypeInt16.equals(type)) {
            return row.AppendInt16(Short.parseShort(v));
        } else if (DataType.kTypeInt32.equals(type)) {
            return row.AppendInt32(Integer.parseInt(v));
        } else if (DataType.kTypeInt64.equals(type)) {
            return row.AppendInt64(Long.parseLong(v));
        } else if (DataType.kTypeFloat.equals(type)) {
            return row.AppendFloat(Float.parseFloat(v));
        } else if (DataType.kTypeDouble.equals(type)) {
            return row.AppendDouble(Double.parseDouble(v));
        } else if (DataType.kTypeString.equals(type)) {
            return row.AppendString(v);
        } else if (DataType.kTypeDate.equals(type)) {
            String[] parts = v.split("-");
            if (parts.length != 3) {
                return false;
            }
            long year = Long.parseLong(parts[0]);
            long mon = Long.parseLong(parts[1]);
            long day = Long.parseLong(parts[2]);
            return row.AppendDate(year, mon, day);
        } else if (DataType.kTypeTimestamp.equals(type)) {
            return row.AppendTimestamp(Long.parseLong(v));
        }
        return false;
    }
}
