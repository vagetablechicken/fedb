package com._4paradigm.dataimporter;

import com._4paradigm.openmldb.DataType;
import com._4paradigm.openmldb.SQLInsertRow;
import com._4paradigm.openmldb.Schema;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

class InsertImporter implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(InsertImporter.class);
    private final SqlExecutor router;
    private final String dbName;
    private final String tableName;
    private final List<CSVRecord> records;
    private final Pair<Integer, Integer> range;

    public InsertImporter(SqlExecutor router, String dbName, String tableName, List<CSVRecord> records, Pair<Integer, Integer> range) {
        this.router = router;
        this.dbName = dbName;
        this.tableName = tableName;
        this.records = records;
        this.range = range;
    }

    @Override
    public void run() {
        logger.info("Thread {} insert range {} ", Thread.currentThread().getId(), range);
        if (records.isEmpty()) {
            logger.warn("records is empty");
            return;
        }
        // Use one record to generate insert place holder
        StringBuilder builder = new StringBuilder("insert into " + tableName + " values(");
        CSVRecord peekRecord = records.get(0);
        for (int i = 0; i < peekRecord.size(); ++i) {
            builder.append((i == 0) ? "?" : ",?");
        }
        builder.append(");");
        String insertPlaceHolder = builder.toString();
        SQLInsertRow insertRow = router.getInsertRow(dbName, insertPlaceHolder);
        if (insertRow == null) {
            logger.warn("get insert row failed");
            return;
        }
        Schema schema = insertRow.GetSchema();

        // TODO(hw): record.getParser().getHeaderMap() check schema? In future, we may read multi files, so check schema in each worker.
        // What if the header of record is missing?
        List<String> stringCols = getStringColumnsFromSchema(schema);
        for (int i = range.getLeft(); i < range.getRight(); i++) {
            // insert placeholder
            SQLInsertRow row = router.getInsertRow(dbName, insertPlaceHolder);
            CSVRecord record = records.get(i);
//            logger.info("{}", record.getParser().getHeaderMap());
            int strLength = stringCols.stream().mapToInt(col -> record.get(col).length()).sum();

            row.Init(strLength);
            boolean rowIsValid = true;
            for (int j = 0; j < schema.GetColumnCnt(); j++) {
                String v = record.get(schema.GetColumnName(j));
                DataType type = schema.GetColumnType(j);
                if (!appendToRow(v, type, row)) {
                    logger.warn("append to row failed, can't insert");
                    rowIsValid = false;
                }
            }
            if (rowIsValid) {
                logger.debug("insert one row data size {}", row.GetRow().length());
                boolean ok = router.executeInsert(dbName, insertPlaceHolder, row);
                // TODO(hw): retry
                if (!ok) {
                    logger.error("insert one row failed, {}", record);
                }
            }
        }
    }

    public static List<String> getStringColumnsFromSchema(Schema schema) {
        List<String> stringCols = new ArrayList<>();
        for (int i = 0; i < schema.GetColumnCnt(); i++) {
            if (schema.GetColumnType(i) == DataType.kTypeString) {
                // TODO(hw): what if data don't have column names?
                stringCols.add(schema.GetColumnName(i));
            }
        }
        return stringCols;
    }

    public static boolean appendToRow(String v, DataType type, SQLInsertRow row) {
        // TODO(hw): true/false case sensitive? is null?
        // csv isSet?
        if (DataType.kTypeBool.equals(type)) {
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
            // TODO(hw): no need to support data time. Converting here is only for simplify. May fix later.
            if (v.contains("-")) {
                Timestamp ts = Timestamp.valueOf(v);
                return row.AppendTimestamp(ts.getTime()); // milliseconds
            }
            return row.AppendTimestamp(Long.parseLong(v));
        }
        return false;
    }
}
