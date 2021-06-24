package com._4paradigm.dataimporter;

import com._4paradigm.openmldb.common.Common.ColumnDesc;
import com._4paradigm.openmldb.type.Type.DataType;
import com._4paradigm.openmldb.common.Common;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RowBuilder {

    private final static Logger logger = LoggerFactory.getLogger(RowBuilder.class);

    private ByteBuffer buf;
    private int size = 0;
    private int cnt = 0;
    List<ColumnDesc> schema = new ArrayList<>();
    private int strFieldCnt = 0;
    private int strFieldStartOffset = 0;
    private int strAddrLength = 0;
    private int strOffset = 0;
    private int schemaVersion = 1;
    private List<Integer> offsetVec = new ArrayList<>();
    private List<Integer> strIdx = new ArrayList<>();

    public RowBuilder(List<ColumnDesc> schema) {
        calcSchemaOffset(schema);
    }

    public RowBuilder(List<ColumnDesc> schema, int schemaversion) {
        this.schemaVersion = schemaversion;
        calcSchemaOffset(schema);
    }

    private void calcSchemaOffset(List<ColumnDesc> schema) {
        strFieldStartOffset = RowCodecCommon.HEADER_LENGTH + RowCodecCommon.getBitMapSize(schema.size());
        this.schema = schema;
        for (int idx = 0; idx < schema.size(); idx++) {
            ColumnDesc column = schema.get(idx);
            if (column.getDataType() == DataType.kVarchar || column.getDataType() == DataType.kString) {
                offsetVec.add(strFieldCnt);
                strIdx.add(idx);
                strFieldCnt++;
            } else {
                if (RowCodecCommon.TYPE_SIZE_MAP.get(column.getDataType()) == null) {
                    logger.warn("type {} is not supported", column.getDataType());
                } else {
                    offsetVec.add(strFieldStartOffset);
                    strFieldStartOffset += RowCodecCommon.TYPE_SIZE_MAP.get(column.getDataType());
                }
            }
        }
    }

    public void SetSchemaVersion(int version) {
        this.schemaVersion = version;
    }

    public int calTotalLength(List<Object> row) throws RuntimeException {
        if (row.size() != schema.size()) {
            throw new RuntimeException("row size is not equal schema size");
        }
        int stringLength = 0;
        for (Integer idx : strIdx) {
            Object obj = row.get(idx);
            if (obj == null) {
                continue;
            }
            stringLength += ((String) obj).getBytes(RowCodecCommon.CHARSET).length;
        }
        return calTotalLength(stringLength);
    }

    public int calTotalLength(int stringLength) throws RuntimeException {
        if (schema.size() == 0) {
            return 0;
        }
        long totalLength = strFieldStartOffset + stringLength;
        if (totalLength + strFieldCnt <= RowCodecCommon.UINT8_MAX) {
            totalLength += strFieldCnt;
        } else if (totalLength + strFieldCnt * 2 <= RowCodecCommon.UINT16_MAX) {
            totalLength += strFieldCnt * 2;
        } else if (totalLength + strFieldCnt * 3 <= RowCodecCommon.UINT24_MAX) {
            totalLength += strFieldCnt * 3;
        } else if (totalLength + strFieldCnt * 4 <= RowCodecCommon.UINT32_MAX) {
            totalLength += strFieldCnt * 4;
        }
        if (totalLength > Integer.MAX_VALUE) {
            throw new RuntimeException("total length is bigger than integer max value");
        }
        return (int) totalLength;
    }

    public ByteBuffer setBuffer(ByteBuffer buffer, int size) {
        if (buffer == null || size == 0 ||
                size < strFieldStartOffset + strFieldCnt) {
            return null;
        }
        if (buffer.order() == ByteOrder.BIG_ENDIAN) {
            buffer = buffer.order(ByteOrder.LITTLE_ENDIAN);
        }
        this.size = size;
        buffer.put((byte) 1); // FVersion
        buffer.put((byte) schemaVersion); // SVersion
        buffer.putInt(size); // size
        for (int idx = 0; idx < RowCodecCommon.getBitMapSize(schema.size()); idx++) {
            buffer.put((byte) 0xFF);
        }
        this.buf = buffer;
        strAddrLength = RowCodecCommon.getAddrLength(size);
        strOffset = strFieldStartOffset + strAddrLength * strFieldCnt;
        return this.buf;
    }

    private void setField(int index) {
        int pos = RowCodecCommon.HEADER_LENGTH + (index >> 3);
        byte bt = buf.get(pos);
        buf.put(pos, (byte) (bt & (~(1 << (cnt & 0x07)))));
    }

    private boolean check(DataType type) {
        if (cnt >= schema.size()) {
            return false;
        }
        ColumnDesc column = schema.get(cnt);
        if (column.getDataType() != type) {
            return false;
        }
        if (column.getDataType() != DataType.kVarchar && column.getDataType() != DataType.kString) {
            if (RowCodecCommon.TYPE_SIZE_MAP.get(column.getDataType()) == null) {
                return false;
            }
        }
        return true;
    }

    private void setStrOffset(int strPos) {
        if (strPos >= strFieldCnt) {
            return;
        }
        int index = strFieldStartOffset + strAddrLength * strPos;
        buf.position(index);
        if (strAddrLength == 1) {
            buf.put((byte) (strOffset & 0xFF));
        } else if (strAddrLength == 2) {
            buf.putShort((short) (strOffset & 0xFFFF));
        } else if (strAddrLength == 3) {
            buf.put((byte) (strOffset >> 16));
            buf.put((byte) ((strOffset & 0xFF00) >> 8));
            buf.put((byte) (strOffset & 0x00FF));
        } else {
            buf.putInt(strOffset);
        }
    }

    public boolean appendNULL() {
        ColumnDesc column = schema.get(cnt);
        if (column.getDataType() == DataType.kVarchar || column.getDataType() == DataType.kString) {
            int strPos = offsetVec.get(cnt);
            setStrOffset(strPos + 1);
        }
        cnt++;
        return true;
    }

    public boolean appendBool(boolean val) {
        if (!check(DataType.kBool)) {
            return false;
        }
        setField(cnt);
        buf.position(offsetVec.get(cnt));
        if (val) {
            buf.put((byte) 1);
        } else {
            buf.put((byte) 0);
        }
        cnt++;
        return true;
    }

    public boolean appendInt32(int val) {
        if (!check(DataType.kInt)) {
            return false;
        }
        setField(cnt);
        buf.position(offsetVec.get(cnt));
        buf.putInt(val);
        cnt++;
        return true;
    }

    public boolean appendInt16(short val) {
        if (!check(DataType.kSmallInt)) {
            return false;
        }
        setField(cnt);
        buf.position(offsetVec.get(cnt));
        buf.putShort(val);
        cnt++;
        return true;
    }

    public boolean appendTimestamp(long val) {
        if (!check(DataType.kTimestamp)) {
            return false;
        }
        setField(cnt);
        buf.position(offsetVec.get(cnt));
        buf.putLong(val);
        cnt++;
        return true;
    }

    public boolean appendInt64(long val) {
        if (!check(DataType.kBigInt)) {
            return false;
        }
        setField(cnt);
        buf.position(offsetVec.get(cnt));
        buf.putLong(val);
        cnt++;
        return true;
    }

//    public boolean appendBlob(long val) {
//        if (!check(DataType.Blob)) {
//            return false;
//        }
//        setField(cnt);
//        buf.position(offsetVec.get(cnt));
//        buf.putLong(val);
//        cnt++;
//        return true;
//    }

    public boolean appendFloat(float val) {
        if (!check(DataType.kFloat)) {
            return false;
        }
        setField(cnt);
        buf.position(offsetVec.get(cnt));
        buf.putFloat(val);
        cnt++;
        return true;
    }

    public boolean appendDouble(double val) {
        if (!check(DataType.kDouble)) {
            return false;
        }
        setField(cnt);
        buf.position(offsetVec.get(cnt));
        buf.putDouble(val);
        cnt++;
        return true;
    }

    public boolean appendDate(Date date) {
        int year = date.getYear();
        int month = date.getMonth();
        int day = date.getDate();
        int data = year << 16;
        data = data | (month << 8);
        data = data | day;
        buf.position(offsetVec.get(cnt));
        buf.putInt(data);
        setField(cnt);
        cnt++;
        return true;
    }

    public boolean appendString(String val) {
        byte[] bytes = val.getBytes(RowCodecCommon.CHARSET);
        int length = bytes.length;
        if (val == null || (!check(DataType.kVarchar) && !check(DataType.kString))) {
            return false;
        }
        if (strOffset + length > size) {
            return false;
        }
        int strPos = offsetVec.get(cnt);
        if (strPos == 0) {
            setStrOffset(strPos);
        }
        if (length != 0) {
            buf.position(strOffset);
            buf.put(val.getBytes(RowCodecCommon.CHARSET), 0, length);
        }
        strOffset += length;
        setStrOffset(strPos + 1);
        setField(cnt);
        cnt++;
        return true;
    }

    public static ByteBuffer encode(Object[] row, List<Common.ColumnDesc> schema, int schemaVer) throws RuntimeException {
        if (row == null || row.length == 0 || schema == null || schema.size() == 0 || row.length != schema.size()) {
            throw new RuntimeException("input error");
        }
        int strLength = RowCodecCommon.calStrLength(row, schema);
        RowBuilder builder = new RowBuilder(schema, schemaVer);
        int size = builder.calTotalLength(strLength);
        ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        buffer = builder.setBuffer(buffer, size);
        for (int i = 0; i < schema.size(); i++) {
            Common.ColumnDesc columnDesc = schema.get(i);
            Object column = row[i];
            if (columnDesc.getNotNull()
                    && column == null) {
                throw new RuntimeException("col " + columnDesc.getName() + " should not be null");
            } else if (column == null) {
                builder.appendNULL();
                continue;
            }
            boolean ok = false;
            switch (columnDesc.getDataType()) {
                case kVarchar:
                case kString:
                    ok = builder.appendString((String) column);
                    break;
                case kBool:
                    ok = builder.appendBool((Boolean) column);
                    break;
                case kSmallInt:
                    ok = builder.appendInt16((Short) column);
                    break;
                case kInt:
                    ok = builder.appendInt32((Integer) column);
                    break;
                case kTimestamp:
                    if (column instanceof DateTime) {
                        ok = builder.appendTimestamp(((DateTime) column).getMillis());
                    } else if (column instanceof Timestamp) {
                        ok = builder.appendTimestamp(((Timestamp) column).getTime());
                    } else {
                        ok = builder.appendTimestamp((Long) column);
                    }
                    break;
//                case Blob:
//                    ok = builder.appendBlob((long) column);
//                    break;
                case kBigInt:
                    ok = builder.appendInt64((Long) column);
                    break;
                case kFloat:
                    ok = builder.appendFloat((Float) column);
                    break;
                case kDate:
                    ok = builder.appendDate((Date) column);
                    break;
                case kDouble:
                    ok = builder.appendDouble((Double) column);
                    break;
                default:
                    throw new RuntimeException("unsupported data type " + columnDesc.getDataType());
            }
            if (!ok) {
                throw new RuntimeException("append " + i + " col " + columnDesc.getDataType() + " error");
            }
        }
        return buffer;
    }

    public static ByteBuffer encode(Map<String, Object> row, List<ColumnDesc> schema, Map<String, Long> blobKeys, int schemaVersion) throws RuntimeException {
        if (row == null || row.size() == 0 || schema == null || schema.size() == 0 || row.size() != schema.size()) {
            throw new RuntimeException("input error");
        }
        int strLength = RowCodecCommon.calStrLength(row, schema);
        RowBuilder builder = new RowBuilder(schema, schemaVersion);
        int size = builder.calTotalLength(strLength);
        ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        buffer = builder.setBuffer(buffer, size);
        for (int i = 0; i < schema.size(); i++) {
            ColumnDesc columnDesc = schema.get(i);
            Object column = row.get(columnDesc.getName());
            if (columnDesc.getNotNull()
                    && column == null) {
                throw new RuntimeException("col " + columnDesc.getName() + " should not be null");
            } else if (column == null) {
                builder.appendNULL();
                continue;
            }
            boolean ok = false;
            switch (columnDesc.getDataType()) {
                case kVarchar:
                case kString:
                    ok = builder.appendString((String) column);
                    break;
                case kBool:
                    ok = builder.appendBool((Boolean) column);
                    break;
                case kSmallInt:
                    ok = builder.appendInt16((Short) column);
                    break;
                case kInt:
                    ok = builder.appendInt32((Integer) column);
                    break;
                case kTimestamp:
                    if (column instanceof DateTime) {
                        ok = builder.appendTimestamp(((DateTime) column).getMillis());
                    } else if (column instanceof Timestamp) {
                        ok = builder.appendTimestamp(((Timestamp) column).getTime());
                    } else {
                        ok = builder.appendTimestamp((Long) column);
                    }
                    break;
//                case Blob:
//                    if (blobKeys == null || blobKeys.isEmpty()) {
//                        ok = builder.appendBlob((Long) column);
//                    } else {
//                        Long key = blobKeys.get(columnDesc.getName());
//                        if (key == null) {
//                            ok = false;
//                            break;
//                        }
//                        ok = builder.appendBlob(key);
//                    }
//                    break;
                case kBigInt:
                    ok = builder.appendInt64((Long) column);
                    break;
                case kFloat:
                    ok = builder.appendFloat((Float) column);
                    break;
                case kDate:
                    ok = builder.appendDate((Date) column);
                    break;
                case kDouble:
                    ok = builder.appendDouble((Double) column);
                    break;
                default:
                    throw new RuntimeException("unsupported data type");
            }
            if (!ok) {
                throw new RuntimeException("append " + columnDesc.getDataType().toString() + " error");
            }
        }
        return buffer;
    }
}

