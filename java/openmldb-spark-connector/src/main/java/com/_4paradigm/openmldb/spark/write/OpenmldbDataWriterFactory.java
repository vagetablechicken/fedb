package com._4paradigm.openmldb.spark.write;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;

public class OpenmldbDataWriterFactory implements DataWriterFactory {
    public OpenmldbDataWriterFactory(PhysicalWriteInfo info) {
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
        return new OpenmldbDataWriter(partitionId, taskId);
    }
}
