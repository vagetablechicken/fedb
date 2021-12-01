package com._4paradigm.openmldb.spark.write;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

public class OpenmldbBatchWrite implements BatchWrite {
    public OpenmldbBatchWrite(LogicalWriteInfo info) {
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
        return new OpenmldbDataWriterFactory(info);
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {

    }

    @Override
    public void abort(WriterCommitMessage[] messages) {

    }
}
