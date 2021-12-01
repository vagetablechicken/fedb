package com._4paradigm.openmldb.spark.write;

import com._4paradigm.openmldb.sdk.SqlExecutor;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

public class OpenmldbBatchWrite implements BatchWrite {
    private final OpenmldbWriteConfig config;

    public OpenmldbBatchWrite(OpenmldbWriteConfig config, LogicalWriteInfo info) {
        this.config = config;
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
        return new OpenmldbDataWriterFactory(config);
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {

    }

    @Override
    public void abort(WriterCommitMessage[] messages) {

    }
}
